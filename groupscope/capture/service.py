"""采集服务：有界队列 + 后台批量写入 + 例行维护。

设计目标：
* 事件回调只做规范化与 ``put_nowait``，**绝不阻塞消息主流程**；
* 队列满时丢弃并计数（可用 ``/gs status`` 观察），不 await；
* 单写者串行落库，批量事务，写失败仅记日志不影响后续批次；
* ``stop()`` 时先排空队列再关库，保证停机一致性。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from ..config import CaptureSettings
from ..privacy import CapturePolicy
from ..storage import repo
from ..storage.db import Database
from .models import NormalizedMessage

_SENTINEL = None


class CaptureService:
    def __init__(
        self,
        db: Database,
        settings: CaptureSettings,
        policy: CapturePolicy,
        data_dir: Path,
        logger: Any,
    ) -> None:
        self.db = db
        self.settings = settings
        self.policy = policy
        self.data_dir = Path(data_dir)
        self.log = logger
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_maxsize)
        self.metrics: dict[str, int] = {
            "accepted": 0,
            "dropped": 0,
            "written": 0,
            "inserted": 0,
            "batches": 0,
            "write_errors": 0,
        }
        self._tasks: list[asyncio.Task] = []
        self._idle = asyncio.Event()
        self._idle.set()
        self._started = False
        self._stopping = False

    # -- 生命周期 ---------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        try:
            paused = await repo.paused_groups(self.db.conn)
            self.policy.paused_group_refs = set(paused)
        except Exception:
            self.log.exception("加载暂停群列表失败")
        self._stopping = False
        self._tasks = [
            asyncio.create_task(self._writer_loop(), name="groupscope-writer"),
            asyncio.create_task(self._maintenance_loop(), name="groupscope-maint"),
        ]
        self._started = True
        if self.settings.log_detail:
            self.log.info("采集服务已启动（队列上限 %s）", self.queue.maxsize)

    async def stop(self, timeout: float = 8.0) -> None:
        if not self._started:
            return
        self._stopping = True
        self._enqueue_sentinel()
        writer = next(
            (t for t in self._tasks if t.get_name() == "groupscope-writer"), None
        )
        if writer is not None:
            try:
                await asyncio.wait_for(asyncio.shield(writer), timeout)
            except (asyncio.TimeoutError, TimeoutError):
                writer.cancel()
                try:
                    await writer
                except (asyncio.CancelledError, Exception):
                    pass
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._tasks = []
        self._started = False
        self.log.info("采集服务已停止（已写入 %s 条）", self.metrics["written"])

    def _enqueue_sentinel(self) -> None:
        try:
            if self.queue.full():
                self.queue.get_nowait()  # 腾位给停机信号
            self.queue.put_nowait(_SENTINEL)
        except asyncio.QueueEmpty:
            pass

    # -- 入队 -------------------------------------------------------

    def enqueue(self, item: NormalizedMessage) -> bool:
        if self._stopping:
            return False
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            self.metrics["dropped"] += 1
            if self.metrics["dropped"] % 100 == 1:
                self.log.warning(
                    "采集队列已满，累计丢弃 %s 条", self.metrics["dropped"]
                )
            return False
        self.metrics["accepted"] += 1
        self._idle.clear()
        return True

    # -- 写入循环 ---------------------------------------------------

    async def _collect(self) -> tuple[list[NormalizedMessage] | None, bool]:
        item = await self.queue.get()
        if item is _SENTINEL:
            return None, True
        batch = [item]
        stop = False
        deadline = (
            asyncio.get_event_loop().time() + self.settings.batch_max_interval_ms / 1000
        )
        while len(batch) < self.settings.batch_max_rows:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                nxt = await asyncio.wait_for(self.queue.get(), remaining)
            except (asyncio.TimeoutError, TimeoutError):
                break
            if nxt is _SENTINEL:
                stop = True
                break
            batch.append(nxt)
        return batch, stop

    async def _write_batch(self, batch: list[NormalizedMessage]) -> None:
        try:
            inserted = await repo.insert_batch(self.db.conn, batch)
        except Exception:
            self.metrics["write_errors"] += 1
            self.log.exception("写入批次失败（%s 条，已跳过）", len(batch))
            return
        self.metrics["written"] += len(batch)
        self.metrics["inserted"] += inserted
        self.metrics["batches"] += 1
        try:
            await self.db.set_state("last_write_at", str(int(time.time())))
        except Exception:
            pass
        if self.settings.log_detail:
            self.log.debug("写入批次 %s 条（新增 %s）", len(batch), inserted)

    async def _writer_loop(self) -> None:
        while True:
            self._idle.clear()
            try:
                batch, stop = await self._collect()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("采集写入循环异常")
                await asyncio.sleep(0.2)
                continue
            if batch:
                await self._write_batch(batch)
            if self.queue.empty():
                self._idle.set()
            if stop:
                break
        self._idle.set()

    # -- 例行维护 ---------------------------------------------------

    async def _maintenance_loop(self) -> None:
        last_backup_day = await self._last_backup_day()
        while True:
            try:
                await asyncio.sleep(3600)
                if self.settings.log_retention_days > 0:
                    deleted = await repo.retention_purge(
                        self.db.conn, self.settings.log_retention_days
                    )
                    if deleted:
                        self.log.info(
                            "按保留期清理历史消息 %s 条（保留 %s 天）",
                            deleted,
                            self.settings.log_retention_days,
                        )
                if self.settings.backup_auto:
                    today = time.strftime("%Y%m%d")
                    if today != last_backup_day:
                        dest = await self.db.backup(
                            self.data_dir / "backups", self.settings.backup_keep
                        )
                        await self.db.set_state("last_backup_day", today)
                        last_backup_day = today
                        self.log.info("自动备份完成: %s", dest)
                try:
                    await self.db.conn.execute("PRAGMA optimize")
                except Exception:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("维护任务异常")

    async def _last_backup_day(self) -> str:
        try:
            return await self.db.get_state("last_backup_day", "") or ""
        except Exception:
            return ""

    # -- 辅助 -------------------------------------------------------

    async def wait_idle(self, timeout: float = 10.0) -> bool:
        """等待队列排空（测试与指令使用）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.queue.empty() and self._idle.is_set():
                return True
            await asyncio.sleep(0.02)
        return False

    @property
    def queue_depth(self) -> int:
        return self.queue.qsize()
