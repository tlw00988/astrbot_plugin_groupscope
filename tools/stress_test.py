#!/usr/bin/env python3
"""GroupScope 压力与稳定性测试工具（Phase 6）。

干什么：
  1. 在**隔离的临时 ASTRBOT_ROOT** 下打开全新数据库（不碰真实数据）；
  2. 通过真实采集管线（有界队列 → 批量写入）灌入 N 条合成消息；
  3. 全程采样**事件循环延迟**（判断是否阻塞消息主流程）；
  4. 结束后输出吞吐、丢弃数、库体积、`PRAGMA quick_check` 结果；
  5. 可选（``--archive``）：对测试数据做一次 归档→purge→恢复 全链路演练。

用法：
  python tools/stress_test.py                      # 默认 1 万条
  python tools/stress_test.py --count 50000 --batch 200
  python tools/stress_test.py --count 2000 --archive

退出码：0 = 全部断言通过；1 = 有断言失败。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

# ---- 环境隔离（必须在导入插件前设置） --------------------------------
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_workdir = Path(tempfile.mkdtemp(prefix="groupscope-stress-"))
os.environ["ASTRBOT_ROOT"] = str(_workdir)

from groupscope.capture.models import NormalizedMessage, Segment  # noqa: E402
from groupscope.capture.service import CaptureService  # noqa: E402
from groupscope.config import CaptureSettings  # noqa: E402
from groupscope.ops import archives as archive_ops  # noqa: E402
from groupscope.privacy import CapturePolicy  # noqa: E402
from groupscope.storage.db import Database  # noqa: E402

LOG = logging.getLogger("groupscope-stress")

# 合成语料（避免全库同一条文本，便于检索与归档断言）
_WORDS = [
    "周末去爬山吗", "晚上开黑差一个人", "新出的番看了没", "食堂今天有红烧肉",
    "作业明天交谁写了", "代码又跑不起来了", "球赛几点开始", "下雨了记得收衣服",
]


def make_item(i: int, *, group: str, sender: str, ts: int) -> NormalizedMessage:
    text = f"{_WORDS[i % len(_WORDS)]} #{i}"
    dedup = f"stress-{group}-{i}"
    return NormalizedMessage(
        dedup_key=dedup,
        platform_id="aiocqhttp-1",
        platform_name="aiocqhttp",
        group_raw_id=group,
        group_name=f"压测群{group}",
        sender_raw_id=sender,
        sender_raw_name=f"用户{sender}",
        event_kind="message",
        notice_type=None,
        sent_at=ts,
        received_at=ts,
        message_type="group",
        text=text,
        message_id=dedup,
        segments=[
            Segment(idx=0, seg_type="text", payload_json="{}", text=text),
            Segment(idx=1, seg_type="image", payload_json='{"file":"x.jpg"}', text=None),
        ],
        mentions=[],
    )


class LoopLagSampler:
    """事件循环延迟采样：每 20ms 醒一次，记录实际超时时间。"""

    def __init__(self, interval: float = 0.02) -> None:
        self.interval = interval
        self.samples: list[float] = []
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def _run(self) -> None:
        self_loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            t0 = self_loop.time()
            try:
                await asyncio.wait_for(self._stop.wait(), self.interval)
                return
            except (asyncio.TimeoutError, TimeoutError):
                pass
            self.samples.append((self_loop.time() - t0 - self.interval) * 1000)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="lag-sampler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task

    @property
    def max_ms(self) -> float:
        return max(self.samples) if self.samples else 0.0

    @property
    def p99_ms(self) -> float:
        if not self.samples:
            return 0.0
        s = sorted(self.samples)
        return s[min(len(s) - 1, int(len(s) * 0.99))]


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n} B"


async def run_stress(args: argparse.Namespace) -> int:
    db_path = _workdir / "data" / "plugin_data" / "astrbot_plugin_groupscope" / "groupscope.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = Database(db_path)
    failures: list[str] = []

    settings = CaptureSettings.from_conf(
        {
            "capture_enabled": True,
            "group_whitelist": ["aiocqhttp:900001", "aiocqhttp:900002"],
            "group_blacklist": [],
            "excluded_users": [],
            "record_notices": True,
            "batch_max_rows": args.batch,
            "batch_max_interval_ms": 50,
            "queue_maxsize": args.queue,
        }
    )
    policy = CapturePolicy(settings)
    svc = CaptureService(db, settings, policy, _workdir, LOG)
    sampler = LoopLagSampler()

    await db.open()
    await svc.start()
    sampler.start()

    groups = ["900001", "900002"]
    senders = [str(1000 + (i % args.senders)) for i in range(args.senders)]
    now = int(time.time())
    accepted = rejected = 0

    t_start = time.perf_counter()
    for i in range(args.count):
        item = make_item(
            i,
            group=groups[i % len(groups)],
            sender=senders[i % len(senders)],
            ts=now - (args.count - i),  # 过去 1 秒 1 条，避免时间戳进未来
        )
        if svc.enqueue(item):
            accepted += 1
        else:
            rejected += 1
        # 主动让出，模拟真实事件循环（不然后台写入任务抢不到 CPU）
        if i % 50 == 0:
            await asyncio.sleep(0)

    enqueued_at = time.perf_counter()
    idle = await svc.wait_idle(timeout=args.timeout)
    done_at = time.perf_counter()
    await sampler.stop()

    if not idle:
        failures.append(f"写入队列 {args.timeout}s 内未排空（可能写入阻塞）")

    # ---- 指标 ------------------------------------------------------
    m = svc.metrics
    cur = await db.conn.execute("SELECT COUNT(*) FROM messages")
    rows = int((await cur.fetchone())[0])
    cur = await db.conn.execute("SELECT COUNT(*) FROM message_segments")
    segs = int((await cur.fetchone())[0])
    db_size = await db.file_size()
    try:
        qc = await db.quick_check()
    except Exception as e:  # noqa: BLE001
        qc = f"FAIL: {e}"
        failures.append(f"quick_check 失败: {e}")

    total_s = done_at - t_start
    write_s = done_at - enqueued_at
    rate = accepted / total_s if total_s else 0.0

    print("=" * 62)
    print("GroupScope 压力测试报告")
    print("=" * 62)
    print(f"注入消息        : {args.count} 条（{args.senders} 个发送者 × {len(groups)} 个群）")
    print(f"入队            : accepted={m['accepted']} 拒绝={rejected}")
    print(f"写入            : inserted={m['inserted']} written={m['written']}"
          f" batches={m['batches']} write_errors={m['write_errors']}")
    print(f"耗时            : 入队 {enqueued_at - t_start:.2f}s + 排空 {write_s:.2f}s"
          f" = {total_s:.2f}s")
    print(f"吞吐            : {rate:,.0f} msg/s（含排空）")
    print(f"队列上限        : {args.queue}，注入期被丢弃 {m['dropped']} 条")
    print(f"库内消息        : {rows} 条 / 消息段 {segs} 条")
    print(f"数据库体积      : {fmt_size(db_size)}"
          f"（{db_size / max(1, rows):.0f} B/条）")
    print(f"事件循环延迟    : max {sampler.max_ms:.1f} ms / p99 {sampler.p99_ms:.1f} ms"
          f"（采样 {len(sampler.samples)} 次）")
    print(f"quick_check     : {qc}")

    # ---- 断言 ------------------------------------------------------
    if rows != m["inserted"]:
        failures.append(f"落库数 {rows} != inserted {m['inserted']}")
    if m["write_errors"]:
        failures.append(f"出现写入错误 {m['write_errors']} 次")
    if rows < args.count * 0.99 and not m["dropped"]:
        failures.append(f"落库 {rows} < 注入 {args.count}*0.99 且无丢弃记录")
    if qc != "ok":
        failures.append(f"quick_check != ok: {qc}")
    if sampler.max_ms > args.lag_budget_ms:
        failures.append(
            f"事件循环最大延迟 {sampler.max_ms:.0f}ms > 预算 {args.lag_budget_ms}ms"
        )

    # ---- 可选：归档全链路 ------------------------------------------
    if args.archive and rows:
        month = time.strftime("%Y-%m", time.localtime(now - args.count))
        arch_dir = db_path.parent / "archives"
        try:
            info = await archive_ops.create_archive(
                db.conn, month=month, archives_dir=arch_dir
            )
            v = await archive_ops.verify_manifest(
                db.conn, month=month, archives_dir=arch_dir
            )
            p = await archive_ops.purge_month(
                db.conn, month=month, archives_dir=arch_dir
            )
            r = await archive_ops.restore_month(
                db.conn, month=month, archives_dir=arch_dir
            )
            after = await db.conn.execute("SELECT COUNT(*) FROM messages")
            after_n = int((await after.fetchone())[0])
            print("-" * 62)
            print(f"归档 {month}: 创建 {info['messages']} 条 → verify ok={v['ok']}"
                  f" → purge 删 {p['deleted']} 条 → 恢复 {r['restored']} 条")
            print(f"恢复后库内消息: {after_n} 条")
            if not v["ok"]:
                failures.append("归档 verify 失败")
            if r["restored"] != info["messages"]:
                failures.append(
                    f"恢复数 {r['restored']} != 归档数 {info['messages']}"
                )
            if after_n != rows:
                failures.append(f"恢复后消息数 {after_n} != 原 {rows}")
        except Exception as e:  # noqa: BLE001
            failures.append(f"归档链路异常: {e}")
            import traceback
            traceback.print_exc()

    await svc.stop()
    await db.close()
    return report(failures)


def report(failures: list[str]) -> int:
    print("-" * 62)
    if failures:
        print(f"❌ 失败 {len(failures)} 项：")
        for f in failures:
            print(f"   - {f}")
        return 1
    print("✅ 全部断言通过")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="GroupScope 压力测试")
    parser.add_argument("--count", type=int, default=10000, help="注入消息数")
    parser.add_argument("--senders", type=int, default=20, help="发送者数量")
    parser.add_argument("--queue", type=int, default=10000, help="有界队列上限")
    parser.add_argument("--batch", type=int, default=100, help="批量写入行数阈值")
    parser.add_argument("--timeout", type=float, default=120.0, help="排空超时秒数")
    parser.add_argument("--lag-budget-ms", type=float, default=250.0,
                        help="事件循环最大延迟预算（毫秒）")
    parser.add_argument("--archive", action="store_true", help="附带归档全链路演练")
    parser.add_argument("--keep", action="store_true", help="保留临时数据目录")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    try:
        code = asyncio.run(run_stress(args))
    finally:
        if not args.keep:
            shutil.rmtree(_workdir, ignore_errors=True)
        else:
            print(f"数据目录保留: {_workdir}")
    return code


if __name__ == "__main__":
    sys.exit(main())
