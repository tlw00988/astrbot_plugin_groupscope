"""本地分析任务队列：可入队、可认领、可重试、可重放。

落盘在 SQLite（``analysis_tasks``），因此：
* 插件重启后未完成的任务继续跑（断点续跑）；
* 同一 ``kind + dedup_key`` 只会存在一条，重复触发不会堆积；
* 重新分析（``/gs reanalyze``）可按类型清空后重新入队。
"""

from __future__ import annotations

import json
import time
from typing import Any

import aiosqlite

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"


class TaskQueue:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.conn = conn

    async def enqueue(
        self,
        kind: str,
        dedup_key: str,
        *,
        payload: dict[str, Any] | None = None,
        priority: int = 0,
        prompt_version: str | None = None,
    ) -> bool:
        """入队一条任务；已存在同 key 的未完成任务时跳过。返回是否新建。"""
        now = int(time.time())
        cur = await self.conn.execute(
            "INSERT OR IGNORE INTO analysis_tasks("
            " kind, dedup_key, status, priority, payload_json, prompt_version,"
            " created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                kind,
                dedup_key,
                STATUS_PENDING,
                priority,
                json.dumps(payload or {}, ensure_ascii=False),
                prompt_version,
                now,
                now,
            ),
        )
        if getattr(cur, "rowcount", 0) == 0:
            # 已存在：若是失败/取消状态则重新激活
            cur2 = await self.conn.execute(
                "UPDATE analysis_tasks SET status=?, updated_at=?, last_error=NULL"
                " WHERE kind=? AND dedup_key=? AND status IN (?,?)",
                (STATUS_PENDING, now, kind, dedup_key, STATUS_FAILED, STATUS_CANCELLED),
            )
            return getattr(cur2, "rowcount", 0) > 0
        return True

    async def claim(self, kind: str | None = None, limit: int = 1) -> list[dict[str, Any]]:
        """认领待处理任务（pending → running）。"""
        claimed: list[dict[str, Any]] = []
        now = int(time.time())
        for _ in range(max(1, limit)):
            sql = (
                "SELECT id FROM analysis_tasks WHERE status=?"
                + (" AND kind=?" if kind else "")
                + " ORDER BY priority DESC, created_at ASC LIMIT 1"
            )
            params: list[Any] = [STATUS_PENDING] + ([kind] if kind else [])
            cur = await self.conn.execute(sql, params)
            row = await cur.fetchone()
            if row is None:
                break
            task_id = int(row["id"])
            await self.conn.execute(
                "UPDATE analysis_tasks SET status=?, claimed_at=?, updated_at=?,"
                " attempts=attempts+1 WHERE id=? AND status=?",
                (STATUS_RUNNING, now, now, task_id, STATUS_PENDING),
            )
            cur2 = await self.conn.execute(
                "SELECT * FROM analysis_tasks WHERE id=?", (task_id,)
            )
            task = await cur2.fetchone()
            if task is not None:
                claimed.append(dict(task))
        return claimed

    async def complete(self, task_id: int) -> None:
        await self.conn.execute(
            "UPDATE analysis_tasks SET status=?, updated_at=?, last_error=NULL WHERE id=?",
            (STATUS_DONE, int(time.time()), task_id),
        )

    async def fail(self, task: dict[str, Any], error: str, *, retry: bool = True) -> None:
        """失败：未超重试上限则退回 pending，否则标记 failed。"""
        task_id = int(task["id"])
        attempts = int(task.get("attempts") or 0)
        max_attempts = int(task.get("max_attempts") or 3)
        now = int(time.time())
        status = STATUS_PENDING if (retry and attempts < max_attempts) else STATUS_FAILED
        await self.conn.execute(
            "UPDATE analysis_tasks SET status=?, updated_at=?, last_error=? WHERE id=?",
            (status, now, error[:500], task_id),
        )

    async def cancel(self, task_id: int) -> None:
        await self.conn.execute(
            "UPDATE analysis_tasks SET status=?, updated_at=? WHERE id=?",
            (STATUS_CANCELLED, int(time.time()), task_id),
        )

    async def stats(self) -> dict[str, int]:
        cur = await self.conn.execute(
            "SELECT status, COUNT(*) AS c FROM analysis_tasks GROUP BY status"
        )
        out = {s: 0 for s in (STATUS_PENDING, STATUS_RUNNING, STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED)}
        for row in await cur.fetchall():
            out[str(row["status"])] = int(row["c"])
        return out

    async def recent_errors(self, limit: int = 5) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT kind, dedup_key, attempts, last_error, updated_at"
            " FROM analysis_tasks WHERE last_error IS NOT NULL"
            " ORDER BY updated_at DESC LIMIT ?",
            (max(1, min(limit, 50)),),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def clear(self, kind: str, *, status: tuple[str, ...] = (STATUS_DONE, STATUS_FAILED)) -> int:
        """清空某类任务（重新分析前调用），返回删除条数。"""
        placeholders = ",".join("?" for _ in status)
        cur = await self.conn.execute(
            f"DELETE FROM analysis_tasks WHERE kind=? AND status IN ({placeholders})",
            (kind, *status),
        )
        return getattr(cur, "rowcount", 0) or 0

    async def reset_running(self) -> int:
        """启动时把上次崩溃遗留的 running 退回 pending。"""
        cur = await self.conn.execute(
            "UPDATE analysis_tasks SET status=?, updated_at=? WHERE status=?",
            (STATUS_PENDING, int(time.time()), STATUS_RUNNING),
        )
        return getattr(cur, "rowcount", 0) or 0
