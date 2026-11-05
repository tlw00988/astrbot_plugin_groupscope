"""分析执行器：规则层零成本跑批，LLM 层按需消歧，全部走任务队列。

设计：
* 后台协程按固定节奏 ``run_once()``，单次只处理少量任务，绝不挤占事件循环；
* 规则任务不碰 LLM；LLM 任务必须通过 ``LLMGateway`` 的限流；
* 每次 LLM 调用记录 ``prompt_version``，结果带证据消息 id，可回溯原消息；
* 任何异常都落在任务的 ``last_error`` 上，不打断整个分析循环。
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from ..storage.db import Database
from . import prompts
from .addressing import AddressingStore
from .llm import LLMCallError, LLMGateway, RateLimitExceeded, parse_json_array
from .rules import RuleMessage, extract_addressing_rules
from .tasks import STATUS_PENDING, STATUS_RUNNING, TaskQueue

KIND_RULE = "addressing_rule"
KIND_LLM = "addressing_llm"

DEFAULT_BATCH = 60
MAX_MESSAGES_PER_PROMPT = 80


class AnalysisRunner:
    def __init__(
        self,
        db: Database,
        settings: Any,
        gateway: LLMGateway | None,
        logger: Any,
        *,
        tick_seconds: float = 5.0,
    ) -> None:
        self.db = db
        self.settings = settings
        self.gateway = gateway
        self.log = logger
        self.tick = tick_seconds
        self.queue = TaskQueue(db.conn)
        self.store = AddressingStore(db.conn)
        self.stats = {"rule_tasks": 0, "llm_tasks": 0, "errors": 0}
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._last_auto = 0.0

    # -- 生命周期 ---------------------------------------------------

    async def start(self) -> None:
        if self._task is not None:
            return
        try:
            reset = await self.queue.reset_running()
            if reset:
                self.log.info("重置上次中断的分析任务 %s 条", reset)
        except Exception:
            self.log.exception("重置分析任务失败")
        self._stopping.clear()
        self._task = asyncio.create_task(self._loop(), name="groupscope-analysis")

    async def stop(self, timeout: float = 5.0) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout)
        except (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._auto_enqueue()
            except asyncio.CancelledError:
                break
            except Exception:
                self.stats["errors"] += 1
                self.log.exception("自动入队失败")
            try:
                handled = await self.run_once(
                    limit=int(getattr(self.settings, "analysis_batch_size", 3))
                )
            except asyncio.CancelledError:
                break
            except Exception:
                self.stats["errors"] += 1
                self.log.exception("分析循环异常")
                handled = 0
            try:
                if handled == 0:
                    await asyncio.wait_for(self._stopping.wait(), self.tick)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    async def _auto_enqueue(self, force: bool = False) -> int:
        """为活跃群自动入队分析任务（按时间桶去重，避免重复堆积）。

        * 规则层：每群每小时一个任务（零 LLM 成本）
        * LLM 层：每群每 6 小时一个任务，且仅在分析开关打开、有可用额度时
        """
        now = time.time()
        if not force and now - self._last_auto < 60:
            return 0
        self._last_auto = now
        window = int(getattr(self.settings, "analysis_window_hours", 24)) * 3600
        cur = await self.db.conn.execute(
            "SELECT id FROM groups WHERE capture_enabled = 1 AND last_seen >= ?",
            (int(now) - window,),
        )
        group_ids = [int(r["id"]) for r in await cur.fetchall()]
        enqueued = 0
        hour = int(now // 3600)
        six_hours = int(now // 21600)
        for gid in group_ids:
            since = int(now) - window
            until = int(now) + 1  # 含当前秒
            if await self.queue.enqueue(
                KIND_RULE,
                f"{KIND_RULE}:{gid}:h{hour}",
                payload={"group_ref": gid, "since": since, "until": until},
            ):
                enqueued += 1
            if self.gateway is not None and self.gateway.remaining_today != 0:
                spec = prompts.prompt_for("addressing")
                if await self.queue.enqueue(
                    KIND_LLM,
                    f"{KIND_LLM}:{gid}:s{six_hours}:{spec['version']}",
                    payload={"group_ref": gid, "since": since, "until": until},
                    priority=-1,
                    prompt_version=spec["version"],
                ):
                    enqueued += 1
        return enqueued

    # -- 单次调度 ---------------------------------------------------

    async def run_once(self, limit: int = 3) -> int:
        """处理最多 ``limit`` 个任务，返回处理数量。"""
        quota_exhausted = (
            self.gateway is not None and self.gateway.remaining_today == 0
        )
        handled = 0
        for _ in range(max(1, limit)):
            # 配额耗尽时只跑规则任务（零 LLM 成本），LLM 任务等次日额度
            if quota_exhausted:
                tasks = await self.queue.claim(KIND_RULE, limit=1)
            else:
                tasks = await self.queue.claim(limit=1)
            if not tasks:
                break
            task = tasks[0]
            try:
                if task["kind"] == KIND_RULE:
                    await self._run_rule_task(task)
                    self.stats["rule_tasks"] += 1
                elif task["kind"] == KIND_LLM:
                    await self._run_llm_task(task)
                    self.stats["llm_tasks"] += 1
                else:
                    # Phase 4 类型在各自阶段接入；未知类型直接取消
                    await self.queue.cancel(int(task["id"]))
                    continue
                await self.queue.complete(int(task["id"]))
            except asyncio.CancelledError:
                raise
            except RateLimitExceeded as e:
                # 限流（配额/并发超时）不重试：退回 pending，本轮直接收手
                self.stats["errors"] += 1
                await self._requeue(task, str(e))
                return handled
            except Exception as e:
                self.stats["errors"] += 1
                self.log.warning("分析任务失败(%s): %s", task["kind"], e)
                await self.queue.fail(task, str(e))
            handled += 1
        return handled

    async def _requeue(self, task: dict[str, Any], error: str) -> None:
        """限流时把任务退回 pending（不消耗重试次数）。"""
        await self.db.conn.execute(
            "UPDATE analysis_tasks SET status=?, claimed_at=NULL, updated_at=?,"
            " last_error=? WHERE id=? AND status=?",
            (
                STATUS_PENDING,
                int(time.time()),
                error[:500],
                int(task["id"]),
                STATUS_RUNNING,
            ),
        )

    # -- 规则层 -----------------------------------------------------

    async def _run_rule_task(self, task: dict[str, Any]) -> None:
        payload = _payload(task)
        group_ref = payload.get("group_ref")
        if group_ref is None:
            raise ValueError("任务缺少 group_ref")
        messages = await self._fetch_messages(
            int(group_ref), payload.get("since"), payload.get("until")
        )
        if not messages:
            return
        aliases = await self.store.known_aliases(int(group_ref))
        rule_msgs = [
            RuleMessage(
                msg_id=m["message_id"],
                sender_id=str(m["sender_raw_id"] or ""),
                text=m["text"] or "",
                reply_to_sender_id=str(m["reply_to_sender_id"])
                if m["reply_to_sender_id"]
                else None,
                at_targets=m.get("at_targets") or [],
            )
            for m in messages
        ]
        rows = extract_addressing_rules(rule_msgs, known_aliases=aliases)
        await self.store.upsert(
            rows,
            group_ref=int(group_ref),
            resolve_member=self._resolver_for(int(group_ref)),
        )

    # -- LLM 层 -----------------------------------------------------

    async def _run_llm_task(self, task: dict[str, Any]) -> None:
        if self.gateway is None:
            raise LLMCallError("LLM 分析未启用（无可 Provider 或已关闭）")
        payload = _payload(task)
        group_ref = payload.get("group_ref")
        if group_ref is None:
            raise ValueError("任务缺少 group_ref")
        messages = await self._fetch_messages(
            int(group_ref), payload.get("since"), payload.get("until")
        )
        if not messages:
            return
        members = await self._fetch_members(int(group_ref))
        if not members:
            return

        member_lines = "\n".join(
            f"- {mid}: {nick or '-'}" for mid, nick in members.items()
        )
        msg_lines = "\n".join(
            f"[{m['message_id']}] {m['sender_raw_id']}({m['sender_nick'] or '?'}): "
            f"{(m['text'] or '')[:200]}"
            for m in messages
        )
        spec = prompts.prompt_for("addressing")
        text = await self.gateway.chat(
            system=spec["system"],
            prompt=prompts.format_user(
                "addressing", members=member_lines, messages=msg_lines
            ),
        )
        rows = parse_json_array(text)

        valid_ids = {m["message_id"] for m in messages}
        valid_members = set(members)
        clean: list[dict[str, Any]] = []
        for row in rows:
            address = str(row.get("address") or "").strip()
            evidence = str(row.get("evidence_msg_id") or "")
            from_raw = str(row.get("from") or "")
            to_raw = row.get("to")
            if not address or evidence not in valid_ids or from_raw not in valid_members:
                continue
            if to_raw is not None and str(to_raw) not in valid_members:
                continue  # 只允许指认库内成员，避免幻觉身份
            try:
                confidence = float(row.get("confidence") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            clean.append(
                {
                    "from_raw_id": from_raw,
                    "to_raw_id": str(to_raw) if to_raw is not None else None,
                    "address": address,
                    "source": "llm",
                    "confidence": max(0.0, min(1.0, confidence)),
                    "evidence_msg_id": evidence,
                    "prompt_version": spec["version"],
                }
            )
        if clean:
            await self.store.upsert(
                clean,
                group_ref=int(group_ref),
                resolve_member=self._resolver_for(int(group_ref)),
                prompt_version=spec["version"],
            )

    # -- 数据装载 ---------------------------------------------------

    async def _fetch_messages(
        self, group_ref: int, since: Any, until: Any
    ) -> list[dict[str, Any]]:
        now = int(time.time())
        since = int(since) if since else now - 86400 * 7
        # 默认窗口右端开区间到“下一秒”，否则 sent_at == now 的消息会被排除
        until = int(until) if until else now + 1
        cur = await self.db.conn.execute(
            "SELECT m.id, m.message_id, m.text, m.sender_raw_id, m.reply_to_sender_id,"
            " m.sent_at, s.current_nickname AS sender_nick"
            " FROM messages m LEFT JOIN members s ON s.id = m.sender_ref"
            " WHERE m.group_ref = ? AND m.event_kind = 'message'"
            "   AND m.sent_at >= ? AND m.sent_at < ? AND m.text IS NOT NULL"
            " ORDER BY m.id DESC LIMIT ?",
            (group_ref, since, until, MAX_MESSAGES_PER_PROMPT),
        )
        rows = [dict(r) for r in await cur.fetchall()]
        rows.reverse()  # 保持时间正序
        if not rows:
            return []
        refs = [int(r["id"]) for r in rows]
        placeholders = ",".join("?" for _ in refs)
        cur2 = await self.db.conn.execute(
            f"SELECT message_ref, target_raw_id FROM message_mentions"
            f" WHERE message_ref IN ({placeholders}) AND target_raw_id IS NOT NULL",
            refs,
        )
        at_map: dict[int, list[str]] = {}
        for row in await cur2.fetchall():
            at_map.setdefault(int(row["message_ref"]), []).append(
                str(row["target_raw_id"])
            )
        for r in rows:
            r["at_targets"] = at_map.get(int(r["id"]), [])
        return rows

    async def _fetch_members(self, group_ref: int) -> dict[str, str]:
        cur = await self.db.conn.execute(
            "SELECT DISTINCT m.user_id, m.current_nickname FROM members m"
            " WHERE m.platform_id = (SELECT platform_id FROM groups WHERE id = ?)"
            " ORDER BY m.last_seen DESC LIMIT 200",
            (group_ref,),
        )
        return {str(r["user_id"]): (r["current_nickname"] or "") for r in await cur.fetchall()}

    def _resolver_for(self, group_ref: int):
        """返回把平台 raw id 解析成成员主键的异步函数（按群所在平台限定，带缓存）。"""
        cache: dict[str, int | None] = {}

        async def resolve(raw_id: str) -> int | None:
            if raw_id in cache:
                return cache[raw_id]
            cur = await self.db.conn.execute(
                "SELECT m.id FROM members m"
                " WHERE m.user_id = ?"
                "   AND m.platform_id = (SELECT platform_id FROM groups WHERE id = ?)"
                " ORDER BY m.last_seen DESC LIMIT 1",
                (str(raw_id), group_ref),
            )
            row = await cur.fetchone()
            cache[str(raw_id)] = int(row["id"]) if row else None
            return cache[str(raw_id)]

        return resolve

    # -- 入队 -------------------------------------------------------

    async def enqueue_rule(
        self, group_ref: int, *, since: int | None = None, until: int | None = None, priority: int = 0
    ) -> bool:
        window = _window_key(since)
        return await self.queue.enqueue(
            KIND_RULE,
            f"{KIND_RULE}:{group_ref}:{window}",
            payload={"group_ref": group_ref, "since": since, "until": until},
            priority=priority,
        )

    async def enqueue_llm(
        self, group_ref: int, *, since: int | None = None, until: int | None = None, priority: int = 0
    ) -> bool:
        spec = prompts.prompt_for("addressing")
        window = _window_key(since)
        return await self.queue.enqueue(
            KIND_LLM,
            f"{KIND_LLM}:{group_ref}:{window}:{spec['version']}",
            payload={"group_ref": group_ref, "since": since, "until": until},
            priority=priority,
            prompt_version=spec["version"],
        )


def _payload(task: dict[str, Any]) -> dict[str, Any]:
    raw = task.get("payload_json")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _window_key(since: Any) -> str:
    if not since:
        return "default"
    return str(int(since))
