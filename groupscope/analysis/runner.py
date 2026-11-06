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

from ..memory.rules import MemMessage, extract_events, extract_topics
from ..memory.store import MemoryStore
from ..storage.db import Database
from . import prompts
from .addressing import AddressingStore
from .llm import LLMCallError, LLMGateway, RateLimitExceeded, parse_json_array
from .rules import RuleMessage, extract_addressing_rules
from .tasks import STATUS_PENDING, STATUS_RUNNING, TaskQueue

KIND_RULE = "addressing_rule"
KIND_LLM = "addressing_llm"
KIND_TOPIC_RULE = "topic_rule"
KIND_TOPIC_LLM = "topic_llm"
KIND_EVENT_RULE = "event_rule"
KIND_EVENT_LLM = "event_llm"

RULE_KINDS = (KIND_RULE, KIND_TOPIC_RULE, KIND_EVENT_RULE)
LLM_KINDS = (KIND_LLM, KIND_TOPIC_LLM, KIND_EVENT_LLM)

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
        self.memory = MemoryStore(db.conn)
        self.stats = {"rule_tasks": 0, "llm_tasks": 0, "errors": 0}
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()
        self._last_auto = 0.0
        self._last_maintain = 0.0

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
                await self._maintain()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("周期维护失败")
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

    async def _maintain(self) -> None:
        """每小时一次的长期运行维护：回收滞留任务 + SQLite 维护。"""
        now = time.time()
        if now - self._last_maintain < 3600:
            return
        self._last_maintain = now
        requeued = await self.queue.requeue_stale()
        if requeued:
            self.log.warning("回收滞留过久的分析任务 %s 条", requeued)
        await self.db.maintain()

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
            # Phase 4：群体记忆规则任务（每群每小时一桶）
            enqueued += len(
                await self.enqueue_memory(
                    gid,
                    kinds=(KIND_TOPIC_RULE, KIND_EVENT_RULE),
                    since=since,
                    until=until,
                )
            )
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
                # Phase 4：LLM 归纳（每群每 12 小时一桶，成本更低频）
                twelve_hours = int(now // 43200)
                for kind, pkind in (
                    (KIND_TOPIC_LLM, "topic"),
                    (KIND_EVENT_LLM, "event"),
                ):
                    pspec = prompts.prompt_for(pkind)
                    if await self.queue.enqueue(
                        kind,
                        f"{kind}:{gid}:t{twelve_hours}:{pspec['version']}",
                        payload={"group_ref": gid, "since": since, "until": until},
                        priority=-2,
                        prompt_version=pspec["version"],
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
                task = await self._claim_any(RULE_KINDS)
            else:
                task = await self._claim_any(None)
            if task is None:
                break
            try:
                if task["kind"] == KIND_RULE:
                    await self._run_rule_task(task)
                    self.stats["rule_tasks"] += 1
                elif task["kind"] == KIND_LLM:
                    await self._run_llm_task(task, "addressing")
                    self.stats["llm_tasks"] += 1
                elif task["kind"] == KIND_TOPIC_RULE:
                    await self._run_topic_rule(task)
                    self.stats["rule_tasks"] += 1
                elif task["kind"] == KIND_TOPIC_LLM:
                    await self._run_topic_llm(task)
                    self.stats["llm_tasks"] += 1
                elif task["kind"] == KIND_EVENT_RULE:
                    await self._run_event_rule(task)
                    self.stats["rule_tasks"] += 1
                elif task["kind"] == KIND_EVENT_LLM:
                    await self._run_event_llm(task)
                    self.stats["llm_tasks"] += 1
                else:
                    # 未知类型直接取消（前向兼容）
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

    async def _claim_any(self, kinds: tuple[str, ...] | None) -> dict[str, Any] | None:
        tasks = await self.queue.claim_kinds(kinds, limit=1)
        return tasks[0] if tasks else None

    async def _requeue(self, task: dict[str, Any], error: str) -> None:
        """限流时把任务退回 pending（不消耗重试次数、不进入退避）。"""
        await self.db.conn.execute(
            "UPDATE analysis_tasks SET status=?, claimed_at=NULL, updated_at=?,"
            " last_error=?, not_before=NULL WHERE id=? AND status=?",
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

    async def _run_llm_task(self, task: dict[str, Any], prompt_kind: str = "addressing") -> None:
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
        spec = prompts.prompt_for(prompt_kind)
        text = await self.gateway.chat(
            system=spec["system"],
            prompt=prompts.format_user(
                prompt_kind, members=member_lines, messages=msg_lines
            ),
        )
        rows = parse_json_array(text)

        valid_ids = {m["message_id"] for m in messages}
        valid_members = set(members)
        if prompt_kind == "addressing":
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
            return

        if prompt_kind == "topic":
            await self._persist_topics(int(group_ref), rows, valid_ids, spec)
        elif prompt_kind == "event":
            await self._persist_events(int(group_ref), rows, valid_ids, spec)

    # -- Phase 4：话题 / 事件 ---------------------------------------

    async def _run_topic_rule(self, task: dict[str, Any]) -> None:
        payload = _payload(task)
        group_ref = payload.get("group_ref")
        if group_ref is None:
            raise ValueError("任务缺少 group_ref")
        messages = await self._fetch_messages(
            int(group_ref), payload.get("since"), payload.get("until")
        )
        if len(messages) < 3:
            return
        mem_msgs = _to_mem_messages(messages)
        topics = extract_topics(mem_msgs)
        for topic in topics:
            await self.memory.upsert_topic(int(group_ref), topic)

    async def _run_event_rule(self, task: dict[str, Any]) -> None:
        payload = _payload(task)
        group_ref = payload.get("group_ref")
        if group_ref is None:
            raise ValueError("任务缺少 group_ref")
        messages = await self._fetch_messages(
            int(group_ref), payload.get("since"), payload.get("until")
        )
        if len(messages) < 3:
            return
        mem_msgs = _to_mem_messages(messages)
        events = extract_events(mem_msgs)
        for event in events:
            await self.memory.upsert_event(int(group_ref), event)

    async def _run_topic_llm(self, task: dict[str, Any]) -> None:
        await self._run_llm_task(task, "topic")

    async def _run_event_llm(self, task: dict[str, Any]) -> None:
        await self._run_llm_task(task, "event")

    async def _persist_topics(
        self,
        group_ref: int,
        rows: list[dict[str, Any]],
        valid_ids: set[str],
        spec: dict[str, str],
    ) -> None:
        for row in rows:
            title = str(row.get("title") or "").strip()
            if not title or len(title) > 40:
                continue
            try:
                confidence = float(row.get("confidence") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            evidence = [
                str(e)
                for e in (row.get("evidence_msg_id") or [])
                if str(e) in valid_ids
            ][:5]
            topic = {
                "title": title,
                "keywords": [str(k) for k in (row.get("keywords") or [])][:8],
                "summary": str(row.get("summary") or "")[:300],
                "confidence": max(0.0, min(1.0, confidence)),
                "evidence_msg_ids": evidence,
                "mention_count": len(evidence) or 1,
                "source": "llm",
                "prompt_version": spec["version"],
            }
            await self.memory.upsert_topic(group_ref, topic)

    async def _persist_events(
        self,
        group_ref: int,
        rows: list[dict[str, Any]],
        valid_ids: set[str],
        spec: dict[str, str],
    ) -> None:
        allowed_kinds = ("meme", "event", "dispute", "routine")
        for row in rows:
            title = str(row.get("title") or "").strip()
            if not title or len(title) > 40:
                continue
            kind = str(row.get("event_kind") or "event")
            if kind not in allowed_kinds:
                kind = "event"
            try:
                confidence = float(row.get("confidence") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            evidence = []
            for ev in row.get("evidence") or []:
                if not isinstance(ev, dict):
                    continue
                msg_id = str(ev.get("msg_id") or "")
                if msg_id not in valid_ids:
                    continue  # 幻觉证据直接丢弃
                role = str(ev.get("role") or "spread")
                if role not in ("origin", "spread", "evolve"):
                    role = "spread"
                evidence.append(
                    {
                        "msg_id": msg_id,
                        "role": role,
                        "note": str(ev.get("note") or "")[:100],
                    }
                )
                if len(evidence) >= 5:
                    break
            if not evidence:
                continue  # 无有效证据的事件不落库
            event = {
                "title": title,
                "event_kind": kind,
                "summary": str(row.get("summary") or "")[:300],
                "confidence": max(0.0, min(1.0, confidence)),
                "evidence": evidence,
                "mention_count": len(evidence),
                "source": "llm",
                "prompt_version": spec["version"],
            }
            await self.memory.upsert_event(group_ref, event)

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

    async def enqueue_memory(
        self,
        group_ref: int,
        *,
        kinds: tuple[str, ...] = (KIND_TOPIC_RULE, KIND_EVENT_RULE),
        since: int | None = None,
        until: int | None = None,
        priority: int = 0,
        with_llm: bool = False,
    ) -> list[str]:
        """入队群体记忆任务（话题/事件，可选附带 LLM 归纳），返回实际入队的类型。"""
        window = _window_key(since)
        done: list[str] = []
        for kind in kinds:
            if kind in LLM_KINDS and not with_llm:
                continue
            prompt_version = None
            if kind in LLM_KINDS:
                spec = prompts.prompt_for("topic" if kind.startswith("topic") else "event")
                prompt_version = spec["version"]
            if await self.queue.enqueue(
                kind,
                f"{kind}:{group_ref}:{window}"
                + (f":{prompt_version}" if prompt_version else ""),
                payload={"group_ref": group_ref, "since": since, "until": until},
                priority=priority,
                prompt_version=prompt_version,
            ):
                done.append(kind)
        return done


def _to_mem_messages(rows: list[dict[str, Any]]) -> list[MemMessage]:
    return [
        MemMessage(
            msg_id=str(m["message_id"]),
            sender_id=str(m["sender_raw_id"] or ""),
            text=m["text"] or "",
            sent_at=int(m["sent_at"] or 0),
            at_targets=m.get("at_targets") or [],
        )
        for m in rows
    ]


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
