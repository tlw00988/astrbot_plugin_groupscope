"""Phase 4 群体记忆落库：话题、事件、证据、时间线。

原则：
* 话题/事件是**推断**，每条都带 ``source`` / ``confidence`` / ``prompt_version``
* 证据消息必须能回跳 ``messages``（WebUI 与指令都能跳原消息）
* 时间线由话题/事件派生，删除话题/事件时级联清理
"""

from __future__ import annotations

import json
import time
from typing import Any

import aiosqlite

SOURCE_RULE = "rule"
SOURCE_LLM = "llm"


class MemoryStore:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.conn = conn

    # -- 话题 -------------------------------------------------------

    async def upsert_topic(
        self,
        group_ref: int,
        topic: dict[str, Any],
        *,
        resolve_message: Any = None,
    ) -> int | None:
        """写入话题与其证据，返回 topic id（同名话题复用并累加）。"""
        title = str(topic.get("title") or "").strip()
        if not title:
            return None
        source = str(topic.get("source") or SOURCE_RULE)
        now = int(time.time())
        first_seen = int(topic.get("first_seen") or now)
        last_seen = int(topic.get("last_seen") or now)
        keywords = topic.get("keywords") or []
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await self.conn.execute(
                "INSERT INTO topics(group_ref, title, summary, keywords_json,"
                " first_seen, last_seen, mention_count, source, confidence,"
                " prompt_version, created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(group_ref, title, source) DO UPDATE SET"
                "  summary=COALESCE(excluded.summary, topics.summary),"
                "  keywords_json=COALESCE(excluded.keywords_json, topics.keywords_json),"
                "  first_seen=MIN(topics.first_seen, excluded.first_seen),"
                "  last_seen=MAX(topics.last_seen, excluded.last_seen),"
                "  mention_count=MAX(topics.mention_count, excluded.mention_count),"
                "  confidence=MAX(topics.confidence, excluded.confidence),"
                "  prompt_version=COALESCE(excluded.prompt_version, topics.prompt_version)",
                (
                    group_ref,
                    title,
                    str(topic.get("summary") or "")[:500] or None,
                    json.dumps(keywords, ensure_ascii=False) if keywords else None,
                    first_seen,
                    last_seen,
                    int(topic.get("mention_count") or 0),
                    source,
                    float(topic.get("confidence") or 0.0),
                    topic.get("prompt_version"),
                    now,
                ),
            )
            # 取 id：先按唯一键查
            row = await (
                await self.conn.execute(
                    "SELECT id FROM topics WHERE group_ref=? AND title=? AND source=?",
                    (group_ref, title, source),
                )
            ).fetchone()
            topic_id = int(row["id"]) if row else None
            if topic_id is None:
                await self.conn.execute("COMMIT")
                return None

            for msg_id in topic.get("evidence_msg_ids") or []:
                ref = await _resolve_msg(self.conn, msg_id, resolve_message)
                if ref is None:
                    continue  # 证据必须真实存在，查不到就丢弃（禁止幻觉证据）
                await self.conn.execute(
                    "INSERT INTO topic_mentions(topic_id, message_ref, weight, created_at)"
                    " VALUES(?,?,1,?) ON CONFLICT(topic_id, message_ref) DO NOTHING",
                    (topic_id, ref, now),
                )
                # 更新时间跨度为证据实际时间
                mrow = await (
                    await self.conn.execute(
                        "SELECT sent_at FROM messages WHERE id=?", (ref,)
                    )
                ).fetchone()
                if mrow:
                    await self.conn.execute(
                        "UPDATE topics SET first_seen=MIN(first_seen,?),"
                        " last_seen=MAX(last_seen,?) WHERE id=?",
                        (int(mrow["sent_at"]), int(mrow["sent_at"]), topic_id),
                    )

            await self.conn.execute(
                "INSERT INTO memory_timeline(group_ref, item_kind, item_id, occurred_at,"
                " label, detail) VALUES(?,?,?,?,?,?)"
                " ON CONFLICT(group_ref, item_kind, item_id, occurred_at) DO NOTHING",
                (
                    group_ref,
                    "topic",
                    topic_id,
                    first_seen,
                    title,
                    str(topic.get("summary") or "")[:200] or None,
                ),
            )
            await self.conn.execute("COMMIT")
        except BaseException:
            await self.conn.execute("ROLLBACK")
            raise
        return topic_id

    # -- 事件 -------------------------------------------------------

    async def upsert_event(
        self,
        group_ref: int,
        event: dict[str, Any],
        *,
        resolve_message: Any = None,
    ) -> int | None:
        title = str(event.get("title") or "").strip()
        if not title:
            return None
        source = str(event.get("source") or SOURCE_RULE)
        kind = str(event.get("event_kind") or "event")
        now = int(time.time())
        first_seen = int(event.get("first_seen") or now)
        last_seen = int(event.get("last_seen") or now)
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            await self.conn.execute(
                "INSERT INTO events(group_ref, title, summary, event_kind, first_seen,"
                " last_seen, peak_at, mention_count, source, confidence,"
                " prompt_version, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(group_ref, title, event_kind, source) DO UPDATE SET"
                "  summary=COALESCE(excluded.summary, events.summary),"
                "  first_seen=MIN(events.first_seen, excluded.first_seen),"
                "  last_seen=MAX(events.last_seen, excluded.last_seen),"
                "  peak_at=COALESCE(excluded.peak_at, events.peak_at),"
                "  mention_count=MAX(events.mention_count, excluded.mention_count),"
                "  confidence=MAX(events.confidence, excluded.confidence),"
                "  prompt_version=COALESCE(excluded.prompt_version, events.prompt_version)",
                (
                    group_ref,
                    title,
                    str(event.get("summary") or "")[:500] or None,
                    kind,
                    first_seen,
                    last_seen,
                    int(event["peak_at"]) if event.get("peak_at") else None,
                    int(event.get("mention_count") or 0),
                    source,
                    float(event.get("confidence") or 0.0),
                    event.get("prompt_version"),
                    now,
                ),
            )
            row = await (
                await self.conn.execute(
                    "SELECT id FROM events WHERE group_ref=? AND title=?"
                    " AND event_kind=? AND source=?",
                    (group_ref, title, kind, source),
                )
            ).fetchone()
            event_id = int(row["id"]) if row else None
            if event_id is None:
                await self.conn.execute("COMMIT")
                return None

            for ev in event.get("evidence") or []:
                msg_id = ev.get("msg_id") if isinstance(ev, dict) else ev
                ref = await _resolve_msg(self.conn, msg_id, resolve_message)
                if ref is None:
                    continue
                role = str(ev.get("role") or "spread") if isinstance(ev, dict) else "spread"
                if role not in ("origin", "spread", "evolve"):
                    role = "spread"
                await self.conn.execute(
                    "INSERT INTO event_evidence(event_id, message_ref, role, note,"
                    " created_at) VALUES(?,?,?,?,?)"
                    " ON CONFLICT(event_id, message_ref, role) DO NOTHING",
                    (
                        event_id,
                        ref,
                        role,
                        (str(ev.get("note") or "")[:100] or None)
                        if isinstance(ev, dict)
                        else None,
                        now,
                    ),
                )
                mrow = await (
                    await self.conn.execute(
                        "SELECT sent_at FROM messages WHERE id=?", (ref,)
                    )
                ).fetchone()
                if mrow:
                    await self.conn.execute(
                        "UPDATE events SET first_seen=MIN(first_seen,?),"
                        " last_seen=MAX(last_seen,?) WHERE id=?",
                        (int(mrow["sent_at"]), int(mrow["sent_at"]), event_id),
                    )

            await self.conn.execute(
                "INSERT INTO memory_timeline(group_ref, item_kind, item_id, occurred_at,"
                " label, detail) VALUES(?,?,?,?,?,?)"
                " ON CONFLICT(group_ref, item_kind, item_id, occurred_at) DO NOTHING",
                (
                    group_ref,
                    "event",
                    event_id,
                    first_seen,
                    title,
                    str(event.get("summary") or "")[:200] or None,
                ),
            )
            await self.conn.execute("COMMIT")
        except BaseException:
            await self.conn.execute("ROLLBACK")
            raise
        return event_id

    # -- 查询 -------------------------------------------------------

    async def list_topics(
        self, group_ref: int | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT t.*, (SELECT COUNT(*) FROM topic_mentions tm WHERE tm.topic_id=t.id)"
            " AS evidence FROM topics t WHERE 1=1"
        )
        params: list[Any] = []
        if group_ref is not None:
            sql += " AND t.group_ref = ?"
            params.append(group_ref)
        sql += " ORDER BY t.last_seen DESC, t.mention_count DESC LIMIT ?"
        params.append(max(1, min(limit, 100)))
        cur = await self.conn.execute(sql, params)
        rows = [dict(r) for r in await cur.fetchall()]
        for r in rows:
            try:
                r["keywords"] = json.loads(r.get("keywords_json") or "[]")
            except Exception:
                r["keywords"] = []
        return rows

    async def list_events(
        self,
        group_ref: int | None = None,
        *,
        event_kind: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT e.*, (SELECT COUNT(*) FROM event_evidence ee WHERE ee.event_id=e.id)"
            " AS evidence FROM events e WHERE 1=1"
        )
        params: list[Any] = []
        if group_ref is not None:
            sql += " AND e.group_ref = ?"
            params.append(group_ref)
        if event_kind:
            sql += " AND e.event_kind = ?"
            params.append(event_kind)
        sql += " ORDER BY e.last_seen DESC, e.mention_count DESC LIMIT ?"
        params.append(max(1, min(limit, 100)))
        cur = await self.conn.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]

    async def topic_evidence(self, topic_id: int, limit: int = 5) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT m.id, m.message_id, m.sent_at, m.text, m.sender_raw_name,"
            " m.sender_raw_id FROM topic_mentions tm JOIN messages m ON m.id=tm.message_ref"
            " WHERE tm.topic_id=? ORDER BY m.sent_at LIMIT ?",
            (topic_id, max(1, min(limit, 50))),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def event_evidence(self, event_id: int, limit: int = 5) -> list[dict[str, Any]]:
        cur = await self.conn.execute(
            "SELECT m.id, m.message_id, m.sent_at, m.text, m.sender_raw_name,"
            " m.sender_raw_id, ee.role, ee.note FROM event_evidence ee"
            " JOIN messages m ON m.id=ee.message_ref"
            " WHERE ee.event_id=? ORDER BY m.sent_at LIMIT ?",
            (event_id, max(1, min(limit, 50))),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def timeline(
        self, group_ref: int | None = None, limit: int = 30
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM memory_timeline WHERE 1=1"
        params: list[Any] = []
        if group_ref is not None:
            sql += " AND group_ref = ?"
            params.append(group_ref)
        sql += " ORDER BY occurred_at DESC LIMIT ?"
        params.append(max(1, min(limit, 200)))
        cur = await self.conn.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]

    # -- 重新分析 ---------------------------------------------------

    async def purge(
        self, *, group_ref: int | None, source: str = SOURCE_LLM
    ) -> tuple[int, int]:
        """删除指定来源的话题与事件（含证据与时间线），返回 (topics, events)。"""
        sql_t = "DELETE FROM topics WHERE source=?"
        sql_e = "DELETE FROM events WHERE source=?"
        params_t: list[Any] = [source]
        params_e: list[Any] = [source]
        if group_ref is not None:
            sql_t += " AND group_ref=?"
            sql_e += " AND group_ref=?"
            params_t.append(group_ref)
            params_e.append(group_ref)
        ct = await self.conn.execute(sql_t, params_t)
        ce = await self.conn.execute(sql_e, params_e)
        # 时间线孤儿清理（topic/event 删除后 item 指向已不存在的行）
        await self.conn.execute(
            "DELETE FROM memory_timeline WHERE (item_kind='topic' AND item_id NOT IN"
            " (SELECT id FROM topics)) OR (item_kind='event' AND item_id NOT IN"
            " (SELECT id FROM events))"
        )
        return getattr(ct, "rowcount", 0) or 0, getattr(ce, "rowcount", 0) or 0


async def _resolve_msg(conn: aiosqlite.Connection, message_id: Any, resolver: Any) -> int | None:
    """把外部消息 id 解析为本库 messages.id；找不到返回 None（禁止幻觉证据）。"""
    if message_id in (None, ""):
        return None
    if resolver is not None:
        ref = resolver(str(message_id))
        if hasattr(ref, "__await__"):
            ref = await ref
        if ref:
            return int(ref)
        return None
    cur = await conn.execute(
        "SELECT id FROM messages WHERE message_id=? ORDER BY id DESC LIMIT 1",
        (str(message_id),),
    )
    row = await cur.fetchone()
    return int(row["id"]) if row else None
