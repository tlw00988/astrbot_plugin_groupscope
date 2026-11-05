"""称呼结果的落库与查询（观察事实 + AI 推断，两者严格区分）。"""

from __future__ import annotations

import time
from typing import Any

import aiosqlite

SOURCE_RULE = "rule"
SOURCE_LLM = "llm"
SOURCE_MANUAL = "manual"


class AddressingStore:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self.conn = conn

    # -- 写入 -------------------------------------------------------

    async def upsert(
        self,
        rows: list[dict[str, Any]],
        *,
        group_ref: int | None,
        resolve_member: Any = None,
        prompt_version: str | None = None,
    ) -> int:
        """写入称呼记录。``resolve_member(raw_id) -> member_id|None``（可 async）。

        返回实际写入（含更新）条数。
        """
        if not rows:
            return 0
        now = int(time.time())
        written = 0
        await self.conn.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                from_raw = row.get("from_raw_id")
                to_raw = row.get("to_raw_id")
                from_member = await _resolve(resolve_member, from_raw)
                to_member = await _resolve(resolve_member, to_raw)
                evidence_ref = await self._evidence_ref(row.get("evidence_msg_id"))
                cur = await self.conn.execute(
                    "INSERT INTO addressing("
                    " group_ref, from_member, from_raw_id, to_member, to_raw_id,"
                    " address, source, confidence, evidence_message, prompt_version,"
                    " created_at, last_seen, seen_count)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1)"
                    " ON CONFLICT(group_ref, IFNULL(from_member,-1),"
                    "  IFNULL(to_member,-1), address, source) DO UPDATE SET"
                    "  last_seen = MAX(addressing.last_seen, excluded.last_seen),"
                    "  seen_count = addressing.seen_count + 1,"
                    "  confidence = MAX(addressing.confidence, excluded.confidence),"
                    "  evidence_message = COALESCE(excluded.evidence_message,"
                    "     addressing.evidence_message),"
                    "  prompt_version = COALESCE(excluded.prompt_version,"
                    "     addressing.prompt_version)",
                    (
                        group_ref,
                        from_member,
                        from_raw,
                        to_member,
                        to_raw,
                        str(row.get("address") or "").strip(),
                        str(row.get("source") or SOURCE_RULE),
                        float(row.get("confidence") or 0.0),
                        evidence_ref,
                        row.get("prompt_version") or prompt_version,
                        now,
                        now,
                    ),
                )
                if getattr(cur, "rowcount", 0):
                    written += 1
            await self.conn.execute("COMMIT")
        except BaseException:
            await self.conn.execute("ROLLBACK")
            raise
        return written

    async def _evidence_ref(self, message_id: Any) -> int | None:
        if message_id in (None, ""):
            return None
        cur = await self.conn.execute(
            "SELECT id FROM messages WHERE message_id = ? ORDER BY id DESC LIMIT 1",
            (str(message_id),),
        )
        row = await cur.fetchone()
        return int(row["id"]) if row else None

    async def add_manual(
        self,
        *,
        group_ref: int,
        from_member: int | None,
        to_member: int,
        address: str,
        operator: str,
    ) -> None:
        """管理员显式指定的称呼（source='manual'，优先级最高）。

        ``from_member=None`` 表示“全员通用的称呼”（不限定发出者）。
        """
        now = int(time.time())
        await self.conn.execute(
            "INSERT INTO addressing(group_ref, from_member, from_raw_id, to_member,"
            " to_raw_id, address, source, confidence, created_at, last_seen,"
            " seen_count, reviewed) VALUES(?,?,?,?,?,?,?,?,?,?,1,1)"
            " ON CONFLICT(group_ref, IFNULL(from_member,-1), IFNULL(to_member,-1),"
            "  address, source) DO UPDATE SET last_seen=excluded.last_seen, reviewed=1",
            (group_ref, from_member, None, to_member, None, address, SOURCE_MANUAL, 1.0, now, now),
        )
        await self.conn.execute(
            "INSERT INTO member_manual_aliases(member_id, alias, operator, created_at)"
            " VALUES(?,?,?,?) ON CONFLICT(member_id, alias) DO NOTHING",
            (to_member, address, operator, now),
        )

    # -- 查询 -------------------------------------------------------

    async def query(
        self,
        *,
        group_ref: int | None = None,
        member_id: int | None = None,
        address: str | None = None,
        source: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT a.id, a.group_ref, a.from_member, a.to_member, a.address,"
            " a.source, a.confidence, a.seen_count, a.last_seen, a.prompt_version,"
            " a.evidence_message,"
            " f.user_id AS from_user_id, f.current_nickname AS from_nick,"
            " t.user_id AS to_user_id, t.current_nickname AS to_nick"
            " FROM addressing a"
            " LEFT JOIN members f ON f.id = a.from_member"
            " LEFT JOIN members t ON t.id = a.to_member"
            " WHERE 1=1"
        )
        params: list[Any] = []
        if group_ref is not None:
            sql += " AND a.group_ref = ?"
            params.append(group_ref)
        if member_id is not None:
            sql += " AND (a.to_member = ? OR a.from_member = ?)"
            params.extend([member_id, member_id])
        if address:
            sql += " AND a.address LIKE ?"
            params.append(f"%{address}%")
        if source:
            sql += " AND a.source = ?"
            params.append(source)
        sql += " ORDER BY a.seen_count DESC, a.last_seen DESC LIMIT ?"
        params.append(max(1, min(limit, 200)))
        cur = await self.conn.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]

    async def group_by_address(
        self, *, group_ref: int | None, limit: int = 30
    ) -> list[dict[str, Any]]:
        """按称呼聚合：谁被这样叫过、有多少证据、来自哪一层。"""
        sql = (
            "SELECT address, COUNT(*) AS rows_cnt, SUM(seen_count) AS total,"
            " SUM(CASE WHEN source='rule' THEN 1 ELSE 0 END) AS rule_rows,"
            " SUM(CASE WHEN source='llm' THEN 1 ELSE 0 END) AS llm_rows,"
            " SUM(CASE WHEN source='manual' THEN 1 ELSE 0 END) AS manual_rows,"
            " MIN(confidence) AS min_conf, MAX(confidence) AS max_conf,"
            " COUNT(DISTINCT to_member) AS targets"
            " FROM addressing WHERE 1=1"
        )
        params: list[Any] = []
        if group_ref is not None:
            sql += " AND group_ref = ?"
            params.append(group_ref)
        sql += " GROUP BY address ORDER BY total DESC LIMIT ?"
        params.append(max(1, min(limit, 200)))
        cur = await self.conn.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]

    async def known_aliases(self, group_ref: int | None) -> dict[str, set[str]]:
        """规则层需要的 ``成员id(raw) -> 已知别名集合``。"""
        sql = (
            "SELECT m.user_id, a.alias FROM member_aliases a"
            " JOIN members m ON m.id = a.member_id"
        )
        params: list[Any] = []
        if group_ref is not None:
            sql += " WHERE m.platform_id IN (SELECT DISTINCT platform_id FROM groups WHERE id = ?)"
            params.append(group_ref)
        cur = await self.conn.execute(sql, params)
        index: dict[str, set[str]] = {}
        for row in await cur.fetchall():
            index.setdefault(str(row["user_id"]), set()).add(str(row["alias"]))
        return index

    # -- 重新分析 ---------------------------------------------------

    async def purge(self, *, group_ref: int | None, sources: tuple[str, ...] = (SOURCE_LLM,)) -> int:
        """删除指定来源的称呼结论（重新分析前调用）。"""
        if not sources:
            return 0
        sql = "DELETE FROM addressing WHERE source IN ("
        sql += ",".join("?" for _ in sources) + ")"
        params: list[Any] = list(sources)
        if group_ref is not None:
            sql += " AND group_ref = ?"
            params.append(group_ref)
        cur = await self.conn.execute(sql, params)
        return getattr(cur, "rowcount", 0) or 0


async def _resolve(resolver: Any, raw_id: Any) -> int | None:
    if resolver is None or raw_id in (None, "", "all"):
        return None
    result = resolver(str(raw_id))
    if hasattr(result, "__await__"):
        result = await result
    return int(result) if result else None
