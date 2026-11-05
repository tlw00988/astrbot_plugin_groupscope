"""消息档案读写（查询层 + 批量写入层）。"""

from __future__ import annotations

import time
from typing import Any

import aiosqlite

from ..capture.models import NormalizedMessage

# ---------------------------------------------------------------------------
# 写入
# ---------------------------------------------------------------------------


async def _upsert_group(
    conn: aiosqlite.Connection, item: NormalizedMessage
) -> int | None:
    if not item.group_raw_id:
        return None
    now = item.received_at
    await conn.execute(
        "INSERT INTO groups(platform_id, platform_name, group_id, group_name,"
        " first_seen, last_seen, capture_enabled)"
        " VALUES(?,?,?,?,?,?,1)"
        " ON CONFLICT(platform_id, group_id) DO UPDATE SET"
        "  last_seen = MAX(groups.last_seen, excluded.last_seen),"
        "  group_name = COALESCE(excluded.group_name, groups.group_name)",
        (
            item.platform_id,
            item.platform_name,
            item.group_raw_id,
            item.group_name,
            now,
            now,
        ),
    )
    cur = await conn.execute(
        "SELECT id FROM groups WHERE platform_id=? AND group_id=?",
        (item.platform_id, item.group_raw_id),
    )
    row = await cur.fetchone()
    return int(row["id"]) if row else None


async def _upsert_member(
    conn: aiosqlite.Connection, item: NormalizedMessage
) -> int | None:
    if not item.sender_raw_id:
        return None
    now = item.received_at
    await conn.execute(
        "INSERT INTO members(platform_id, user_id, current_nickname, first_seen,"
        " last_seen, message_count) VALUES(?,?,?,?,?,0)"
        " ON CONFLICT(platform_id, user_id) DO UPDATE SET"
        "  current_nickname = COALESCE(excluded.current_nickname,"
        "    members.current_nickname),"
        "  last_seen = MAX(members.last_seen, excluded.last_seen)",
        (
            item.platform_id,
            item.sender_raw_id,
            item.sender_raw_name,
            now,
            now,
        ),
    )
    cur = await conn.execute(
        "SELECT id FROM members WHERE platform_id=? AND user_id=?",
        (item.platform_id, item.sender_raw_id),
    )
    row = await cur.fetchone()
    return int(row["id"]) if row else None


async def _touch_alias(
    conn: aiosqlite.Connection, member_ref: int, alias: str, ts: int
) -> None:
    await conn.execute(
        "INSERT INTO member_aliases(member_id, alias, source, first_seen,"
        " last_seen, seen_count) VALUES(?,?,?,?,?,1)"
        " ON CONFLICT(member_id, alias, source) DO UPDATE SET"
        "  last_seen = MAX(member_aliases.last_seen, excluded.last_seen),"
        "  seen_count = member_aliases.seen_count + 1",
        (member_ref, alias, "nickname", ts, ts),
    )


async def _touch_member_group(
    conn: aiosqlite.Connection,
    member_ref: int,
    group_ref: int,
    nick: str | None,
    ts: int,
    count_one: bool,
) -> None:
    await conn.execute(
        "INSERT INTO member_group(member_id, group_id, current_nick, first_seen,"
        " last_seen, message_count) VALUES(?,?,?,?,?,?)"
        " ON CONFLICT(member_id, group_id) DO UPDATE SET"
        "  current_nick = COALESCE(excluded.current_nick, member_group.current_nick),"
        "  last_seen = MAX(member_group.last_seen, excluded.last_seen),"
        "  message_count = member_group.message_count + excluded.message_count",
        (member_ref, group_ref, nick, ts, ts, 1 if count_one else 0),
    )


async def insert_batch(conn: aiosqlite.Connection, items: list) -> int:
    """单事务写入一批规范化消息，返回实际新增（去重后）条数。"""
    if not items:
        return 0
    inserted = 0
    await conn.execute("BEGIN IMMEDIATE")
    try:
        member_cache: dict[tuple[str, str], int | None] = {}
        for item in items:
            group_ref = await _upsert_group(conn, item)

            member_ref: int | None = None
            cache_key = (item.platform_id, item.sender_raw_id or "")
            if item.sender_raw_id:
                if cache_key not in member_cache:
                    member_cache[cache_key] = await _upsert_member(conn, item)
                member_ref = member_cache[cache_key]
                if member_ref is not None and item.sender_raw_name:
                    await _touch_alias(
                        conn, member_ref, item.sender_raw_name, item.received_at
                    )

            cur = await conn.execute(
                "INSERT OR IGNORE INTO messages("
                " platform_id, platform_name, group_ref, group_raw_id, message_id,"
                " dedup_key, sender_ref, sender_raw_id, sender_raw_name, event_kind,"
                " notice_type, sent_at, received_at, message_type, text,"
                " reply_to_message_id, reply_to_sender_id, raw_json, inserted_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    item.platform_id,
                    item.platform_name,
                    group_ref,
                    item.group_raw_id,
                    item.message_id,
                    item.dedup_key,
                    member_ref,
                    item.sender_raw_id,
                    item.sender_raw_name,
                    item.event_kind,
                    item.notice_type,
                    item.sent_at,
                    item.received_at,
                    item.message_type,
                    item.text,
                    item.reply_to_message_id,
                    item.reply_to_sender_id,
                    item.raw_json,
                    int(time.time()),
                ),
            )
            if getattr(cur, "rowcount", 0) == 0:
                continue  # 重复消息
            inserted += 1
            message_ref = int(cur.lastrowid)

            for seg in item.segments:
                await conn.execute(
                    "INSERT INTO message_segments(message_ref, seg_index, seg_type,"
                    " payload_json, text) VALUES(?,?,?,?,?)",
                    (message_ref, seg.idx, seg.seg_type, seg.payload_json, seg.text),
                )

            for mention in item.mentions:
                target_ref: int | None = None
                if mention.target_raw_id and mention.kind == "at":
                    key = (item.platform_id, mention.target_raw_id)
                    if key not in member_cache:
                        cur2 = await conn.execute(
                            "SELECT id FROM members WHERE platform_id=? AND user_id=?",
                            key,
                        )
                        row = await cur2.fetchone()
                        member_cache[key] = int(row["id"]) if row else None
                    target_ref = member_cache[key]
                await conn.execute(
                    "INSERT INTO message_mentions(message_ref, kind, target_raw_id,"
                    " target_ref) VALUES(?,?,?,?)",
                    (
                        message_ref,
                        mention.kind,
                        mention.target_raw_id,
                        target_ref,
                    ),
                )

            if item.event_kind == "message":
                if member_ref is not None:
                    await conn.execute(
                        "UPDATE members SET message_count = message_count + 1"
                        " WHERE id=?",
                        (member_ref,),
                    )
                if member_ref is not None and group_ref is not None:
                    await _touch_member_group(
                        conn,
                        member_ref,
                        group_ref,
                        item.sender_raw_name,
                        item.received_at,
                        count_one=True,
                    )
            elif member_ref is not None and group_ref is not None:
                # 通知事件只更新活跃时间，不计入发言数
                await _touch_member_group(
                    conn,
                    member_ref,
                    group_ref,
                    item.sender_raw_name,
                    item.received_at,
                    count_one=False,
                )
        await conn.execute("COMMIT")
    except BaseException:
        try:
            await conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    return inserted


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

MESSAGE_SELECT = (
    "SELECT m.id, m.platform_id, m.platform_name, m.group_raw_id, m.message_id,"
    " m.event_kind, m.notice_type, m.sent_at, m.received_at, m.message_type,"
    " m.text, m.reply_to_message_id, m.sender_raw_id, m.sender_raw_name"
    " FROM messages m"
)


async def count_messages(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute("SELECT COUNT(*) AS c FROM messages")
    row = await cur.fetchone()
    return int(row["c"])


async def count_members(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute("SELECT COUNT(*) AS c FROM members")
    row = await cur.fetchone()
    return int(row["c"])


async def count_notices(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute(
        "SELECT COUNT(*) AS c FROM messages WHERE event_kind='notice'"
    )
    row = await cur.fetchone()
    return int(row["c"])


async def list_groups(conn: aiosqlite.Connection) -> list[dict[str, Any]]:
    cur = await conn.execute(
        "SELECT id, platform_id, platform_name, group_id, group_name, first_seen,"
        " last_seen, capture_enabled FROM groups ORDER BY last_seen DESC"
    )
    return [dict(row) for row in await cur.fetchall()]


async def daily_counts(
    conn: aiosqlite.Connection, group_ref: int | None = None, days: int = 7
) -> list[dict[str, Any]]:
    since = int(time.time()) - days * 86400
    sql = (
        "SELECT date(sent_at, 'unixepoch', 'localtime') AS day, COUNT(*) AS c"
        " FROM messages WHERE event_kind='message' AND sent_at >= ?"
    )
    params: list[Any] = [since]
    if group_ref is not None:
        sql += " AND group_ref = ?"
        params.append(group_ref)
    sql += " GROUP BY day ORDER BY day"
    cur = await conn.execute(sql, params)
    return [{"day": row["day"], "count": int(row["c"])} for row in await cur.fetchall()]


async def find_group(
    conn: aiosqlite.Connection, entry: str
) -> dict[str, Any] | None:
    """按 ``群ID`` 或 ``平台:群ID`` 查找群记录。"""
    from ..privacy import split_entry

    plat, gid = split_entry(entry)
    if not gid:
        return None
    if plat:
        cur = await conn.execute(
            "SELECT id, platform_id, platform_name, group_id, group_name,"
            " capture_enabled FROM groups"
            " WHERE group_id=? AND (platform_id=? OR platform_name=?)"
            " ORDER BY last_seen DESC LIMIT 1",
            (gid, plat, plat),
        )
    else:
        cur = await conn.execute(
            "SELECT id, platform_id, platform_name, group_id, group_name,"
            " capture_enabled FROM groups WHERE group_id=?"
            " ORDER BY last_seen DESC LIMIT 1",
            (gid,),
        )
    row = await cur.fetchone()
    return dict(row) if row else None


async def recent_messages(
    conn: aiosqlite.Connection, group_ref: int | None, limit: int
) -> list[dict[str, Any]]:
    sql = MESSAGE_SELECT
    params: list[Any] = []
    if group_ref is not None:
        sql += " WHERE m.group_ref = ?"
        params.append(group_ref)
    sql += " ORDER BY m.id DESC LIMIT ?"
    params.append(max(1, min(limit, 200)))
    cur = await conn.execute(sql, params)
    return [dict(row) for row in await cur.fetchall()]


async def search_messages(
    conn: aiosqlite.Connection, keyword: str, group_ref: int | None, limit: int = 50
) -> list[dict[str, Any]]:
    sql = MESSAGE_SELECT + " WHERE m.text LIKE ?"
    params: list[Any] = [f"%{keyword}%"]
    if group_ref is not None:
        sql += " AND m.group_ref = ?"
        params.append(group_ref)
    sql += " ORDER BY m.id DESC LIMIT ?"
    params.append(max(1, min(limit, 200)))
    cur = await conn.execute(sql, params)
    return [dict(row) for row in await cur.fetchall()]


async def message_context(
    conn: aiosqlite.Connection, message_id: int, before: int = 5, after: int = 5
) -> list[dict[str, Any]]:
    """围绕某条消息的原始上下文（用于从分析结果跳转到消息）。"""
    cur = await conn.execute(
        MESSAGE_SELECT + " WHERE m.id <= ? ORDER BY m.id DESC LIMIT ?",
        (message_id, before + 1),
    )
    head = [dict(row) for row in await cur.fetchall()]
    cur = await conn.execute(
        MESSAGE_SELECT + " WHERE m.id > ? ORDER BY m.id ASC LIMIT ?",
        (message_id, after),
    )
    tail = [dict(row) for row in await cur.fetchall()]
    return list(reversed(head)) + tail


async def segments_of(conn: aiosqlite.Connection, message_ref: int) -> list[dict]:
    cur = await conn.execute(
        "SELECT seg_index, seg_type, payload_json, text FROM message_segments"
        " WHERE message_ref=? ORDER BY seg_index",
        (message_ref,),
    )
    return [dict(row) for row in await cur.fetchall()]


async def set_group_capture(
    conn: aiosqlite.Connection, group_ref: int, enabled: bool
) -> None:
    await conn.execute(
        "UPDATE groups SET capture_enabled=? WHERE id=?",
        (1 if enabled else 0, group_ref),
    )


async def paused_groups(conn: aiosqlite.Connection) -> list[tuple[str, str]]:
    """返回暂停采集的 ``(platform_id, group_id)`` 集合元素。"""
    cur = await conn.execute(
        "SELECT platform_id, group_id FROM groups WHERE capture_enabled=0"
    )
    return [(str(row["platform_id"]), str(row["group_id"])) for row in await cur.fetchall()]


async def retention_purge(conn: aiosqlite.Connection, days: int) -> int:
    """删除超过保留期的消息（段与 @ 记录由外键级联删除）。"""
    if days <= 0:
        return 0
    cutoff = int(time.time()) - days * 86400
    await conn.execute("BEGIN IMMEDIATE")
    try:
        cur = await conn.execute(
            "DELETE FROM messages WHERE received_at < ?", (cutoff,)
        )
        deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        await conn.execute("COMMIT")
    except BaseException:
        await conn.execute("ROLLBACK")
        raise
    return deleted
