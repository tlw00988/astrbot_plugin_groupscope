"""成员档案查询与人工身份合并。"""

from __future__ import annotations

import time
from typing import Any

import aiosqlite


async def member_search(
    conn: aiosqlite.Connection, keyword: str, limit: int = 20
) -> list[dict[str, Any]]:
    """按成员 ID 或昵称/别名模糊查找（跨平台返回，调用方负责展示平台）。"""
    kw = f"%{keyword}%"
    cur = await conn.execute(
        "SELECT DISTINCT m.id, m.platform_id, m.user_id, m.current_nickname,"
        " m.message_count, m.first_seen, m.last_seen"
        " FROM members m"
        " LEFT JOIN member_aliases a ON a.member_id = m.id"
        " WHERE m.user_id LIKE ? OR m.current_nickname LIKE ? OR a.alias LIKE ?"
        " ORDER BY m.last_seen DESC LIMIT ?",
        (kw, kw, kw, max(1, min(limit, 100))),
    )
    return [dict(r) for r in await cur.fetchall()]


async def member_profile(conn: aiosqlite.Connection, member_id: int) -> dict[str, Any] | None:
    """成员档案：身份、别名史、群内发言、最近消息、有向互动。"""
    cur = await conn.execute(
        "SELECT id, platform_id, user_id, current_nickname, message_count,"
        " first_seen, last_seen FROM members WHERE id = ?",
        (member_id,),
    )
    member = await cur.fetchone()
    if member is None:
        return None
    profile: dict[str, Any] = dict(member)

    cur = await conn.execute(
        "SELECT alias, source, first_seen, last_seen, seen_count"
        " FROM member_aliases WHERE member_id = ?"
        " ORDER BY last_seen DESC LIMIT 30",
        (member_id,),
    )
    profile["aliases"] = [dict(r) for r in await cur.fetchall()]

    cur = await conn.execute(
        "SELECT g.id AS group_ref, g.platform_name, g.group_id, g.group_name,"
        " mg.current_nick, mg.message_count, mg.first_seen, mg.last_seen"
        " FROM member_group mg JOIN groups g ON g.id = mg.group_id"
        " WHERE mg.member_id = ? ORDER BY mg.last_seen DESC",
        (member_id,),
    )
    profile["groups"] = [dict(r) for r in await cur.fetchall()]

    cur = await conn.execute(
        "SELECT id, group_raw_id, sent_at, text, event_kind FROM messages"
        " WHERE sender_ref = ? ORDER BY id DESC LIMIT 10",
        (member_id,),
    )
    profile["recent_messages"] = [dict(r) for r in await cur.fetchall()]

    # 有向互动：发出 / 收到（全量，带对方身份）
    out = await _interaction_pairs(conn, "from_member", member_id)
    inn = await _interaction_pairs(conn, "to_member", member_id)
    profile["interactions_out"] = out
    profile["interactions_in"] = inn
    return profile


async def _interaction_pairs(
    conn: aiosqlite.Connection, column: str, member_id: int
) -> list[dict[str, Any]]:
    other = "to_member" if column == "from_member" else "from_member"
    cur = await conn.execute(
        f"SELECT {other} AS other_ref, kind, COUNT(*) AS c, MAX(created_at) AS last_at"
        " FROM interactions"
        f" WHERE {column} = ? AND {other} IS NOT NULL"
        f" GROUP BY {other}, kind ORDER BY c DESC LIMIT 20",
        (member_id,),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    for row in rows:
        cur2 = await conn.execute(
            "SELECT user_id, current_nickname FROM members WHERE id = ?",
            (row["other_ref"],),
        )
        other_row = await cur2.fetchone()
        row["other_user_id"] = other_row["user_id"] if other_row else None
        row["other_nickname"] = other_row["current_nickname"] if other_row else None
    return rows


async def merge_members(
    conn: aiosqlite.Connection,
    src_id: int,
    dst_id: int,
    *,
    operator: str | None = None,
    note: str | None = None,
) -> str:
    """把 ``src`` 身份并入 ``dst``（**仅人工触发**，全量留审计）。

    默认原则是不合并（不同平台/账号是不同身份）；本操作必须显式执行，
    并在 ``member_merges`` 中留痕以便回溯。返回描述文本。
    """
    if src_id == dst_id:
        raise ValueError("源成员与目标成员相同")
    now = int(time.time())

    for mid in (src_id, dst_id):
        cur = await conn.execute("SELECT id FROM members WHERE id = ?", (mid,))
        if await cur.fetchone() is None:
            raise ValueError(f"成员不存在: {mid}")

    await conn.execute("BEGIN IMMEDIATE")
    try:
        await conn.execute(
            "INSERT INTO member_merges(src_member, dst_member, operator, note, created_at)"
            " VALUES(?,?,?,?,?)",
            (src_id, dst_id, operator, note, now),
        )
        # 1) 原始消息归属
        await conn.execute(
            "UPDATE messages SET sender_ref = ? WHERE sender_ref = ?", (dst_id, src_id)
        )
        # 2) 互动方向
        await conn.execute(
            "UPDATE interactions SET from_member = ? WHERE from_member = ?",
            (dst_id, src_id),
        )
        await conn.execute(
            "UPDATE interactions SET to_member = ? WHERE to_member = ?",
            (dst_id, src_id),
        )
        # 3) @ 目标
        await conn.execute(
            "UPDATE message_mentions SET target_ref = ? WHERE target_ref = ?",
            (dst_id, src_id),
        )
        # 4) 称呼（Phase 3）
        await conn.execute(
            "UPDATE addressing SET from_member = ? WHERE from_member = ?",
            (dst_id, src_id),
        )
        await conn.execute(
            "UPDATE addressing SET to_member = ? WHERE to_member = ?",
            (dst_id, src_id),
        )
        # 5) 别名迁移（冲突则累加计数）
        await conn.execute(
            "INSERT INTO member_aliases(member_id, alias, source, first_seen, last_seen,"
            " seen_count) SELECT ?, alias, source, first_seen, last_seen, seen_count"
            " FROM member_aliases WHERE member_id = ?"
            " ON CONFLICT(member_id, alias, source) DO UPDATE SET"
            "  first_seen = MIN(member_aliases.first_seen, excluded.first_seen),"
            "  last_seen = MAX(member_aliases.last_seen, excluded.last_seen),"
            "  seen_count = member_aliases.seen_count + excluded.seen_count",
            (dst_id, src_id),
        )
        await conn.execute("DELETE FROM member_aliases WHERE member_id = ?", (src_id,))
        # 6) 群内档案迁移
        await conn.execute(
            "INSERT INTO member_group(member_id, group_id, current_nick, first_seen,"
            " last_seen, message_count) SELECT ?, group_id, current_nick, first_seen,"
            " last_seen, message_count FROM member_group WHERE member_id = ?"
            " ON CONFLICT(member_id, group_id) DO UPDATE SET"
            "  current_nick = COALESCE(excluded.current_nick, member_group.current_nick),"
            "  first_seen = MIN(member_group.first_seen, excluded.first_seen),"
            "  last_seen = MAX(member_group.last_seen, excluded.last_seen),"
            "  message_count = member_group.message_count + excluded.message_count",
            (dst_id, src_id),
        )
        await conn.execute("DELETE FROM member_group WHERE member_id = ?", (src_id,))
        # 7) 删除源身份（其外键引用已全部迁移）
        await conn.execute("DELETE FROM members WHERE id = ?", (src_id,))
        await conn.execute("COMMIT")
    except BaseException:
        await conn.execute("ROLLBACK")
        raise

    # 发言计数以 messages 表为准重算（合并后 src 的消息已归属 dst）
    cur = await conn.execute(
        "SELECT COUNT(*) AS c FROM messages WHERE sender_ref = ? AND event_kind='message'",
        (dst_id,),
    )
    row = await cur.fetchone()
    actual = int(row["c"]) if row else 0
    await conn.execute("UPDATE members SET message_count = ? WHERE id = ?", (actual, dst_id))
    return f"已合并成员 {src_id} → {dst_id}（审计记录 member_merges）"
