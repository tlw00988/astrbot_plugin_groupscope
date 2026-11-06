"""关系窗口快照：把确定性互动按日/周/月/季聚合成可查询的有向统计。

**只存计数与证据量，不存“亲密分”**：``confidence`` 是证据强度
（证据条数折算的 0~1 值），用于区分“有据可查”与“样本过少”，
禁止据此给人物关系排序。
"""

from __future__ import annotations

import math
import time
from typing import Any

import aiosqlite

from ..storage import tx

WINDOWS: dict[str, int] = {
    "day": 86400,
    "week": 604800,
    "month": 2592000,
    "quarter": 7776000,
}

WINDOW_LABELS = {"day": "日", "week": "周", "month": "月", "quarter": "季"}


def window_start(ts: int, window: str) -> int:
    """把时间戳对齐到窗口起点（本地时区：日界 0 点、周一、1 号、季度首日）。"""
    if window not in WINDOWS:
        window = "week"
    if window == "day":
        local = time.localtime(ts)
        return int(time.mktime((local.tm_year, local.tm_mon, local.tm_mday, 0, 0, 0, 0, 0, -1)))
    if window == "week":
        local = time.localtime(ts)
        base = time.mktime((local.tm_year, local.tm_mon, local.tm_mday, 0, 0, 0, 0, 0, -1))
        return int(base - local.tm_wday * 86400)
    if window == "month":
        local = time.localtime(ts)
        return int(time.mktime((local.tm_year, local.tm_mon, 1, 0, 0, 0, 0, 0, -1)))
    local = time.localtime(ts)
    month = ((local.tm_mon - 1) // 3) * 3 + 1
    return int(time.mktime((local.tm_year, month, 1, 0, 0, 0, 0, 0, -1)))


def next_window_start(start: int, window: str) -> int:
    return window_start(start + WINDOWS.get(window, 604800), window)


def confidence_for(evidence: int) -> float:
    """证据强度：0 条=0，10 条及以上=1.0（渐近逼近）。"""
    if evidence <= 0:
        return 0.0
    return round(min(1.0, 1 - math.exp(-evidence / 5.0)), 4)


async def compute_snapshots(
    conn: aiosqlite.Connection,
    *,
    group_ref: int | None = None,
    window: str = "week",
    since: int | None = None,
    until: int | None = None,
) -> int:
    """按窗口聚合互动，写入/更新 ``relationship_snapshots``，返回处理的窗口数。"""
    if window not in WINDOWS:
        raise ValueError(f"未知窗口: {window}")
    now = int(time.time())
    until = until if until is not None else now
    start = window_start(since if since is not None else since_window_default(now, window), window)

    windows_done = 0
    while start < until:
        end = next_window_start(start, window)
        await _compute_one_window(conn, group_ref, window, start, min(end, until))
        windows_done += 1
        start = end
        if windows_done > 400:  # 防御：避免异常参数导致死循环
            break
    return windows_done


def since_window_default(now: int, window: str) -> int:
    """默认回溯 4 个窗口。"""
    step = WINDOWS.get(window, 604800)
    return window_start(now - step * 4, window)


async def _compute_one_window(
    conn: aiosqlite.Connection,
    group_ref: int | None,
    window: str,
    start: int,
    end: int,
) -> None:
    sql = (
        "SELECT from_member, to_member,"
        " SUM(CASE WHEN kind='at' THEN 1 ELSE 0 END) AS at_c,"
        " SUM(CASE WHEN kind='reply' THEN 1 ELSE 0 END) AS reply_c,"
        " SUM(CASE WHEN kind='quote' THEN 1 ELSE 0 END) AS quote_c,"
        " COUNT(*) AS evidence"
        " FROM interactions"
        " WHERE created_at >= ? AND created_at < ?"
        "   AND from_member IS NOT NULL AND to_member IS NOT NULL"
    )
    params: list[Any] = [start, end]
    if group_ref is not None:
        sql += " AND group_ref = ?"
        params.append(group_ref)
    sql += " GROUP BY from_member, to_member"
    cur = await conn.execute(sql, params)
    pairs = await cur.fetchall()

    # 背景发言量（仅作参照）
    msg_sql = (
        "SELECT sender_ref, COUNT(*) AS c FROM messages"
        " WHERE event_kind='message' AND sent_at >= ? AND sent_at < ?"
        "   AND sender_ref IS NOT NULL"
    )
    msg_params: list[Any] = [start, end]
    if group_ref is not None:
        msg_sql += " AND group_ref = ?"
        msg_params.append(group_ref)
    msg_sql += " GROUP BY sender_ref"
    cur = await conn.execute(msg_sql, msg_params)
    msg_counts = {int(r["sender_ref"]): int(r["c"]) for r in await cur.fetchall()}

    now = int(time.time())
    await tx.begin(conn)
    try:
        for row in pairs:
            frm = int(row["from_member"])
            to = int(row["to_member"])
            evidence = int(row["evidence"])
            await conn.execute(
                "INSERT INTO relationship_snapshots("
                " group_ref, window, window_start, window_end, from_member, to_member,"
                " at_count, reply_count, quote_count, from_msgs, to_msgs,"
                " evidence_count, confidence, computed_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(group_ref, window, window_start, from_member, to_member)"
                " DO UPDATE SET"
                "  window_end=excluded.window_end,"
                "  at_count=excluded.at_count, reply_count=excluded.reply_count,"
                "  quote_count=excluded.quote_count, from_msgs=excluded.from_msgs,"
                "  to_msgs=excluded.to_msgs, evidence_count=excluded.evidence_count,"
                "  confidence=excluded.confidence, computed_at=excluded.computed_at",
                (
                    group_ref,
                    window,
                    start,
                    end,
                    frm,
                    to,
                    int(row["at_c"]),
                    int(row["reply_c"]),
                    int(row["quote_c"]),
                    msg_counts.get(frm, 0),
                    msg_counts.get(to, 0),
                    evidence,
                    confidence_for(evidence),
                    now,
                ),
            )
        await tx.commit(conn)
    except BaseException:
        await tx.rollback(conn)
        raise


async def snapshot_windows(
    conn: aiosqlite.Connection,
    *,
    group_ref: int | None,
    window: str = "week",
) -> list[dict[str, Any]]:
    """读取**最新窗口**的全部有向关系边，按证据量降序。"""
    base = "SELECT MAX(window_start) AS s FROM relationship_snapshots WHERE window = ?"
    params: list[Any] = [window]
    if group_ref is not None:
        base += " AND group_ref = ?"
        params.append(group_ref)
    cur = await conn.execute(base, params)
    row = await cur.fetchone()
    if not row or row["s"] is None:
        return []
    latest = int(row["s"])
    sql = (
        "SELECT window_start, window_end, from_member, to_member, at_count,"
        " reply_count, quote_count, from_msgs, to_msgs, evidence_count, confidence"
        " FROM relationship_snapshots"
        " WHERE window = ? AND window_start = ?"
    )
    qparams: list[Any] = [window, latest]
    if group_ref is not None:
        sql += " AND group_ref = ?"
        qparams.append(group_ref)
    sql += " ORDER BY evidence_count DESC"
    cur = await conn.execute(sql, qparams)
    return [dict(r) for r in await cur.fetchall()]
