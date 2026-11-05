"""数据导出：JSON / CSV（流式分页，不整表进内存）。"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite

from ..storage import repo
from ..storage.db import Database

EXPORT_COLUMNS = [
    "message_id",
    "platform_id",
    "platform_name",
    "group_id",
    "sender_id",
    "sender_name",
    "event_kind",
    "notice_type",
    "sent_at",
    "sent_at_iso",
    "message_type",
    "text",
    "reply_to_message_id",
    "reply_to_sender_id",
    "segments",
]


async def _iter_rows(
    conn: aiosqlite.Connection, group_ref: int | None, page: int = 500
) -> AsyncIterator[dict[str, Any]]:
    last_id = 0
    while True:
        sql = (
            "SELECT m.id, m.message_id, m.platform_id, m.platform_name,"
            " m.group_raw_id, m.sender_raw_id, m.sender_raw_name, m.event_kind,"
            " m.notice_type, m.sent_at, m.message_type, m.text,"
            " m.reply_to_message_id, m.reply_to_sender_id"
            " FROM messages m WHERE m.id > ?"
        )
        params: list[Any] = [last_id]
        if group_ref is not None:
            sql += " AND m.group_ref = ?"
            params.append(group_ref)
        sql += " ORDER BY m.id LIMIT ?"
        params.append(page)
        cur = await conn.execute(sql, params)
        rows = await cur.fetchall()
        if not rows:
            return
        for row in rows:
            record = dict(row)
            last_id = int(record.pop("id"))
            segments = await repo.segments_of(conn, int(last_id))
            record["segments"] = [
                {
                    "index": seg["seg_index"],
                    "type": seg["seg_type"],
                    "payload": json.loads(seg["payload_json"]),
                    **({"text": seg["text"]} if seg["text"] else {}),
                }
                for seg in segments
            ]
            record["group_raw_id"] = record.pop("group_raw_id")
            yield record


def _flatten(record: dict[str, Any]) -> dict[str, Any]:
    out = {
        "message_id": record.get("message_id"),
        "platform_id": record.get("platform_id"),
        "platform_name": record.get("platform_name"),
        "group_id": record.get("group_raw_id"),
        "sender_id": record.get("sender_raw_id"),
        "sender_name": record.get("sender_raw_name"),
        "event_kind": record.get("event_kind"),
        "notice_type": record.get("notice_type"),
        "sent_at": record.get("sent_at"),
        "sent_at_iso": time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(record.get("sent_at") or 0)
        ),
        "message_type": record.get("message_type"),
        "text": record.get("text"),
        "reply_to_message_id": record.get("reply_to_message_id"),
        "reply_to_sender_id": record.get("reply_to_sender_id"),
        "segments": json.dumps(
            record.get("segments") or [], ensure_ascii=False
        ),
    }
    return out


async def export_messages(
    db: Database, dest_dir: Path, fmt: str, group_ref: int | None = None
) -> Path:
    """导出消息（含消息段）。``fmt`` 支持 ``json`` / ``csv``。"""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    fmt = fmt.lower()
    if fmt not in ("json", "csv"):
        raise ValueError(f"不支持的导出格式: {fmt}")
    stamp = time.strftime("%Y%m%d-%H%M%S")
    dest = dest_dir / f"messages-{stamp}.{fmt}"

    conn = db.conn
    if fmt == "json":
        with dest.open("w", encoding="utf-8") as fh:
            fh.write("[\n")
            first = True
            async for record in _iter_rows(conn, group_ref):
                chunk = json.dumps(record, ensure_ascii=False)
                fh.write(("" if first else ",\n") + chunk)
                first = False
            fh.write("\n]\n")
    else:
        with dest.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()
            async for record in _iter_rows(conn, group_ref):
                writer.writerow(_flatten(record))
    return dest
