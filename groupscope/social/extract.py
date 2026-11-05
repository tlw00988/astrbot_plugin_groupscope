"""从一条规范化消息中提取**确定性互动**（@ / 回复 / 引用）。

只做可观测事实的抽取，不做任何推断：
* ``At`` 段 → ``kind='at'``
* ``Reply`` 段带被引原文（``chain``/``message_str``）→ ``kind='quote'``
* ``Reply`` 段只有消息 ID → ``kind='reply'``

目标成员的解析交给调用方提供的 ``resolve``（查得到就是成员，查不到只留 raw id），
保证“对方尚未在本库出现过”时互动记录依然完整。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from ..capture.models import NormalizedMessage

KIND_AT = "at"
KIND_REPLY = "reply"
KIND_QUOTE = "quote"


def _reply_has_text(payload_json: str) -> bool:
    """判断 Reply 段是否携带被引用原文（区分“引用”与“纯回复”）。"""
    try:
        data = json.loads(payload_json)
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    if data.get("chain"):
        return True
    return bool(data.get("message_str") or data.get("text"))


def extract_interactions(
    item: NormalizedMessage,
    *,
    message_ref: int,
    group_ref: int | None,
    from_member: int | None,
    from_raw_id: str | None,
    resolve: Callable[[str], int | None] | None = None,
    created_at: int | None = None,
) -> list[dict[str, Any]]:
    """返回可直接写入 ``interactions`` 的记录列表。

    ``resolve`` 用于把目标 raw id 解析成成员主键；传 ``None`` 或解析不到时
    只保留 ``to_raw_id``（互动事实不因对方尚未入库而丢失）。
    """
    if item.event_kind != "message":
        return []
    ts = created_at if created_at is not None else item.received_at
    rows: list[dict[str, Any]] = []

    def _add(kind: str, to_raw_id: str | None) -> None:
        if not to_raw_id:
            return
        to_member = None
        if resolve is not None and to_raw_id != "all":
            to_member = resolve(to_raw_id)
        rows.append(
            {
                "group_ref": group_ref,
                "from_member": from_member,
                "from_raw_id": from_raw_id,
                "to_member": to_member,
                "to_raw_id": to_raw_id,
                "kind": kind,
                "message_ref": message_ref,
                "created_at": ts,
            }
        )

    for mention in item.mentions:
        _add(KIND_AT, mention.target_raw_id)

    if item.reply_to_message_id:
        has_text = any(
            seg.seg_type == "Reply" and _reply_has_text(seg.payload_json)
            for seg in item.segments
        )
        _add(KIND_QUOTE if has_text else KIND_REPLY, item.reply_to_sender_id)

    return rows


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同一条消息里重复的 (kind, to_raw_id) 只保留一条。"""
    seen: set[tuple[str, str | None]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (row["kind"], row.get("to_raw_id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out
