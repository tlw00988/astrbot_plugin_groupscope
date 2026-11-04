"""事件规范化：AstrMessageEvent -> NormalizedMessage。

要点：
* 消息段使用 pydantic v1 兼容层的 ``.json()`` 全量序列化（已实测
  ``model_dump`` 不存在，``toDict()`` 会丢字段），失败时逐级降级，
  绝不因单个消息段失败丢弃整条消息。
* 撤回/戳一戳等通知事件没有统一事件类型，通过平台原始对象中的
  ``notice_type`` 识别，只保存可观测内容，不做推断。
* 去重键：普通消息用 ``platform_id + message_id``；通知事件用
  剔除时间字段后的内容指纹（其 message_id 由适配器随机生成）。
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from typing import Any

from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.core.platform.astrbot_message import AstrBotMessage
from astrbot.core.platform.message_type import MessageType

from .models import Mention, NormalizedMessage, Segment

RAW_JSON_CAP = 64 * 1024
PAYLOAD_CAP = 64 * 1024

_MESSAGE_TYPE_MAP = {
    MessageType.GROUP_MESSAGE: "group",
    MessageType.FRIEND_MESSAGE: "friend",
    MessageType.OTHER_MESSAGE: "other",
}


def _message_type_name(msg: AstrBotMessage) -> str:
    try:
        return _MESSAGE_TYPE_MAP.get(msg.type, "other")
    except Exception:
        return "other"


def serialize_segment(seg: Any) -> tuple[str, str | None]:
    """返回 ``(seg_type, payload_json)``，带多级降级。"""
    seg_type = type(seg).__name__
    try:
        payload = seg.json()  # pydantic v1 API：全量字段 + Enum 转字符串
        scrubbed = _scrub_base64(payload)
        if len(scrubbed) > PAYLOAD_CAP:
            scrubbed = json.dumps(
                {"type": seg_type, "truncated": True, "bytes": len(scrubbed)},
                ensure_ascii=False,
            )
        return seg_type, scrubbed
    except Exception:
        pass
    try:
        data = json.dumps(
            seg.dict(), ensure_ascii=False, default=str  # pydantic v1
        )
        if len(data) > PAYLOAD_CAP:
            data = json.dumps({"type": seg_type, "truncated": True})
        return seg_type, data
    except Exception:
        return seg_type, json.dumps(
            {"type": seg_type, "raw": str(seg)[:2000]}, ensure_ascii=False
        )


def _scrub_base64(payload_str: str) -> str:
    """去掉内联 base64 附件内容（只保留元数据）。"""
    if "base64://" not in payload_str:
        return payload_str
    try:
        data = json.loads(payload_str)
    except Exception:
        return payload_str.replace("base64://", "base64://<omitted>")
    changed = False

    def walk(node: Any) -> None:
        nonlocal changed
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str) and value.startswith("base64://"):
                    node[key] = "base64://<omitted>"
                    changed = True
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(data)
    if not changed:
        return payload_str
    return json.dumps(data, ensure_ascii=False)


def _raw_json(raw: Any) -> str | None:
    if raw is None:
        return None
    try:
        text = json.dumps(raw, ensure_ascii=False, default=str)
    except Exception:
        try:
            text = json.dumps(str(raw), ensure_ascii=False)
        except Exception:
            return None
    if len(text) > RAW_JSON_CAP:
        return text[:RAW_JSON_CAP] + "…<truncated>"
    return text


def _notice_info(msg: AstrBotMessage) -> tuple[bool, str | None]:
    raw = msg.raw_message
    if isinstance(raw, Mapping):
        notice_type = raw.get("notice_type")
        if isinstance(notice_type, str) and notice_type:
            sub = raw.get("sub_type")
            if isinstance(sub, str) and sub and notice_type == "group_recall":
                return True, notice_type
            return True, notice_type
        if raw.get("post_type") == "notice":
            return True, "notice"
    return False, None


def _notice_fingerprint(platform_id: str, raw: Any) -> str:
    if isinstance(raw, Mapping):
        data = {k: v for k, v in raw.items() if k != "time"}
        try:
            blob = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            blob = str(sorted(data.items(), key=lambda kv: str(kv[0])))
    else:
        blob = str(raw)
    digest = hashlib.sha1(blob.encode("utf-8", "replace")).hexdigest()
    return f"n:{platform_id}:{digest}"


def _content_fingerprint(platform_id: str, msg: AstrBotMessage) -> str:
    blob = f"{msg.sender.user_id if msg.sender else ''}|{msg.timestamp}|{msg.message_str}"
    return f"c:{platform_id}:{hashlib.sha1(blob.encode('utf-8', 'replace')).hexdigest()}"


def normalize_event(event: AstrMessageEvent) -> NormalizedMessage | None:
    """把事件规范化为档案记录；无法识别的事件返回 None。"""
    msg = event.message_obj
    now = int(time.time())
    received_at = now

    platform_id = str(event.get_platform_id() or "")
    platform_name = str(event.get_platform_name() or "")
    group_raw_id = event.get_group_id() or None
    group_name = None
    group = getattr(msg, "group", None)
    if group is not None:
        group_name = getattr(group, "group_name", None)

    sender_id = None
    sender_name = None
    sender = getattr(msg, "sender", None)
    if sender is not None:
        sender_id = str(getattr(sender, "user_id", "") or "") or None
        sender_name = getattr(sender, "nickname", None)
        if sender_name is not None:
            sender_name = str(sender_name) or None

    is_notice, notice_type = _notice_info(msg)

    sent_at = int(getattr(msg, "timestamp", 0) or 0) or received_at

    segments: list[Segment] = []
    mentions: list[Mention] = []
    reply_to_message_id = None
    reply_to_sender_id = None

    chain = list(getattr(msg, "message", None) or [])
    for idx, seg in enumerate(chain):
        seg_type, payload = serialize_segment(seg)
        if isinstance(seg, Plain):
            text = seg.text
        else:
            text = None
        segments.append(Segment(idx=idx, seg_type=seg_type, payload_json=payload, text=text))
        if isinstance(seg, At):
            qq = str(getattr(seg, "qq", "") or "")
            mentions.append(
                Mention(kind="at_all" if qq == "all" else "at", target_raw_id=qq or None)
            )
        if reply_to_message_id is None and isinstance(seg, Reply):
            rid = getattr(seg, "id", None)
            if rid not in (None, "", 0, "0"):
                reply_to_message_id = str(rid)
            sid = getattr(seg, "sender_id", None)
            if sid not in (None, "", 0, "0"):
                reply_to_sender_id = str(sid)

    message_id = str(getattr(msg, "message_id", "") or "")
    if is_notice:
        if not message_id:
            message_id = hashlib.sha1(
                f"{group_raw_id}|{notice_type}|{sent_at}".encode()
            ).hexdigest()[:16]
        dedup_key = _notice_fingerprint(platform_id, msg.raw_message)
        event_kind = "notice"
    else:
        if not message_id:
            dedup_key = _content_fingerprint(platform_id, msg)
        else:
            dedup_key = f"m:{platform_id}:{message_id}"
        event_kind = "message"

    text = getattr(msg, "message_str", None)
    if text is None:
        text = ""
    text = str(text)
    if not text:
        text = None

    return NormalizedMessage(
        dedup_key=dedup_key,
        platform_id=platform_id,
        platform_name=platform_name,
        group_raw_id=str(group_raw_id) if group_raw_id else None,
        group_name=str(group_name) if group_name else None,
        sender_raw_id=sender_id,
        sender_raw_name=sender_name,
        event_kind=event_kind,
        notice_type=notice_type,
        sent_at=sent_at,
        received_at=received_at,
        message_type=_message_type_name(msg),
        text=text,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        reply_to_sender_id=reply_to_sender_id,
        raw_json=_raw_json(getattr(msg, "raw_message", None)),
        segments=segments,
        mentions=mentions,
    )
