"""规范化消息模型：采集层与存储层之间的唯一契约。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Segment:
    idx: int
    seg_type: str
    payload_json: str
    text: str | None = None


@dataclass
class Mention:
    kind: str  # at / at_all
    target_raw_id: str | None


@dataclass
class NormalizedMessage:
    dedup_key: str
    platform_id: str
    platform_name: str
    group_raw_id: str | None
    group_name: str | None
    sender_raw_id: str | None
    sender_raw_name: str | None
    event_kind: str  # message / notice / unknown
    notice_type: str | None
    sent_at: int
    received_at: int
    message_type: str  # group / friend / other
    text: str | None
    message_id: str
    reply_to_message_id: str | None = None
    reply_to_sender_id: str | None = None
    raw_json: str | None = None
    segments: list[Segment] = field(default_factory=list)
    mentions: list[Mention] = field(default_factory=list)
