"""事件规范化：段全量保真、@/引用抽取、去重键、脱敏与降级。"""

from __future__ import annotations

import json

from astrbot.core.message.components import At, Image, Plain, Reply
from astrbot.core.platform.message_type import MessageType

from conftest import make_event, make_message
from groupscope.capture.serializer import (
    PAYLOAD_CAP,
    RAW_JSON_CAP,
    normalize_event,
    serialize_segment,
)


def test_plain_segment_full_fidelity():
    """Plain 段必须用 .json() 全量序列化，且 text 字段单独可查。"""
    item = normalize_event(make_event())
    assert item.text == "hello"
    assert len(item.segments) == 1
    seg = item.segments[0]
    assert seg.seg_type == "Plain"
    assert seg.text == "hello"
    payload = json.loads(seg.payload_json)
    assert payload["text"] == "hello"
    assert payload["type"] == "Plain"


def test_multi_segment_and_dedup_key():
    msg = make_message(
        segments=[
            Plain(text="hi "),
            At(qq=200001),
            Image(file="http://x/y.png"),
        ],
        message_id="42",
    )
    item = normalize_event(make_event(msg))
    assert [s.seg_type for s in item.segments] == ["Plain", "At", "Image"]
    assert item.segments[1].payload_json  # At 段不丢字段
    assert item.dedup_key == "m:aiocqhttp-1:42"


def test_at_and_at_all_mentions():
    msg = make_message(segments=[At(qq=200001), At(qq="all"), Plain(text="x")])
    item = normalize_event(make_event(msg))
    assert [(m.kind, m.target_raw_id) for m in item.mentions] == [
        ("at", "200001"),
        ("at_all", "all"),
    ]


def test_reply_extraction():
    msg = make_message(segments=[Reply(id=98765, sender_id=200009), Plain(text="回你")])
    item = normalize_event(make_event(msg))
    assert item.reply_to_message_id == "98765"
    assert item.reply_to_sender_id == "200009"


def test_base64_payload_scrubbed():
    """内联 base64 附件内容必须被洗除，只保留元数据标记。"""
    seg = Image(file="base64://" + "A" * 500)
    seg_type, payload = serialize_segment(seg)
    assert seg_type == "Image"
    assert "AAAA" not in payload
    assert "base64://<omitted>" in payload


def test_oversize_payload_truncated_marker():
    seg = Plain(text="x" * (PAYLOAD_CAP + 100))
    seg_type, payload = serialize_segment(seg)
    data = json.loads(payload)
    assert data.get("truncated") is True
    assert len(payload) < PAYLOAD_CAP


def test_broken_segment_degrades_not_raises():
    """段序列化失败时降级为可诊断文本，绝不抛异常丢整条消息。"""

    class Broken:
        def json(self):
            raise RuntimeError("boom")

        def dict(self):
            raise RuntimeError("boom")

    seg_type, payload = serialize_segment(Broken())
    assert seg_type == "Broken"
    assert json.loads(payload)["raw"]


def test_raw_json_capped():
    raw = {"post_type": "message", "blob": "z" * (RAW_JSON_CAP + 10)}
    item = normalize_event(make_event(make_message(raw_message=raw)))
    assert item.raw_json is not None
    assert len(item.raw_json) <= RAW_JSON_CAP + 20
    assert item.raw_json.endswith("…<truncated>")


def test_notice_detection_and_dedup():
    """通知事件（撤回）识别为 notice，去重键与普通消息不同且与时间无关。"""
    notice = {
        "post_type": "notice",
        "notice_type": "group_recall",
        "group_id": 900001,
        "user_id": 1001,
        "operator_id": 1001,
        "message_id": 555,
        "time": 1700000000,
    }
    msg = make_message(notice_raw=notice, text="", segments=[], message_id="")
    item = normalize_event(make_event(msg))
    assert item.event_kind == "notice"
    assert item.notice_type == "group_recall"
    assert item.dedup_key.startswith("n:aiocqhttp-1:")

    # 去掉 time 字段后指纹不变（同一事件重复投递会被去重）
    notice2 = dict(notice, time=1700009999)
    item2 = normalize_event(make_event(make_message(notice_raw=notice2, text="", segments=[], message_id="")))
    assert item2.dedup_key == item.dedup_key


def test_private_chat_has_no_group():
    msg = make_message(
        group_id=None,
        message_type=MessageType.FRIEND_MESSAGE,
        text="私聊",
    )
    item = normalize_event(make_event(msg))
    assert item.group_raw_id is None
    assert item.message_type == "friend"


def test_missing_message_id_falls_back_to_content_fingerprint():
    msg = make_message(message_id="", timestamp=1700000000)
    item = normalize_event(make_event(msg))
    assert item.dedup_key.startswith("c:aiocqhttp-1:")
    # 同内容同时刻 → 同指纹（可去重）
    item2 = normalize_event(make_event(make_message(message_id="", timestamp=1700000000)))
    assert item2.dedup_key == item.dedup_key
