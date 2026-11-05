"""测试夹具：合成 AstrBot 消息事件与临时数据目录。"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 固定 AstrBot 根目录，避免 get_astrbot_root() 取 cwd 导致数据漂移
os.environ.setdefault("ASTRBOT_ROOT", tempfile.mkdtemp(prefix="groupscope-test-"))

from astrbot.core.message.components import At, Image, Plain, Reply  # noqa: E402
from astrbot.core.platform.astr_message_event import AstrMessageEvent  # noqa: E402
from astrbot.core.platform.astrbot_message import (  # noqa: E402
    AstrBotMessage,
    Group,
    MessageMember,
)
from astrbot.core.platform.message_type import MessageType  # noqa: E402
from astrbot.core.platform.platform_metadata import PlatformMetadata  # noqa: E402

from groupscope.config import CaptureSettings  # noqa: E402
from groupscope.paths import reset_cache, plugin_data_dir  # noqa: E402
from groupscope.storage.db import Database  # noqa: E402

PLATFORM_META = PlatformMetadata(
    name="aiocqhttp",
    description="test",
    id="aiocqhttp-1",
    support_streaming_message=False,
)

_counter = {"mid": 0}


def run(coro):
    """在全新事件循环中运行协程（不依赖 pytest-asyncio）。"""
    return asyncio.run(coro)


def make_message(
    *,
    group_id: str | None = "900001",
    user_id: str = "1001",
    nickname: str = "Alice",
    text: str = "hello",
    message_id: str | None = None,
    segments: list | None = None,
    timestamp: int = 1700000000,
    raw_message=None,
    message_type: MessageType = MessageType.GROUP_MESSAGE,
    group_name: str = "测试群",
    self_id: str = "200000",
    notice_raw: dict | None = None,
) -> AstrBotMessage:
    msg = AstrBotMessage()
    msg.type = message_type
    msg.self_id = self_id
    if message_id is None:
        _counter["mid"] += 1
        message_id = f"m{_counter['mid']}"
    msg.message_id = message_id
    msg.sender = MessageMember(user_id=user_id, nickname=nickname)
    if group_id:
        msg.group = Group(group_id=group_id, group_name=group_name)
    else:
        msg.group = None
    if segments is None:
        segments = [Plain(text=text)] if text else []
    msg.message = segments
    msg.message_str = text or "".join(
        getattr(s, "text", "") for s in segments if isinstance(s, Plain)
    )
    msg.timestamp = timestamp
    msg.raw_message = notice_raw if notice_raw is not None else raw_message
    msg.session_id = group_id or user_id
    return msg


def make_event(msg: AstrBotMessage | None = None, **kwargs) -> AstrMessageEvent:
    if msg is None:
        msg = make_message(**kwargs)
    return AstrMessageEvent(
        message_str=msg.message_str,
        message_obj=msg,
        platform_meta=PLATFORM_META,
        session_id=msg.session_id,
    )


def make_settings(**overrides) -> CaptureSettings:
    base = {
        "capture_enabled": True,
        "group_whitelist": ["aiocqhttp:900001"],
        "group_blacklist": [],
        "excluded_users": [],
        "record_notices": True,
        "batch_max_rows": 50,
        "batch_max_interval_ms": 20,
        "queue_maxsize": 64,
    }
    base.update(overrides)
    return CaptureSettings.from_conf(base)


@pytest.fixture()
def db(tmp_path):
    """临时数据库（迁移已应用），用例结束自动关闭。"""

    async def _open():
        database = Database(tmp_path / "groupscope.db")
        await database.open()
        return database

    database = run(_open())
    yield database
    run(database.close()) if database.opened else None


@pytest.fixture()
def data_dir(tmp_path):
    reset_cache()
    path = plugin_data_dir("groupscope-test")
    yield path
    reset_cache()
