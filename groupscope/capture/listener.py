"""事件回调入口：隐私过滤 -> 规范化 -> 入队（全程异常隔离）。

这里是插件主流程上唯一的常驻 handler，必须满足：
* 普通协程（不 ``yield``、不设置消息结果），不干预 AstrBot 管道；
* 任何异常都被吞掉并记日志，绝不外抛——AstrBot 对 handler 异常会
  ``stop_event()``，中断机器人的正常回复。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..privacy import CapturePolicy
from .models import NormalizedMessage
from .serializer import notice_info, normalize_event


class MessageListener:
    def __init__(
        self,
        policy: CapturePolicy,
        enqueue: Callable[[NormalizedMessage], bool],
        logger: Any,
        rejected: Callable[[], None] | None = None,
    ) -> None:
        self.policy = policy
        self._enqueue = enqueue
        self.log = logger
        self._rejected = rejected

    async def handle_event(self, event: Any) -> bool:
        try:
            return self._handle(event)
        except Exception:
            # 绝不外抛：AstrBot 会因 handler 异常停止事件传播
            self.log.exception("GroupScope 采集事件失败（已忽略）")
            return False

    def _handle(self, event: Any) -> bool:
        msg = event.message_obj
        platform_id = str(event.get_platform_id() or "")
        platform_name = str(event.get_platform_name() or "")
        group_raw_id = event.get_group_id() or None
        sender = getattr(msg, "sender", None)
        sender_raw_id = str(getattr(sender, "user_id", "") or "") or None

        is_notice, _ = notice_info(msg)
        if not self.policy.allow(
            platform_name=platform_name,
            platform_id=platform_id,
            group_raw_id=str(group_raw_id) if group_raw_id else None,
            sender_raw_id=sender_raw_id,
            is_notice=is_notice,
        ):
            if self._rejected:
                self._rejected()
            return False

        item = normalize_event(event)
        if item is None:
            if self._rejected:
                self._rejected()
            return False
        return bool(self._enqueue(item))  # 队列满时 enqueue 返回 False


__all__ = ["MessageListener"]
