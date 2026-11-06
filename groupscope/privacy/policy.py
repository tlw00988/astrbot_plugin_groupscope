"""隐私与采集范围控制（Phase 1：白名单/黑名单/排除名单/暂停集合）。"""

from __future__ import annotations

from ..config import CaptureSettings

# AstrBot 的 MessageType 取值（``astrbot.core.platform.message_type``），
# 用于识别 unified_msg_origin（UMO）中间那一段。
_UMO_MESSAGE_TYPES = frozenset({"GroupMessage", "FriendMessage", "OtherMessage"})


def split_entry(entry: str) -> tuple[str | None, str]:
    """解析白/黑名单条目，支持三种写法（第 1 种可从 WebUI 直接粘贴）：

    1. **UMO** —— AstrBot 自带白名单用的 ``平台:消息类型:会话ID``，例如
       日志与 WebUI「访问控制 → 白名单」里显示的
       ``default:GroupMessage:1013637348``，解析为 ``("default", "1013637348")``：
       去掉中间的消息类型后与写法 2 等价，末段就是群/用户 ID。
    2. ``platform:id`` -> ``(platform, id)``
    3. 纯 ``id``      -> ``(None, id)``（匹配所有平台）
    """
    text = entry.strip()
    if text.count(":") >= 2:
        platform, message_type, value = (p.strip() for p in text.split(":", 2))
        if message_type in _UMO_MESSAGE_TYPES:
            return platform or None, value
    if ":" in text:
        platform, _, value = text.partition(":")
        return (platform.strip() or None), value.strip()
    return None, text


class CapturePolicy:
    """决定一条事件是否允许被采集。

    默认 **白名单为空 = 不采集任何群**，插件装上即符合最小采集原则。
    判定全程只读内存（白名单/黑名单/暂停集合），不产生数据库查询，
    保证事件回调的开销可控。
    """

    def __init__(self, settings: CaptureSettings) -> None:
        self.settings = settings
        self._whitelist = [split_entry(e) for e in settings.group_whitelist]
        self._blacklist = [split_entry(e) for e in settings.group_blacklist]
        self._excluded = [split_entry(e) for e in settings.excluded_users]
        # 运行期暂停集合：(platform_id, group_raw_id)
        self.paused: set[tuple[str, str]] = set()

    def allow(
        self,
        *,
        platform_name: str,
        platform_id: str,
        group_raw_id: str | None,
        sender_raw_id: str | None,
        is_notice: bool,
        session_id: str | None = None,
    ) -> bool:
        """白名单准入判定（任何一项不满足即拒绝）。

        ``session_id`` 是会话 ID（= UMO 的末段）：从 AstrBot 白名单粘贴来的
        条目末段是它，某些平台上它与群 ID 不同，因此两个都参与比对。
        """
        if not self.settings.capture_enabled:
            return False
        if not group_raw_id:  # Phase 1 只采集群聊
            return False
        if is_notice and not self.settings.record_notices:
            return False
        group_keys = (group_raw_id, session_id)
        if self._match(self._blacklist, platform_name, platform_id, *group_keys):
            return False
        if not self._match(
            self._whitelist, platform_name, platform_id, *group_keys
        ):
            return False
        if (platform_id, group_raw_id) in self.paused:
            return False
        if sender_raw_id and self._match(
            self._excluded, platform_name, platform_id, sender_raw_id
        ):
            return False
        return True

    @staticmethod
    def _match(
        entries: list[tuple[str | None, str]],
        platform_name: str,
        platform_id: str,
        *values: str | None,
    ) -> bool:
        """任一候选值命中即算匹配；限定了平台时还必须平台一致。"""
        wanted = {v for v in values if v}
        if not wanted:
            return False
        for plat, entry_value in entries:
            if entry_value not in wanted:
                continue
            if plat is None or plat in (platform_name, platform_id):
                return True
        return False
