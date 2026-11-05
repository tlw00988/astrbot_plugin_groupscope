"""隐私与采集范围控制（Phase 1：白名单/黑名单/排除名单/暂停集合）。"""

from __future__ import annotations

from ..config import CaptureSettings


def split_entry(entry: str) -> tuple[str | None, str]:
    """``platform:id`` -> ``(platform, id)``；纯 id -> ``(None, id)``。"""
    if ":" in entry:
        platform, _, value = entry.partition(":")
        platform = platform.strip() or None
        value = value.strip()
        return platform, value
    return None, entry.strip()


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
    ) -> bool:
        """白名单准入判定（任何一项不满足即拒绝）。"""
        if not self.settings.capture_enabled:
            return False
        if not group_raw_id:  # Phase 1 只采集群聊
            return False
        if is_notice and not self.settings.record_notices:
            return False
        if self._match(self._blacklist, platform_name, platform_id, group_raw_id):
            return False
        if not self._match(self._whitelist, platform_name, platform_id, group_raw_id):
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
        value: str,
    ) -> bool:
        for plat, entry_value in entries:
            if entry_value != value:
                continue
            if plat is None or plat in (platform_name, platform_id):
                return True
        return False
