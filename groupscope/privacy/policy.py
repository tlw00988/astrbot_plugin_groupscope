"""隐私与采集范围控制（Phase 1：白名单/黑名单/排除名单/暂停缓存）。"""

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
    """

    def __init__(self, settings: CaptureSettings) -> None:
        self.settings = settings
        self._whitelist = [split_entry(e) for e in settings.group_whitelist]
        self._blacklist = [split_entry(e) for e in settings.group_blacklist]
        self._excluded = [split_entry(e) for e in settings.excluded_users]
        # group_ref -> 暂停标记，由服务层从 groups.capture_enabled 加载并维护
        self.paused_group_refs: set[int] = set()

    def allow_event(
        self,
        *,
        platform_name: str,
        platform_id: str,
        group_raw_id: str | None,
        sender_raw_id: str | None,
        event_kind: str,
        group_ref: int | None,
    ) -> bool:
        """白名单准入判定（任何一项不满足即拒绝）。"""
        if not self.settings.capture_enabled:
            return False
        if not group_raw_id:  # Phase 1 只采集群聊
            return False
        if event_kind == "notice" and not self.settings.record_notices:
            return False
        if self._match(self._blacklist, platform_name, platform_id, group_raw_id):
            return False
        if not self._match(self._whitelist, platform_name, platform_id, group_raw_id):
            return False
        if group_ref is not None and group_ref in self.paused_group_refs:
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
