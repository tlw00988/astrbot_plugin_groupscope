"""插件配置读取与校验。

配置来源是 AstrBot 依据 ``_conf_schema.json`` 生成的 ``AstrBotConfig``
（dict 子类），实体文件位于 ``data/config/<plugin_name>_config.json``。
AstrBot 会在 Schema 变更时自动补默认值，但这里仍对每个字段做防御性取值，
保证配置文件被手工改坏时插件仍可运行。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ATTACHMENT_POLICIES = ("none", "metadata_only", "download_if_small")


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


@dataclass
class CaptureSettings:
    """一次读取、全程使用的采集配置快照。"""

    capture_enabled: bool = True
    group_whitelist: list[str] = field(default_factory=list)
    group_blacklist: list[str] = field(default_factory=list)
    excluded_users: list[str] = field(default_factory=list)
    record_notices: bool = True
    attachment_policy: str = "metadata_only"
    attachment_max_bytes: int = 2 * 1024 * 1024
    batch_max_rows: int = 100
    batch_max_interval_ms: int = 500
    queue_maxsize: int = 10000
    log_retention_days: int = 0
    backup_auto: bool = False
    backup_keep: int = 7
    log_detail: bool = False

    @classmethod
    def from_conf(cls, conf: Any) -> "CaptureSettings":
        if not isinstance(conf, dict):
            conf = {}
        policy = str(conf.get("attachment_policy", "metadata_only"))
        if policy not in ATTACHMENT_POLICIES:
            policy = "metadata_only"
        return cls(
            capture_enabled=_as_bool(conf.get("capture_enabled"), True),
            group_whitelist=_as_str_list(conf.get("group_whitelist")),
            group_blacklist=_as_str_list(conf.get("group_blacklist")),
            excluded_users=_as_str_list(conf.get("excluded_users")),
            record_notices=_as_bool(conf.get("record_notices"), True),
            attachment_policy=policy,
            attachment_max_bytes=max(
                0, _as_int(conf.get("attachment_max_bytes"), 2 * 1024 * 1024)
            ),
            batch_max_rows=max(1, _as_int(conf.get("batch_max_rows"), 100)),
            batch_max_interval_ms=max(
                10, _as_int(conf.get("batch_max_interval_ms"), 500)
            ),
            queue_maxsize=max(16, _as_int(conf.get("queue_maxsize"), 10000)),
            log_retention_days=max(0, _as_int(conf.get("log_retention_days"), 0)),
            backup_auto=_as_bool(conf.get("backup_auto"), False),
            backup_keep=max(1, _as_int(conf.get("backup_keep"), 7)),
            log_detail=_as_bool(conf.get("log_detail"), False),
        )


@dataclass
class AnalysisSettings:
    """Phase 3 分析配置（LLM 完全复用 AstrBot Provider，本插件不存 Key）。"""

    analysis_enabled: bool = True
    llm_provider_id: str = ""
    analysis_concurrency: int = 1
    analysis_interval_ms: int = 1500
    analysis_daily_limit: int = 200
    analysis_window_hours: int = 24
    analysis_batch_size: int = 3

    @classmethod
    def from_conf(cls, conf: Any) -> "AnalysisSettings":
        if not isinstance(conf, dict):
            conf = {}
        return cls(
            analysis_enabled=_as_bool(conf.get("analysis_enabled"), True),
            llm_provider_id=str(conf.get("llm_provider_id") or "").strip(),
            analysis_concurrency=max(1, _as_int(conf.get("analysis_concurrency"), 1)),
            analysis_interval_ms=max(
                0, _as_int(conf.get("analysis_interval_ms"), 1500)
            ),
            analysis_daily_limit=max(0, _as_int(conf.get("analysis_daily_limit"), 200)),
            analysis_window_hours=max(
                1, _as_int(conf.get("analysis_window_hours"), 24)
            ),
            analysis_batch_size=max(1, _as_int(conf.get("analysis_batch_size"), 3)),
        )
