"""插件数据目录解析。

注意：AstrBot 的根目录解析顺序为 ``ASTRBOT_ROOT`` 环境变量 > 桌面运行时
``~/.astrbot`` > 当前工作目录（见 ``astrbot.core.utils.astrbot_path``），
因此路径在初始化时解析一次并缓存，避免运行期 cwd 变化导致数据分裂。
"""

from __future__ import annotations

from pathlib import Path

from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

from . import PLUGIN_NAME

_data_dir: Path | None = None


def plugin_data_dir(plugin_name: str | None = None) -> Path:
    """返回插件专用数据目录（``data/plugin_data/<plugin_name>/``）。"""
    global _data_dir
    name = plugin_name or PLUGIN_NAME
    if _data_dir is None or _data_dir.name != name:
        _data_dir = Path(get_astrbot_plugin_data_path()) / name
        _data_dir.mkdir(parents=True, exist_ok=True)
    return _data_dir


def reset_cache() -> None:
    """仅用于测试：清除已缓存的路径。"""
    global _data_dir
    _data_dir = None
