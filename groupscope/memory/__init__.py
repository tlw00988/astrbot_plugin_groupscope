"""Phase 4：群体记忆（话题 / 事件与梗 / 时间线）。

* ``rules`` — 规则层话题与梗的确定性抽取（jieba 关键词 + 重复短语）
* ``store`` — 话题/事件/证据/时间线落库与查询（全部带证据消息）

注意：本包 ``__init__`` 保持轻量，避免与 ``storage`` 形成循环导入。
"""

__all__ = ["extract_topics", "extract_events", "MemoryStore"]


def __getattr__(name: str):
    if name == "extract_topics":
        from .rules import extract_topics

        return extract_topics
    if name == "extract_events":
        from .rules import extract_events

        return extract_events
    if name == "MemoryStore":
        from .store import MemoryStore

        return MemoryStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
