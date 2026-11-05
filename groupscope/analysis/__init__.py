"""Phase 3：称呼与语义分析。

分层：
* ``rules``     — 规则层确定性抽取（@ 上下文、回复中的称谓词、已知别名）
* ``prompts``   — 提示词版本化（历史数据可按新版本重新分析）
* ``llm``       — 复用 AstrBot 已配置 Provider + 插件侧并发/频率/额度限流
* ``tasks``     — 本地分析任务队列（可重试、可断点续跑）
* ``addressing``— 称呼结果落库与查询（带证据与置信度）
* ``runner``    — 后台分析循环（规则优先，LLM 消歧按需）

注意：本包 ``__init__`` 保持轻量，避免与 ``storage`` 形成循环导入。
"""

__all__ = [
    "PROMPTS",
    "extract_addressing_rules",
    "LLMGateway",
    "RateLimitExceeded",
    "TaskQueue",
    "AddressingStore",
    "AnalysisRunner",
]


def __getattr__(name: str):
    if name == "PROMPTS":
        from .prompts import PROMPTS

        return PROMPTS
    if name == "extract_addressing_rules":
        from .rules import extract_addressing_rules

        return extract_addressing_rules
    if name in ("LLMGateway", "RateLimitExceeded"):
        from . import llm

        return getattr(llm, name)
    if name == "TaskQueue":
        from .tasks import TaskQueue

        return TaskQueue
    if name == "AddressingStore":
        from .addressing import AddressingStore

        return AddressingStore
    if name == "AnalysisRunner":
        from .runner import AnalysisRunner

        return AnalysisRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
