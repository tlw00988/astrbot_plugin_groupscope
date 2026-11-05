"""LLM 网关：复用 AstrBot 已配置 Provider + 插件侧限流。

**不实现**独立的 base_url / api_key 配置——Provider 完全由 AstrBot 管理，
插件只在配置里选择用哪一个（``_special: select_provider``）。

限流三件套（全部在插件侧）：
* 并发上限（信号量）
* 最小调用间隔（单调时钟）
* 每日调用配额（本地日期，超限直接拒绝，不排队）
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Callable

JSON_ARRAY_RE = re.compile(r"\[.*\]", re.S)


class RateLimitExceeded(RuntimeError):
    """超过插件侧限流（并发/间隔/配额）。"""


class ProviderUnavailable(RuntimeError):
    """没有可用的 AstrBot Provider。"""


class LLMCallError(RuntimeError):
    """LLM 调用或返回解析失败。"""


class LLMGateway:
    def __init__(
        self,
        get_provider: Callable[[], Any],
        *,
        concurrency: int = 1,
        interval_ms: int = 1000,
        daily_limit: int = 200,
        logger: Any = None,
    ) -> None:
        """Args:
            get_provider: 返回 AstrBot ``Provider`` 实例（或 None）的 callable，
                可以是 async 函数。
        """
        self._get_provider = get_provider
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._interval = max(0.0, interval_ms / 1000)
        self._daily_limit = max(0, daily_limit)
        self.log = logger
        self._used_today = 0
        self._day = time.strftime("%Y%m%d")
        self._next_allowed = 0.0
        self.stats = {"calls": 0, "errors": 0, "throttled": 0, "rejected": 0}

    # -- 配额 -------------------------------------------------------

    def _roll_day(self) -> None:
        today = time.strftime("%Y%m%d")
        if today != self._day:
            self._day = today
            self._used_today = 0

    @property
    def remaining_today(self) -> int:
        self._roll_day()
        if self._daily_limit <= 0:
            return -1  # 不限制
        return max(0, self._daily_limit - self._used_today)

    # -- 调用 -------------------------------------------------------

    async def chat(
        self, *, system: str, prompt: str, timeout: float = 90.0
    ) -> str:
        """执行一次文本补全，返回纯文本。"""
        self._roll_day()
        if self._daily_limit > 0 and self._used_today >= self._daily_limit:
            self.stats["rejected"] += 1
            raise RateLimitExceeded(
                f"今日 LLM 调用已达上限（{self._daily_limit} 次）"
            )

        if self._sem.locked():
            self.stats["throttled"] += 1

        try:
            await asyncio.wait_for(self._sem.acquire(), timeout=30)
        except (asyncio.TimeoutError, TimeoutError) as e:
            self.stats["throttled"] += 1
            raise RateLimitExceeded("等待 LLM 并发槽超时") from e
        try:
            # 最小间隔（在锁内等待，保证串行化节流）
            now = time.monotonic()
            if self._next_allowed > now:
                await asyncio.sleep(self._next_allowed - now)
            self._next_allowed = time.monotonic() + self._interval

            provider = await _maybe_await(self._get_provider())
            if provider is None:
                raise ProviderUnavailable("未配置可用的 LLM Provider")

            self._used_today += 1
            self.stats["calls"] += 1
            try:
                resp = await asyncio.wait_for(
                    provider.text_chat(prompt=prompt, system_prompt=system),
                    timeout=timeout,
                )
            except (asyncio.TimeoutError, TimeoutError) as e:
                self.stats["errors"] += 1
                raise LLMCallError(f"LLM 调用超时（>{timeout}s）") from e
            except Exception as e:
                self.stats["errors"] += 1
                raise LLMCallError(f"LLM 调用失败: {e}") from e

            text = _response_text(resp)
            if not text.strip():
                self.stats["errors"] += 1
                raise LLMCallError("LLM 返回空内容")
            return text
        finally:
            self._sem.release()


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


def _response_text(resp: Any) -> str:
    chain = getattr(resp, "result_chain", None)
    if chain is not None:
        getter = getattr(chain, "get_plain_text", None)
        if callable(getter):
            return str(getter())
        return str(chain)
    completion = getattr(resp, "completion", None)
    if completion is not None:
        return str(completion)
    return str(resp or "")


def parse_json_array(text: str) -> list[dict[str, Any]]:
    """从 LLM 输出中稳健地提取 JSON 数组（容忍代码块与前后缀）。"""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        data = json.loads(cleaned)
        return _as_dict_list(data)
    except Exception:
        pass
    match = JSON_ARRAY_RE.search(cleaned)
    if match:
        try:
            data = json.loads(match.group(0))
            return _as_dict_list(data)
        except Exception:
            pass
    # 单对象兜底
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return [data]
    except Exception:
        pass
    raise LLMCallError(f"无法解析 LLM 返回的 JSON: {text[:200]}")


def _as_dict_list(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []
