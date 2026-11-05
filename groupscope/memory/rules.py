"""Phase 4 规则层：话题与梗/事件的确定性抽取。

全部基于可观测文本统计，**不做语义推断**（语义归纳交给 LLM 层）：

* **话题（topic）**：窗口内某关键词出现在 ≥``min_msgs`` 条消息、且来自 ≥``min_senders``
  个不同发送者 → 该关键词构成一个话题候选。标题取关键词原文，证据 = 命中消息。
* **梗/事件（meme）**：归一化后的同一短语（去标点、去@、去空白）在窗口内重复出现
  ≥``min_repeat`` 次、且来自 ≥2 个发送者 → 梗候选；首条标 ``origin``，其后标 ``spread``。

产出 ``source='rule'``、置信度是**统计强度**（覆盖人数 × 频次折算），可解释、可复算。
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

# 停用词：虚词、语气词、通用词（不构成话题）
_STOPWORDS: set[str] = {
    "我们", "你们", "他们", "这个", "那个", "什么", "怎么", "为什么", "因为",
    "所以", "但是", "然后", "如果", "就是", "不是", "可以", "没有", "一个",
    "自己", "现在", "今天", "明天", "昨天", "时候", "知道", "觉得", "真的",
    "什么", "怎么", "这样", "那样", "已经", "还是", "或者", "但是", "而且",
    "哈哈", "哈哈哈", "hhh", "233", "666", "www", "emmm", "呃呃", "额",
    "一个", "一下", "有点", "有点儿", "比较", "非常", "特别", "好像", "大概",
    "消息", "图片", "视频", "文件", "语音", "撤回", "@全体成员",
}

_TOKEN_RE = re.compile(r"[一-龥]{2,}|[a-zA-Z]{3,}")
_PUNCT_RE = re.compile(r"[\s\W_]+", re.UNICODE)
_MENTION_RE = re.compile(r"@[^@\s]{1,20}")


@dataclass
class MemMessage:
    """规则层输入的最小消息结构。"""

    msg_id: str
    sender_id: str
    text: str
    sent_at: int = 0
    at_targets: list[str] = field(default_factory=list)


def tokenize(text: str) -> list[str]:
    """分词并过滤停用词（jieba 不可用时退化为正则切词）。"""
    if not text:
        return []
    try:
        import jieba

        words = jieba.lcut(text)
    except Exception:
        words = _TOKEN_RE.findall(text)
    out: list[str] = []
    for w in words:
        w = w.strip().lower()
        if len(w) < 2 or w in _STOPWORDS:
            continue
        if not _TOKEN_RE.fullmatch(w):
            continue
        out.append(w)
    return out


def normalize_phrase(text: str) -> str:
    """归一化：去 @、去标点空白、小写，用于“同一句话重复出现”的判定。"""
    if not text:
        return ""
    t = _MENTION_RE.sub("", text)
    t = _PUNCT_RE.sub("", t)
    return t.lower()


def confidence_for(messages: int, senders: int, span_ratio: float = 1.0) -> float:
    """统计强度：覆盖消息数与人数的折算（0~1），非情感值。"""
    if messages <= 0 or senders <= 0:
        return 0.0
    import math

    base = 1 - math.exp(-messages / 6.0)
    people = min(1.0, senders / 3.0)
    return round(min(1.0, base * (0.5 + 0.5 * people) * max(0.3, span_ratio)), 4)


# ---------------------------------------------------------------------------
# 话题抽取
# ---------------------------------------------------------------------------


def extract_topics(
    messages: list[MemMessage],
    *,
    min_msgs: int = 3,
    min_senders: int = 2,
    max_topics: int = 5,
) -> list[dict[str, Any]]:
    """按关键词共现抽取话题候选（确定性、可复算）。"""
    if not messages:
        return []
    token_msgs: list[tuple[MemMessage, set[str]]] = []
    for m in messages:
        toks = set(tokenize(m.text))
        if toks:
            token_msgs.append((m, toks))

    # 关键词 → 命中消息 / 发送者
    hits: dict[str, list[MemMessage]] = defaultdict(list)
    senders: dict[str, set[str]] = defaultdict(set)
    for m, toks in token_msgs:
        for tok in toks:
            hits[tok].append(m)
            senders[tok].add(m.sender_id)

    candidates = [
        tok
        for tok, ms in hits.items()
        if len(ms) >= min_msgs and len(senders[tok]) >= min_senders
    ]
    if not candidates:
        return []
    # 按覆盖消息数排序；与已选话题关键词高度重叠的合并
    candidates.sort(key=lambda t: len(hits[t]), reverse=True)

    selected: list[str] = []
    for tok in candidates:
        if len(selected) >= max_topics:
            break
        if any(_overlap_ratio(tok, sel, hits) > 0.6 for sel in selected):
            continue
        selected.append(tok)

    total = len(messages)
    now = int(time.time())
    topics: list[dict[str, Any]] = []
    for tok in selected:
        hit_msgs = hits[tok]
        # 共现关键词（同一条消息里一起出现的其他候选词）
        co: dict[str, int] = defaultdict(int)
        hit_ids = {m.msg_id for m in hit_msgs}
        for m, toks in token_msgs:
            if m.msg_id in hit_ids:
                for other in toks:
                    if other != tok and other in candidates:
                        co[other] += 1
        keywords = [tok] + [
            k for k, _ in sorted(co.items(), key=lambda kv: -kv[1])[:7]
        ]
        span = 1.0
        if hit_msgs:
            times = [m.sent_at for m in hit_msgs if m.sent_at]
            if times and min(times) != max(times):
                span = min(1.0, (max(times) - min(times)) / max(1, total))
        topics.append(
            {
                "title": tok[:20],
                "keywords": keywords[:8],
                "summary": f"关键词「{tok}」在 {len(hit_msgs)} 条消息中出现，"
                f"覆盖 {len(senders[tok])} 人",
                "confidence": confidence_for(
                    len(hit_msgs), len(senders[tok]), max(0.3, span)
                ),
                "evidence_msg_ids": [m.msg_id for m in hit_msgs[:5]],
                "mention_count": len(hit_msgs),
                "first_seen": min((m.sent_at for m in hit_msgs if m.sent_at), default=now),
                "last_seen": max((m.sent_at for m in hit_msgs if m.sent_at), default=now),
                "source": "rule",
            }
        )
    topics.sort(key=lambda t: t["mention_count"], reverse=True)
    return topics


def _overlap_ratio(tok: str, other: str, hits: dict[str, list[MemMessage]]) -> float:
    a = {m.msg_id for m in hits.get(tok, [])}
    b = {m.msg_id for m in hits.get(other, [])}
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


# ---------------------------------------------------------------------------
# 梗 / 事件抽取
# ---------------------------------------------------------------------------


def extract_events(
    messages: list[MemMessage],
    *,
    min_repeat: int = 3,
    min_senders: int = 2,
    min_len: int = 4,
    max_events: int = 5,
) -> list[dict[str, Any]]:
    """按“归一化短语重复出现”抽取梗/事件候选，标注首现与传播。"""
    if not messages:
        return []
    groups: dict[str, list[MemMessage]] = defaultdict(list)
    for m in messages:
        phrase = normalize_phrase(m.text or "")
        if len(phrase) < min_len or len(phrase) > 60:
            continue
        groups[phrase].append(m)

    candidates: list[tuple[str, list[MemMessage]]] = []
    for phrase, ms in groups.items():
        senders = {m.sender_id for m in ms}
        if len(ms) >= min_repeat and len(senders) >= min_senders:
            candidates.append((phrase, ms))
    if not candidates:
        return []
    # 短语越长越可能是“梗”（完全相同的一句话），且按频次排序
    candidates.sort(key=lambda kv: (len(kv[0]), len(kv[1])), reverse=True)

    now = int(time.time())
    events: list[dict[str, Any]] = []
    for phrase, ms in candidates[:max_events]:
        ms.sort(key=lambda m: (m.sent_at or 0, m.msg_id))
        senders = {m.sender_id for m in ms}
        times = [m.sent_at for m in ms if m.sent_at]
        evidence = []
        for i, m in enumerate(ms[:5]):
            evidence.append(
                {
                    "msg_id": m.msg_id,
                    "role": "origin" if i == 0 else "spread",
                    "note": "首现" if i == 0 else "重复出现",
                }
            )
        events.append(
            {
                "title": phrase[:24],
                "event_kind": "meme",
                "summary": f"相同内容重复 {len(ms)} 次，{len(senders)} 人参与",
                "confidence": confidence_for(len(ms), len(senders)),
                "evidence": evidence,
                "mention_count": len(ms),
                "first_seen": min(times) if times else now,
                "last_seen": max(times) if times else now,
                "peak_at": _peak_time(ms),
                "source": "rule",
            }
        )
    return events


def _peak_time(ms: list[MemMessage]) -> int | None:
    """峰值时刻：单位小时出现最多的那个小时的起点。"""
    times = [m.sent_at for m in ms if m.sent_at]
    if not times:
        return None
    buckets: dict[int, int] = defaultdict(int)
    for t in times:
        buckets[t // 3600] += 1
    best = max(buckets.items(), key=lambda kv: kv[1])[0]
    return best * 3600
