"""规则层称呼抽取（确定性、可解释、零成本）。

三条规则，全部基于可观测文本与结构，不做语义猜测：

* **R1 称谓词 + 回复目标**：消息引用了某人且正文含称谓词（“老王/王哥/大佬…”）
  → 称呼对象 = 被回复者，证据 = 该消息。
* **R2 称谓词 + @ 目标**：正文含称谓词且消息 @ 了某人 → 称呼对象 = 被 @ 者
  （置信度略低，因为称谓词未必指向被 @ 的人）。
* **R3 已知别名共现**：正文出现其他成员的已知昵称/别名，且该消息是回复或含 @
  → 称呼对象 = 该成员，称呼 = 别名原文。

规则层产出 ``source='rule'``，置信度是**规则强度**而非情感值；
需要语义消歧时由 LLM 层（``source='llm'``）补充。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# 通用称谓词（整词匹配）
HONORIFICS: set[str] = {
    "大佬", "老铁", "铁子", "兄弟", "大兄弟", "哥们", "哥们儿",
    "师傅", "老板", "同学", "同志", "老哥", "老姐", "老兄",
    "老弟", "老妹", "亲爱的", "小哥哥", "小姐姐", "爷们",
    "徒弟", "师兄", "师姐", "师弟", "师妹", "前辈", "后辈",
    "老乡", "战友", "小伙子", "姑娘",
}

# “某哥/某姐/小王/老王/王师傅”等带后缀称谓
_SUFFIX_RE = re.compile(
    r"(?:[一-龥]{1,3})(?:哥|姐|叔|姨|爷|嫂|师傅|老师|老板|大佬|同学|同志|总|工|医|警)"
)
_PREFIX_RE = re.compile(r"(?:老|小|阿)[一-龥]{1,2}(?:哥|姐|叔|姨|师傅|老师|总)?")

# 明显不是称呼的干扰词
_STOPWORDS = {
    "这个", "那个", "哪个", "什么", "怎么", "为什么", "老板", "老铁",
}


@dataclass
class RuleMessage:
    msg_id: str
    sender_id: str
    text: str
    reply_to_sender_id: str | None = None
    at_targets: list[str] = field(default_factory=list)


def find_honorifics(text: str) -> list[str]:
    """按出现顺序返回正文中的称谓词（去重、跳过干扰词）。"""
    if not text:
        return []
    found: list[str] = []
    for match in _SUFFIX_RE.finditer(text):
        word = match.group(0)
        if word in _STOPWORDS:
            continue
        if word not in found:
            found.append(word)
    for word in HONORIFICS:
        if len(word) >= 2 and word in text and word not in found:
            found.append(word)
    for match in _PREFIX_RE.finditer(text):
        word = match.group(0)
        if len(word) < 3 or word in found or word in _STOPWORDS:
            continue
        found.append(word)
    return found


def extract_addressing_rules(
    messages: list[RuleMessage],
    *,
    known_aliases: dict[str, set[str]] | None = None,
    sender_of: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """批量抽取规则层称呼，返回可写入 ``addressing`` 的记录。

    Args:
        messages: 待分析消息。
        known_aliases: ``成员id -> 已知昵称/别名集合``，用于 R3。
        sender_of: ``消息id -> 发送者id``，用于校验别名不是“自称”。
    """
    known_aliases = known_aliases or {}
    sender_of = sender_of or {}
    rows: list[dict[str, Any]] = []

    # 反查：别名 -> 成员 id（长度降序，优先匹配更长的别名）
    alias_index: list[tuple[str, str]] = []
    for member_id, aliases in known_aliases.items():
        for alias in aliases:
            alias = (alias or "").strip()
            if len(alias) >= 2:
                alias_index.append((alias, member_id))
    alias_index.sort(key=lambda pair: len(pair[0]), reverse=True)

    for msg in messages:
        sender = msg.sender_id
        addresses: list[tuple[str, str | None, float, str]] = []

        honorifics = find_honorifics(msg.text or "")

        # R1：回复目标 + 称谓词
        if msg.reply_to_sender_id and honorifics:
            for word in honorifics[:2]:
                addresses.append((word, msg.reply_to_sender_id, 0.9, "R1"))

        # R2：@ 目标 + 称谓词
        if msg.at_targets and honorifics:
            for target in msg.at_targets[:2]:
                if target == "all":
                    continue
                # 已由 R1 覆盖同一对象时不重复
                if any(a[1] == target for a in addresses):
                    continue
                addresses.append((honorifics[0], target, 0.6, "R2"))

        # R3：已知别名共现（且消息带回复或 @，说明是在称呼对方）
        if alias_index and (msg.reply_to_sender_id or msg.at_targets):
            used = msg.text or ""
            for alias, member_id in alias_index:
                if member_id == sender:
                    continue  # 不把自己的别名当“称呼别人”
                if alias in used:
                    addresses.append((alias, member_id, 0.7, "R3"))
                    break  # 每条消息只取最长命中

        seen: set[tuple[str, str | None]] = set()
        for address, target, confidence, rule in addresses:
            key = (address, target)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "from_raw_id": sender,
                    "to_raw_id": target,
                    "address": address,
                    "source": "rule",
                    "confidence": confidence,
                    "evidence_msg_id": msg.msg_id,
                    "rule": rule,
                }
            )
    return rows


def known_aliases_from_rows(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    """把 ``member_aliases`` 查询结果转成规则层需要的索引。"""
    index: dict[str, set[str]] = {}
    for row in rows:
        member_id = str(row["member_id"])
        index.setdefault(member_id, set()).add(str(row["alias"]))
    return index
