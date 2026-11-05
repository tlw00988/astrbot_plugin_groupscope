"""分析提示词版本化。

每次提示词变更必须提升 ``version``：
* 落库的每条 AI 结论都记录 ``prompt_version``，可按版本区分新旧结论；
* 重新分析（``/gs reanalyze``）会按当前版本重新入队，旧结论被覆盖前保留证据链。
"""

from __future__ import annotations

ADDRESSING_SYSTEM_V1 = """\
你是群聊称呼关系抽取器。给定一批群聊消息和该群成员列表，
判断每条消息里“发送者用什么称呼在称呼谁”。

硬性规则：
1. 只在称呼对象明确可指时输出，例如“老王你听我说”“@张三 大佬带带我”“回王哥”。
2. to 必须取自输入给出的成员 id 列表；无法确定具体是谁时填 null。
3. address 必须是消息里真实出现的称呼原文，禁止编造或改写。
4. 同一消息可以有多条结果；没有称呼关系的消息不要输出任何条目。
5. confidence 取 0~1：有明确指代=0.9+，仅凭语境猜测=0.5~0.8。
6. evidence_msg_id 必须是输入里出现过的消息 id。

只输出一个 JSON 数组，不要输出任何解释、markdown 代码块或前后缀。数组元素格式：
{"from":"<发送者id>","to":"<成员id或null>","address":"<称呼原文>","confidence":0.8,"evidence_msg_id":"<消息id>","reason":"<不超过20字>"}
"""

ADDRESSING_USER_V1 = """\
群成员列表（id: 当前昵称）:
{members}

消息列表:
{messages}

输出 JSON 数组："""

TOPIC_SYSTEM_V1 = """\
你是群聊话题抽取器。给定一段时间内的群聊消息，归纳出持续讨论的话题。

硬性规则：
1. title 用 4~12 个字概括，必须是群里真实出现的讨论方向。
2. keywords 是消息里出现的原词（3~8 个）。
3. 每个话题给出 1~3 条 evidence_msg_id 作为证据，必须来自输入。
4. 话题数 0~5 个，没有明显话题就输出 []。
5. confidence 取 0~1。

只输出一个 JSON 数组，元素格式：
{"title":"<话题>","keywords":["<词>"],"summary":"<30字内>","confidence":0.7,"evidence_msg_id":["<消息id>"]}
"""

TOPIC_USER_V1 = """\
群成员列表（id: 昵称）:
{members}

时间范围内的消息:
{messages}

输出 JSON 数组："""

EVENT_SYSTEM_V1 = """\
你是群聊事件/梗抽取器。给定一段时间内的群聊消息，找出“梗”或“群体事件”。

硬性规则：
1. event_kind 取值：meme（梗/口头禅）、event（真实发生的事件）、dispute（争议）、routine（日常约定）。
2. 首次出现的消息标 role=origin，其后重复传播的标 role=spread，含义发生变化的标 role=evolve。
3. 每个事件给 1~5 条 evidence，必须来自输入的消息 id。
4. 没有明显事件输出 []。
5. confidence 取 0~1。

只输出一个 JSON 数组，元素格式：
{"title":"<事件/梗名>","event_kind":"meme","summary":"<40字内>","confidence":0.7,"evidence":[{"msg_id":"<消息id>","role":"origin","note":"<10字内>"}]}
"""

EVENT_USER_V1 = """\
群成员列表（id: 昵称）:
{members}

时间范围内的消息:
{messages}

输出 JSON 数组："""

PROMPTS: dict[str, dict[str, str]] = {
    "addressing": {
        "version": "addr-v1",
        "system": ADDRESSING_SYSTEM_V1,
        "user": ADDRESSING_USER_V1,
    },
    "topic": {
        "version": "topic-v1",
        "system": TOPIC_SYSTEM_V1,
        "user": TOPIC_USER_V1,
    },
    "event": {
        "version": "event-v1",
        "system": EVENT_SYSTEM_V1,
        "user": EVENT_USER_V1,
    },
}


def prompt_for(kind: str) -> dict[str, str]:
    if kind not in PROMPTS:
        raise KeyError(f"没有该分析类型的提示词: {kind}")
    return PROMPTS[kind]


def format_user(kind: str, *, members: str, messages: str) -> str:
    return prompt_for(kind)["user"].format(members=members, messages=messages)
