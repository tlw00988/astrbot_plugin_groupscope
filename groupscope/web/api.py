"""Phase 5 扩展 API：全部经 ``/api/v1/plugins/extensions/<插件名>/<path>`` 暴露。

鉴权：入口 ``require_plugin_scope``（Dashboard JWT / plugin-scope API Key），
本层不再自建鉴权；所有接口只读，不提供删除类操作（删除走指令且需管理员）。

约定（官方文档 docs/zh/dev/star/guides/plugin-pages.md）：
* 注册的 route **必须带插件名前缀** ``/<插件名>/...``，否则与
  ``/api/v1/plugins/extensions/<插件名>/<path>`` 的匹配会失败；
* handler 通过 ``astrbot.api.web.request`` 代理读取 query/path 参数
  （由 ``bind_request_context`` 注入，闭包内可用），返回 ``json_response``；
* 带 ``<param>`` 的 route，handler 以同名关键字参数接收
  （Dashboard 侧 ``view_handler(**path_values)``）；
* 任何异常统一包装为 ``{"status":"error"}``。
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from astrbot.api.web import json_response, request

from .. import PLUGIN_NAME
from ..storage import repo

if TYPE_CHECKING:  # pragma: no cover
    from ...main import Main

logger = logging.getLogger("groupscope.web")

ROUTE_PREFIX = f"/{PLUGIN_NAME}"


def _q(key: str, default: str = "") -> str:
    try:
        value = request.query.get(key)
    except Exception:
        return default
    return (value or default).strip()


def _q_int(key: str, default: int, *, minimum: int = 0, maximum: int = 500) -> int:
    raw = _q(key)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


async def _resolve_group(main: "Main", token: str) -> int | None:
    """把 query 里的 group（主键或平台群 ID）解析为主键；空 = 全部。"""
    if not token:
        return None
    row = await repo.find_group(main.db.conn, token)
    return int(row["id"]) if row else None


def _ok(data: Any):
    return json_response({"status": "ok", "data": data})


def _err(message: str, status_code: int = 500):
    return json_response(
        {"status": "error", "message": message, "data": {}}, status_code=status_code
    )


async def _guard(main: "Main") -> str | None:
    """返回 None 表示就绪，否则返回错误文案。"""
    try:
        if await main._ready():
            return None
        return "GroupScope 未就绪（数据库未打开），详见 AstrBot 日志"
    except Exception as e:  # 防御：指令外的异常也要有响应
        logger.exception("web guard 失败")
        return f"内部错误: {e}"


def register_web_api(main: "Main") -> None:
    """把本插件的扩展 API 注册到 AstrBot Context（重复注册会自动替换）。"""
    ctx = main.context
    prefix = ROUTE_PREFIX

    # ------------------------------------------------------------------
    # 总览
    # ------------------------------------------------------------------
    async def overview() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        conn = main.db.conn
        total = await repo.count_messages(conn)
        notices = await repo.count_notices(conn)
        members = await repo.count_members(conn)
        groups = await repo.list_groups(conn)
        daily = await repo.daily_counts(conn, days=7)
        size = await main.db.file_size()
        tasks: dict[str, Any] = {}
        gateway: dict[str, Any] = {"enabled": main.gateway is not None}
        if main.runner is not None:
            tasks = await main.runner.queue.stats()
            gateway.update(
                {
                    "calls": main.gateway.stats["calls"] if main.gateway else 0,
                    "errors": main.gateway.stats["errors"] if main.gateway else 0,
                    "remaining_today": (
                        main.gateway.remaining_today if main.gateway else 0
                    ),
                    "rule_tasks": main.runner.stats["rule_tasks"],
                    "llm_tasks": main.runner.stats["llm_tasks"],
                }
            )
        return _ok(
            {
                "capture_enabled": main.settings.capture_enabled,
                "analysis_enabled": main.analysis_settings.analysis_enabled,
                "messages": total,
                "notices": notices,
                "members": members,
                "db_size_bytes": size,
                "daily": daily,
                "groups": [_group_dict(g) for g in groups],
                "tasks": tasks,
                "gateway": gateway,
                "prompt_versions": _prompt_versions(),
                "server_time": int(time.time()),
            }
        )

    # ------------------------------------------------------------------
    # 消息：搜索 / 单条上下文
    # ------------------------------------------------------------------
    async def messages() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        q = _q("q")
        limit = _q_int("limit", 30, minimum=1, maximum=100)
        group_ref = await _resolve_group(main, _q("group"))
        if q:
            rows = await repo.search_messages(main.db.conn, q, group_ref, limit=limit)
        else:
            rows = await repo.recent_messages(main.db.conn, group_ref, limit)
        return _ok(
            {
                "items": [
                    {
                        "id": r["id"],
                        "message_id": r["message_id"],
                        "sent_at": r["sent_at"],
                        "text": r["text"],
                        "sender": r["sender_raw_name"] or r["sender_raw_id"],
                        "group": r["group_raw_id"],
                        "event_kind": r["event_kind"],
                    }
                    for r in rows
                ],
                "query": q,
            }
        )

    async def message_detail(message_id: str) -> Any:
        """单条消息及其前后上下文（分析结论回跳原消息用）。"""
        err = await _guard(main)
        if err:
            return _err(err)
        if not message_id.isdigit():
            return _err("message_id 必须是数字", status_code=400)
        conn = main.db.conn
        cur = await conn.execute(
            "SELECT * FROM messages WHERE id = ?", (int(message_id),)
        )
        row = await cur.fetchone()
        if row is None:
            return _err("消息不存在", status_code=404)
        msg = dict(row)
        # 消息段（完整结构）
        cur = await conn.execute(
            "SELECT seg_index, seg_type, payload_json, text FROM message_segments"
            " WHERE message_ref = ? ORDER BY seg_index",
            (msg["id"],),
        )
        segments = [dict(r) for r in await cur.fetchall()]
        # 前后各 5 条同群消息作为上下文
        cur = await conn.execute(
            "SELECT id, sent_at, text, sender_raw_name, sender_raw_id, event_kind"
            " FROM messages WHERE group_ref IS ? AND id < ?"
            " ORDER BY id DESC LIMIT 5",
            (msg["group_ref"], msg["id"]),
        )
        before = [dict(r) for r in await cur.fetchall()][::-1]
        cur = await conn.execute(
            "SELECT id, sent_at, text, sender_raw_name, sender_raw_id, event_kind"
            " FROM messages WHERE group_ref IS ? AND id > ?"
            " ORDER BY id ASC LIMIT 5",
            (msg["group_ref"], msg["id"]),
        )
        after = [dict(r) for r in await cur.fetchall()]
        return _ok({"message": msg, "segments": segments, "before": before, "after": after})

    # ------------------------------------------------------------------
    # 人物
    # ------------------------------------------------------------------
    async def members() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        q = _q("q")
        if not q:
            cur = await main.db.conn.execute(
                "SELECT id, platform_id, user_id, current_nickname, message_count,"
                " first_seen, last_seen FROM members ORDER BY last_seen DESC LIMIT 100"
            )
            rows = [dict(r) for r in await cur.fetchall()]
        else:
            from ..social.profiles import member_search

            rows = await member_search(main.db.conn, q, limit=50)
        return _ok({"items": rows})

    async def member_detail(member_id: str) -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        if not member_id.isdigit():
            return _err("member_id 必须是数字", status_code=400)
        from ..social.profiles import member_profile

        profile = await member_profile(main.db.conn, int(member_id))
        if profile is None:
            return _err("成员不存在", status_code=404)
        # 指称该成员的称呼（被人怎么叫）
        from ..analysis.addressing import AddressingStore

        store = AddressingStore(main.db.conn)
        profile["addressed_as"] = await store.query(
            member_id=int(member_id), limit=30
        )
        return _ok(profile)

    # ------------------------------------------------------------------
    # 关系网络
    # ------------------------------------------------------------------
    async def network() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        from ..social import snapshots as snap

        window = _q("window", "week")
        if window not in snap.WINDOWS:
            window = "week"
        group_ref = await _resolve_group(main, _q("group"))
        await snap.compute_snapshots(main.db.conn, group_ref=group_ref, window=window)
        edges = await snap.snapshot_windows(
            main.db.conn, group_ref=group_ref, window=window
        )
        conn = main.db.conn
        nodes: dict[int, dict[str, Any]] = {}
        for e in edges:
            for key in ("from_member", "to_member"):
                mid = e[key]
                if mid in nodes:
                    continue
                cur = await conn.execute(
                    "SELECT id, user_id, current_nickname, message_count"
                    " FROM members WHERE id = ?",
                    (mid,),
                )
                row = await cur.fetchone()
                nodes[mid] = dict(row) if row else {"id": mid, "user_id": str(mid)}
        # 边上的类型拆分（供前端按 @/回复/引用 着色）
        return _ok(
            {
                "window": window,
                "window_label": snap.WINDOW_LABELS[window],
                "nodes": list(nodes.values()),
                "edges": edges,
                "disclaimer": "边仅表示 @/回复/引用等确定性互动与证据量，不构成亲密度评价。",
            }
        )

    # ------------------------------------------------------------------
    # 称呼体系
    # ------------------------------------------------------------------
    async def addressing() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        from ..analysis.addressing import AddressingStore

        store = AddressingStore(main.db.conn)
        group_ref = await _resolve_group(main, _q("group"))
        rows = await store.query(
            group_ref=group_ref, address=_q("q") or None, limit=100
        )
        agg = await store.group_by_address(group_ref=group_ref, limit=50)
        return _ok({"items": rows, "aggregate": agg})

    # ------------------------------------------------------------------
    # 群体记忆
    # ------------------------------------------------------------------
    async def topics() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        from ..memory.store import MemoryStore

        group_ref = await _resolve_group(main, _q("group"))
        store = MemoryStore(main.db.conn)
        rows = await store.list_topics(group_ref, limit=50)
        return _ok({"items": rows})

    async def topic_detail(topic_id: str) -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        if not topic_id.isdigit():
            return _err("topic_id 必须是数字", status_code=400)
        from ..memory.store import MemoryStore

        store = MemoryStore(main.db.conn)
        cur = await main.db.conn.execute(
            "SELECT * FROM topics WHERE id = ?", (int(topic_id),)
        )
        row = await cur.fetchone()
        if row is None:
            return _err("话题不存在", status_code=404)
        evidence = await store.topic_evidence(int(topic_id), limit=20)
        return _ok({"topic": dict(row), "evidence": evidence})

    async def events() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        from ..memory.store import MemoryStore

        group_ref = await _resolve_group(main, _q("group"))
        kind = _q("kind") or None
        store = MemoryStore(main.db.conn)
        rows = await store.list_events(group_ref, event_kind=kind, limit=50)
        return _ok({"items": rows})

    async def event_detail(event_id: str) -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        if not event_id.isdigit():
            return _err("event_id 必须是数字", status_code=400)
        from ..memory.store import MemoryStore

        store = MemoryStore(main.db.conn)
        cur = await main.db.conn.execute(
            "SELECT * FROM events WHERE id = ?", (int(event_id),)
        )
        row = await cur.fetchone()
        if row is None:
            return _err("事件不存在", status_code=404)
        evidence = await store.event_evidence(int(event_id), limit=20)
        return _ok({"event": dict(row), "evidence": evidence})

    async def timeline() -> Any:
        err = await _guard(main)
        if err:
            return _err(err)
        from ..memory.store import MemoryStore

        group_ref = await _resolve_group(main, _q("group"))
        limit = _q_int("limit", 50, minimum=1, maximum=200)
        store = MemoryStore(main.db.conn)
        rows = await store.timeline(group_ref, limit=limit)
        return _ok({"items": rows})

    # ------------------------------------------------------------------
    # 注册
    # ------------------------------------------------------------------
    ctx.register_web_api(f"{prefix}/overview", overview, ["GET"], "GroupScope 总览")
    ctx.register_web_api(f"{prefix}/messages", messages, ["GET"], "消息搜索/最近消息")
    ctx.register_web_api(
        f"{prefix}/message/<message_id>", message_detail, ["GET"], "单条消息与上下文"
    )
    ctx.register_web_api(f"{prefix}/members", members, ["GET"], "成员列表/搜索")
    ctx.register_web_api(
        f"{prefix}/member/<member_id>", member_detail, ["GET"], "成员档案"
    )
    ctx.register_web_api(f"{prefix}/network", network, ["GET"], "关系网络")
    ctx.register_web_api(f"{prefix}/addressing", addressing, ["GET"], "称呼体系")
    ctx.register_web_api(f"{prefix}/topics", topics, ["GET"], "话题列表")
    ctx.register_web_api(
        f"{prefix}/topic/<topic_id>", topic_detail, ["GET"], "话题证据"
    )
    ctx.register_web_api(f"{prefix}/events", events, ["GET"], "事件/梗列表")
    ctx.register_web_api(
        f"{prefix}/event/<event_id>", event_detail, ["GET"], "事件证据链"
    )
    ctx.register_web_api(f"{prefix}/timeline", timeline, ["GET"], "群体记忆时间线")


def _group_dict(g: Any) -> dict[str, Any]:
    return dict(g) if not isinstance(g, dict) else g


def _prompt_versions() -> dict[str, str]:
    try:
        from ..analysis.prompts import PROMPTS

        return {k: v["version"] for k, v in PROMPTS.items()}
    except Exception:
        return {}
