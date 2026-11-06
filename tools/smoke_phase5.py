"""Phase 5 WebUI 冒烟：注册 -> 官方路由匹配 -> 绑定 request -> 调用全部端点。"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
import time

PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR.parent))   # 使 astrbot_plugin_groupscope 可导入
# 用全新根目录跑（复用旧数据会 UNIQUE 冲突）
os.environ.setdefault("ASTRBOT_ROOT", tempfile.mkdtemp(prefix="gs-phase5-"))

from astrbot.api.web import PluginRequest, bind_request_context  # noqa: E402

from astrbot_plugin_groupscope.main import Main  # noqa: E402
from astrbot_plugin_groupscope.groupscope.storage.db import Database  # noqa: E402


class FakeHeaders(dict):
    def get(self, key, default=None):
        return dict.get(self, key.lower(), default)


class FakeQP:
    def __init__(self, pairs):
        self._pairs = pairs

    def multi_items(self):
        return list(self._pairs)


class StubRequest:
    def __init__(self, path: str, query: list[tuple[str, str]]):
        self.method = "GET"

        class _URL:
            pass

        self.url = _URL()
        self.url.path = path
        self.headers = FakeHeaders()
        self.cookies = {}
        self.client = None
        self.query_params = FakeQP(query)


class FakeContext:
    def __init__(self):
        self.registered: list[tuple[str, object, list, str]] = []
        self.logger = None

    def register_web_api(self, route, handler, methods, desc):
        self.registered.append((route, handler, methods, desc))

    def get_provider_by_id(self, pid):
        return None

    def get_all_providers(self):
        return []


async def seed(db: Database) -> None:
    now = int(time.time())
    c = db.conn
    await c.execute(
        "INSERT INTO groups(platform_id,platform_name,group_id,group_name,"
        " capture_enabled,first_seen,last_seen) VALUES('aiocqhttp','QQ','123',"
        "'测试群',1,?,?)",
        (now, now),
    )
    for uid, nick in (("1001", "王大佬"), ("1002", "小明")):
        await c.execute(
            "INSERT INTO members(platform_id,user_id,current_nickname,first_seen,"
            "last_seen,message_count) VALUES('aiocqhttp',?,?,?, ?,10)",
            (uid, nick, now, now),
        )
    texts = [
        ("m1", "1002", "王哥你听我说", 1001),
        ("m2", "1002", "周末一起去爬山吧", None),
        ("m3", "1001", "爬山好啊周末有空", None),
        ("m4", "1002", "周末爬山记得带水", None),
    ]
    for i, (mid, sender, text, reply) in enumerate(texts, start=1):
        await c.execute(
            "INSERT INTO messages(platform_id,platform_name,group_ref,group_raw_id,"
            "message_id,dedup_key,sender_ref,sender_raw_id,sender_raw_name,event_kind,"
            "sent_at,received_at,message_type,text,reply_to_sender_id,inserted_at)"
            " VALUES('aiocqhttp','QQ',1,'123',?,?,1,? ,?,'message',?,?,'text',?,?,?)",
            (mid, mid, sender, sender, now - 100 + i, now, text, reply, now),
        )
    await c.execute(
        "INSERT INTO message_segments(message_ref,seg_index,seg_type,payload_json,text)"
        " VALUES(1,0,'text','{}','王哥你听我说')"
    )
    await c.execute(
        "INSERT INTO addressing(group_ref,from_member,to_member,address,source,"
        "confidence,created_at,last_seen,seen_count,reviewed)"
        " VALUES(1,2,1,'王哥','rule',0.9,?,?,3,1)",
        (now, now),
    )
    await c.execute(
        "INSERT INTO topics(group_ref,title,summary,keywords_json,first_seen,last_seen,"
        "mention_count,source,confidence,created_at)"
        " VALUES(1,'周末爬山','约爬山','[\"周末\",\"爬山\"]',?,?,4,'rule',0.5,?)",
        (now - 50, now, now),
    )
    await c.execute(
        "INSERT INTO topic_mentions(topic_id,message_ref,weight,created_at) VALUES(1,2,1,?)",
        (now,),
    )
    await c.execute(
        "INSERT INTO events(group_ref,title,summary,event_kind,first_seen,last_seen,"
        "mention_count,source,confidence,created_at) VALUES(1,'周末爬山','重复提及',"
        "'event',?,?,3,'rule',0.4,?)",
        (now - 50, now, now),
    )
    await c.execute(
        "INSERT INTO event_evidence(event_id,message_ref,role,note,created_at)"
        " VALUES(1,2,'origin','首现',?)",
        (now,),
    )
    await c.execute(
        "INSERT INTO memory_timeline(group_ref,item_kind,item_id,occurred_at,label,detail)"
        " VALUES(1,'topic',1,?,'周末爬山',NULL)",
        (now - 50,),
    )
    await c.commit()


async def main() -> None:
    from astrbot.dashboard.api.plugins import (
        _match_registered_web_api,
        _plugin_api_route_pattern,
    )

    ctx = FakeContext()
    plugin = Main(ctx, {})
    assert len(ctx.registered) == 12, f"注册数 {len(ctx.registered)}: {ctx.registered}"
    routes = [r[0] for r in ctx.registered]
    assert all(r.startswith("/astrbot_plugin_groupscope/") for r in routes), routes
    print("routes:", *routes, sep="\n  ")

    # 打开数据库并塞入测试数据（模拟已就绪）
    plugin.db = Database(plugin.data_dir / "groupscope.db")
    await plugin.db.open()
    plugin._started = True
    await seed(plugin.db)

    # 每条 route 用官方匹配逻辑跑一遍
    cases: list[tuple[str, list[tuple[str, str]]]] = [
        ("/astrbot_plugin_groupscope/overview", []),
        ("/astrbot_plugin_groupscope/messages", [("q", "爬山"), ("limit", "5")]),
        ("/astrbot_plugin_groupscope/message/1", []),
        ("/astrbot_plugin_groupscope/message/9999", []),
        ("/astrbot_plugin_groupscope/members", [("q", "小明")]),
        ("/astrbot_plugin_groupscope/member/1", []),
        ("/astrbot_plugin_groupscope/network", [("window", "week")]),
        ("/astrbot_plugin_groupscope/addressing", []),
        ("/astrbot_plugin_groupscope/topics", []),
        ("/astrbot_plugin_groupscope/topic/1", []),
        ("/astrbot_plugin_groupscope/events", []),
        ("/astrbot_plugin_groupscope/event/1", []),
        ("/astrbot_plugin_groupscope/timeline", [("limit", "10")]),
        ("/astrbot_plugin_groupscope/messages", [("q", "不存在的词xyz")]),
        ("/astrbot_plugin_groupscope/member/abc", []),  # 400 分支
        ("/astrbot_plugin_groupscope/unknown", []),  # 未找到路由
    ]

    ok_count = 0
    for path, query in cases:
        matched = _match_registered_web_api(ctx.registered, path, "GET")
        if matched is None:
            assert path.endswith("/unknown"), path
            print(f"  {path} -> 未找到路由 (符合预期)")
            continue
        handler, path_values = matched
        pattern = _plugin_api_route_pattern(
            next(r for r, h, m, d in ctx.registered if h is handler)
        )
        import re

        assert re.fullmatch(pattern, path), (pattern, path)
        stub = StubRequest(path, query)
        preq = PluginRequest(
            stub, path_params=path_values, plugin_name="astrbot_plugin_groupscope"
        )
        with bind_request_context(preq):
            resp = await handler(**path_values)
        body = json.loads(resp.body)
        status = body.get("status")
        if status == "ok":
            ok_count += 1
            data = body.get("data")
            keys = list(data)[:6] if isinstance(data, dict) else f"list[{len(data)}]"
            print(f"  {path}{'?' + str(query) if query else ''} -> ok {keys}")
        else:
            print(
                f"  {path} -> {status} ({body.get('message')})"
                f" [HTTP {resp.status_code}]"
            )
            assert path.endswith("9999") or path.endswith("/abc"), path

    assert ok_count >= 12, f"成功端点 {ok_count}"
    # 关键内容断言
    stub = StubRequest("/x", [])
    with bind_request_context(PluginRequest(stub, plugin_name="astrbot_plugin_groupscope")):
        h = dict((r, hh) for r, hh, m, d in ctx.registered)
        ov = json.loads(
            (await h["/astrbot_plugin_groupscope/overview"]()).body
        )["data"]
        assert ov["messages"] == 4 and ov["groups"], ov
        net = json.loads(
            (await h["/astrbot_plugin_groupscope/network"]()).body
        )["data"]
        assert "disclaimer" in net and net["window"] == "week", net.keys()
        tp = json.loads(
            (await h["/astrbot_plugin_groupscope/topic/<topic_id>"](topic_id="1")).body
        )["data"]
        assert tp["evidence"][0]["message_id"] == "m2", tp["evidence"]
        md = json.loads(
            (await h["/astrbot_plugin_groupscope/message/<message_id>"](
                message_id="1"
            )).body
        )["data"]
        assert md["segments"] and md["after"], md.keys()

    await plugin.db.close()
    print("PHASE5 SMOKE OK:", ok_count, "ok endpoints")


if __name__ == "__main__":
    asyncio.run(main())
