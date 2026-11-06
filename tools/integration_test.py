#!/usr/bin/env python3
"""GroupScope 端到端集成测试：真实 AstrBot 实例 + 假 OneBot 逆向 WS + LLM 陷阱服务。

干什么（在**隔离的 ASTRBOT_ROOT** 里启动一台真的 ``astrbot run``）：

  R1（analysis_enabled = false，零 LLM 断言）
    1. 假 OneBot 客户端反向连接 ``ws://127.0.0.1:<port>/ws``（universal 角色）；
    2. 注入普通群消息 / @ 消息 / 引用消息 / 重复 message_id / 通知事件 /
       白名单外群 / 被排除成员 / 私聊；
    3. 断言：入库条数与去重、消息段与 @/reply 记录、越界消息一条都不入库；
    4. 注入管理员与非管理员的 ``/gs`` 指令，断言有回复（含权限不足提示）；
    5. ``/gs analyze <群> rule`` 跑完规则任务后，**陷阱服务 0 次命中**
       ——即规则层零 LLM、非唤醒消息零 LLM、零回复；
    6. ``/gs backup`` 产出备份文件；SIGINT 干净停机（插件 unload 日志 + quick_check）。

  R2（analysis_enabled = true，真实 LLM 链路）
    7. 重启实例，``/gs analyze <群> llm`` → 陷阱服务命中，
       任务完成、addressing 出现 source=llm 且证据指向真实消息；
    8. ``/gs tasks`` 反馈任务统计；``/gs analyze ... since=/until=`` 范围参数生效；
    9. 二次 SIGINT 干净停机。

用法：
    python tools/integration_test.py            # 全流程
    python tools/integration_test.py --keep     # 保留测试实例目录

退出码：0 = 全部断言通过；1 = 有断言失败。
依赖：本机可执行 ``<插件>/.venv/bin/astrbot``（或用 --astrbot 指定）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

try:
    from aiohttp import web
except ImportError:  # pragma: no cover
    print("需要 aiohttp：pip install aiohttp", file=sys.stderr)
    raise

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover
    print("需要 websockets>=12：pip install websockets", file=sys.stderr)
    raise

PLUGIN_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = Path("/tmp/opencode/gs-itest")

GROUP = "90001"
OUT_GROUP = "999999"
ADMIN = "9001"
MEMBER = "9002"
EXCLUDED = "9005"
SELF_ID = "200000"
DASH_PASSWORD = "gs-itest-pass"

FAILURES: list[str] = []

# aiocqhttp 回复走 send_group_msg / send_private_msg（个别路径用 send_msg）
REPLY_ACTIONS = ("send_msg", "send_group_msg", "send_private_msg")


def check(cond: bool, msg: str) -> bool:
    if not cond:
        FAILURES.append(msg)
        print(f"  ✗ {msg}")
    else:
        print(f"  ✓ {msg}")
    return bool(cond)


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def wait_until(fn, timeout: float = 30.0, interval: float = 0.5):
    """轮询直到 fn() 真值或超时，返回最后一个值。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last:
                return last
        except Exception as e:  # noqa: BLE001
            last = e
        await asyncio.sleep(interval)
    return last


def db_rows(root: Path, sql: str, params: tuple = ()) -> list[tuple]:
    """只读连接主库（WAL 下允许与写进程并发）。"""
    db = root / "data" / "plugin_data" / "astrbot_plugin_groupscope" / "groupscope.db"
    if not db.exists():
        return []
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def db_one(root: Path, sql: str, params: tuple = ()):
    rows = db_rows(root, sql, params)
    return rows[0] if rows else None


def chain_text(message) -> str:
    """把 send_msg 的 message 参数（CQ 字符串或段列表）拍平成纯文本。"""
    if isinstance(message, str):
        return re.sub(r"\[CQ:[^\]]*\]", "", message)
    if isinstance(message, list):
        out = []
        for seg in message:
            if isinstance(seg, dict):
                out.append(str((seg.get("data") or {}).get("text") or ""))
        return "".join(out)
    return str(message)


# --------------------------------------------------------------------------
# LLM 陷阱服务：命中即记录，并按提示词返回"合法"的 JSON 数组
# --------------------------------------------------------------------------


class Trap:
    def __init__(self) -> None:
        self.hits: list[dict] = []
        self.requests: list[dict] = []

    async def start(self) -> int:
        app = web.Application()
        app.router.add_post("/v1/chat/completions", self._chat)
        app.router.add_get("/stats", self._stats)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self._runner = runner
        port = site._server.sockets[0].getsockname()[1]
        return int(port)

    async def stop(self) -> None:
        await self._runner.cleanup()

    async def _stats(self, _req):
        return web.json_response({"hits": len(self.hits), "kinds": [h["kind"] for h in self.hits]})

    async def _chat(self, req: web.Request) -> web.Response:
        body = await req.json()
        system, user = "", ""
        for m in body.get("messages") or []:
            content = m.get("content") or ""
            if isinstance(content, list):  # 多模态 content 分块
                content = "".join(
                    str(p.get("text") or "") if isinstance(p, dict) else str(p)
                    for p in content
                )
            if m.get("role") == "system":
                system += content
            else:
                user += content
        kind = self._kind(system)
        content = self._payload(kind, user)
        self.hits.append({"ts": time.time(), "kind": kind})
        self.requests.append({"kind": kind, "user": user[:4000]})
        data = {
            "id": "chatcmpl-trap",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": body.get("model") or "trap-model",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
        }
        return web.json_response(data)

    @staticmethod
    def _kind(system: str) -> str:
        if "事件/梗抽取器" in system:
            return "event"
        if "话题" in system:
            return "topic"
        return "addressing"

    @staticmethod
    def _payload(kind: str, user: str) -> str:
        msg_ids = re.findall(r"^\[([^\]]+)\]\s", user, re.M)
        members = re.findall(r"^- (\S+):", user, re.M)
        if kind == "event":
            mid = msg_ids[0] if msg_ids else "x"
            rows = [
                {
                    "title": "集成测试事件",
                    "event_kind": "meme",
                    "summary": "集成测试返回的事件",
                    "confidence": 0.8,
                    "evidence": [{"msg_id": mid, "role": "origin", "note": "首现"}],
                }
            ]
        elif kind == "topic":
            rows = [
                {
                    "title": "集成测试话题",
                    "keywords": ["测试"],
                    "summary": "集成测试返回的话题",
                    "confidence": 0.8,
                    "evidence_msg_id": msg_ids[:2],
                }
            ]
        else:
            if not members or not msg_ids:
                return "[]"
            row = {"address": "测试称呼", "from": members[0], "confidence": 0.9,
                   "evidence_msg_id": msg_ids[0]}
            if len(members) > 1:
                row["to"] = members[1]
            rows = [row]
        return json.dumps(rows, ensure_ascii=False)


# --------------------------------------------------------------------------
# 假 OneBot 客户端（反向 WS，universal 角色：收 API 请求 / 发事件）
# --------------------------------------------------------------------------


class FakeOneBot:
    def __init__(self, port: int) -> None:
        self.url = f"ws://127.0.0.1:{port}/ws"
        self.ws = None
        self.api_log: list[dict] = []
        self._reader: asyncio.Task | None = None
        self._reply_mid = 700000
        self._get_msg_payload: dict = {}

    async def start(self, timeout: float = 120.0) -> None:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                self.ws = await ws_connect(
                    self.url,
                    additional_headers={
                        "X-Client-Role": "universal",
                        "X-Self-ID": SELF_ID,
                    },
                    open_timeout=5,
                )
                break
            except Exception as e:  # noqa: BLE001
                last = e
                await asyncio.sleep(1)
        else:
            raise RuntimeError(f"逆向 WS 连接失败: {last}")
        self._reader = asyncio.create_task(self._read(), name="onebot-reader")

    async def close(self) -> None:
        if self._reader:
            self._reader.cancel()
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:  # noqa: BLE001
                pass

    async def _read(self) -> None:
        try:
            async for raw in self.ws:
                try:
                    payload = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(payload, dict) or "action" not in payload:
                    continue
                entry = {
                    "ts": time.time(),
                    "action": payload.get("action"),
                    "params": payload.get("params") or {},
                }
                self.api_log.append(entry)
                try:
                    data = self._respond(payload["action"], payload.get("params") or {})
                    entry["data"] = data
                    await self.ws.send(
                        json.dumps(
                            {"status": "ok", "retcode": 0, "data": data,
                             "echo": payload.get("echo")},
                            ensure_ascii=False,
                        )
                    )
                except Exception as e:  # noqa: BLE001
                    entry["error"] = str(e)
                    try:
                        await self.ws.send(
                            json.dumps(
                                {"status": "failed", "retcode": 1404, "msg": str(e),
                                 "echo": payload.get("echo")},
                                ensure_ascii=False,
                            )
                        )
                    except Exception:  # noqa: BLE001
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            print(f"  [onebot] 读循环退出: {e}")

    def _respond(self, action: str, params: dict):
        if action in REPLY_ACTIONS:
            self._reply_mid += 1
            return {"message_id": self._reply_mid}
        if action == "get_group_member_info":
            uid = params.get("user_id")
            return {"group_id": params.get("group_id"), "user_id": uid,
                    "card": f"群昵称{uid}", "nickname": f"昵称{uid}"}
        if action == "get_stranger_info":
            uid = params.get("user_id")
            return {"user_id": uid, "nickname": f"昵称{uid}", "nick": f"昵称{uid}"}
        if action == "get_msg":
            return dict(self._get_msg_payload)
        if action == "get_login_info":
            return {"user_id": int(SELF_ID), "nickname": "测试机器人"}
        if action == "get_group_info":
            return {"group_id": params.get("group_id"), "group_name": "测试群"}
        return {}

    # -- 事件注入 ---------------------------------------------------

    async def send(self, payload: dict) -> None:
        assert self.ws is not None
        await self.ws.send(json.dumps(payload, ensure_ascii=False))

    async def group_text(self, mid: int, user: str, text: str, *, group: str = GROUP,
                         nickname: str | None = None) -> None:
        await self.send(
            {
                "time": int(time.time()),
                "self_id": int(SELF_ID),
                "post_type": "message",
                "message_type": "group",
                "sub_type": "normal",
                "message_id": mid,
                "group_id": int(group),
                "user_id": int(user),
                "message": [{"type": "text", "data": {"text": text}}],
                "raw_message": text,
                "font": 0,
                "sender": {"user_id": int(user), "nickname": nickname or f"昵称{user}",
                           "card": "", "role": "member"},
            }
        )

    async def group_segments(self, mid: int, user: str, segments: list[dict]) -> None:
        await self.send(
            {
                "time": int(time.time()),
                "self_id": int(SELF_ID),
                "post_type": "message",
                "message_type": "group",
                "sub_type": "normal",
                "message_id": mid,
                "group_id": int(GROUP),
                "user_id": int(user),
                "message": segments,
                "raw_message": "",
                "font": 0,
                "sender": {"user_id": int(user), "nickname": f"昵称{user}",
                           "card": "", "role": "member"},
            }
        )

    async def private_text(self, mid: int, user: str, text: str) -> None:
        await self.send(
            {
                "time": int(time.time()),
                "self_id": int(SELF_ID),
                "post_type": "message",
                "message_type": "private",
                "sub_type": "normal",
                "message_id": mid,
                "user_id": int(user),
                "message": [{"type": "text", "data": {"text": text}}],
                "raw_message": text,
                "font": 0,
                "sender": {"user_id": int(user), "nickname": f"昵称{user}",
                           "card": "", "role": "friend"},
            }
        )

    async def group_notice(self, mid: int, user: str) -> None:
        await self.send(
            {
                "time": int(time.time()),
                "self_id": int(SELF_ID),
                "post_type": "notice",
                "notice_type": "group_increase",
                "sub_type": "approve",
                "group_id": int(GROUP),
                "user_id": int(user),
                "operator_id": int(ADMIN),
            }
        )

    # -- 断言用 -----------------------------------------------------

    def replies(self) -> list[str]:
        return [
            chain_text(e["params"].get("message"))
            for e in self.api_log
            if e["action"] in REPLY_ACTIONS
        ]

    def count(self, action: str) -> int:
        return sum(1 for e in self.api_log if e["action"] == action)

    @property
    def reply_count(self) -> int:
        return sum(1 for e in self.api_log if e["action"] in REPLY_ACTIONS)

    def reply_log(self) -> list[dict]:
        return [e for e in self.api_log if e["action"] in REPLY_ACTIONS]

    def last_reply_containing(self, needle: str) -> str | None:
        for t in reversed(self.replies()):
            if needle in t:
                return t
        return None


# --------------------------------------------------------------------------
# 实例管理
# --------------------------------------------------------------------------


class Instance:
    def __init__(self, root: Path, astrbot_bin: str) -> None:
        self.root = root
        self.bin = astrbot_bin
        self.proc: subprocess.Popen | None = None
        self.log_path = root / "astrbot-integration.log"
        self._log_fh = None

    def init(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, ASTRBOT_ROOT=str(self.root))
        r = subprocess.run(
            [self.bin, "init", "-y"],
            cwd=str(self.root), env=env, capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0:
            raise RuntimeError(f"astrbot init 失败: {r.stdout}\n{r.stderr}")
        if not (self.root / "data" / "cmd_config.json").exists():
            # ``astrbot init`` 只建目录，cmd_config.json 由首次 run 生成
            self._materialize_config()

    def _materialize_config(self, timeout: float = 120.0) -> None:
        env = dict(os.environ, ASTRBOT_ROOT=str(self.root))
        proc = subprocess.Popen(
            [self.bin, "run"], cwd=str(self.root), env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + timeout
        try:
            while time.time() < deadline:
                if (self.root / "data" / "cmd_config.json").exists():
                    break
                if proc.poll() is not None:
                    break
                time.sleep(0.5)
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
        if not (self.root / "data" / "cmd_config.json").exists():
            raise RuntimeError("首次运行未能生成 data/cmd_config.json")

    def read_cmd(self) -> dict:
        return json.loads((self.root / "data" / "cmd_config.json").read_text(encoding="utf-8-sig"))

    def write_cmd(self, cfg: dict) -> None:
        (self.root / "data" / "cmd_config.json").write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def link_plugin(self) -> None:
        dest = self.root / "data" / "plugins" / PLUGIN_DIR.name
        if dest.is_symlink() or dest.exists():
            if dest.is_symlink():
                dest.unlink()
            else:
                shutil.rmtree(dest, ignore_errors=True)
        dest.symlink_to(PLUGIN_DIR, target_is_directory=True)

    def configure(self, *, ws_port: int, dash_port: int, trap_port: int,
                  analysis_enabled: bool) -> None:
        cfg = self.read_cmd()
        cfg["platform"] = [
            {
                "id": "gs-qq",
                "type": "aiocqhttp",
                "enable": True,
                "ws_reverse_host": "127.0.0.1",
                "ws_reverse_port": ws_port,
                "ws_reverse_token": "",
            }
        ]
        cfg["admins_id"] = [ADMIN]
        cfg["wake_prefix"] = ["/"]
        cfg["platform_settings"]["enable_id_white_list"] = False
        cfg["platform_settings"]["id_whitelist"] = []
        # 压测式灌入，别被默认 30 条/60s 限速
        cfg["platform_settings"]["rate_limit"] = {
            "time": 60, "count": 1000, "strategy": "stall",
        }
        cfg["platform_settings"]["no_permission_reply"] = True
        # 私聊也要唤醒前缀：否则 AstrBot 会把任意私聊直接丢给 LLM
        cfg["platform_settings"]["friend_message_needs_wake_prefix"] = True
        cfg["dashboard"]["port"] = dash_port
        cfg["dashboard"]["password_change_required"] = False
        from astrbot.core.utils.auth_password import (  # type: ignore
            hash_dashboard_password,
            hash_md5_dashboard_password,
        )

        cfg["dashboard"]["pbkdf2_password"] = hash_dashboard_password(DASH_PASSWORD)
        cfg["dashboard"]["password"] = hash_md5_dashboard_password(DASH_PASSWORD)
        cfg["dashboard"]["password_storage_upgraded"] = True
        cfg["provider"] = [
            {
                "id": "trap",
                "provider": "openai",
                "type": "openai_chat_completion",
                "provider_type": "chat_completion",
                "enable": True,
                "key": ["sk-trap"],
                "api_base": f"http://127.0.0.1:{trap_port}/v1",
                "timeout": 30,
                "proxy": "",
                "custom_headers": {},
            }
        ]
        cfg["provider_settings"]["enable"] = True
        self.write_cmd(cfg)

        pconf = {
            "capture_enabled": True,
            "group_whitelist": [f"aiocqhttp:{GROUP}"],
            "group_blacklist": [],
            "excluded_users": [f"aiocqhttp:{EXCLUDED}"],
            "record_notices": True,
            "analysis_enabled": analysis_enabled,
            "llm_provider_id": "trap",
            "analysis_concurrency": 1,
            "analysis_interval_ms": 200,
            "analysis_daily_limit": 200,
            "analysis_window_hours": 24,
            "analysis_batch_size": 3,
            "backup_auto": False,
            "backup_keep": 7,
            "log_detail": False,
        }
        (self.root / "data" / "config" /
         f"{PLUGIN_DIR.name}_config.json").write_text(
            json.dumps(pconf, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def set_analysis(self, enabled: bool) -> None:
        path = (self.root / "data" / "config" / f"{PLUGIN_DIR.name}_config.json")
        conf = json.loads(path.read_text(encoding="utf-8-sig"))
        conf["analysis_enabled"] = enabled
        path.write_text(json.dumps(conf, ensure_ascii=False, indent=2), encoding="utf-8")

    def start(self) -> None:
        env = dict(os.environ, ASTRBOT_ROOT=str(self.root))
        self._log_fh = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            [self.bin, "run"],
            cwd=str(self.root), env=env,
            stdout=self._log_fh, stderr=subprocess.STDOUT,
        )

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""

    def wait_log(self, needle: str, timeout: float = 120.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if needle in self.log_text():
                return True
            if self.proc and self.proc.poll() is not None:
                return needle in self.log_text()
            time.sleep(0.5)
        return needle in self.log_text()

    def stop(self, timeout: float = 60.0) -> int | None:
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        code = self.proc.returncode
        self.proc = None
        if self._log_fh:
            self._log_fh.close()
            self._log_fh = None
        return code


# --------------------------------------------------------------------------
# 测试轮次
# --------------------------------------------------------------------------


async def round_one(inst: Instance, ob: FakeOneBot, trap: Trap) -> None:
    print("\n=== R1：采集链路 + 权限 + 规则层零 LLM ===")
    inst.wait_log("Loading plugin " + PLUGIN_DIR.name, 120)
    ok = inst.wait_log("GroupScope", 30)
    check(ok, "插件在日志中被加载")
    await ob.start()

    # -- 注入消息（含越界样本） ------------------------------------
    await ob.group_text(1001, ADMIN, "大家晚上好，今晚开黑吗")
    await ob.group_text(1001, ADMIN, "大家晚上好，今晚开黑吗")  # 重复 message_id
    await asyncio.sleep(0.4)
    ob._get_msg_payload = {
        "time": int(time.time()) - 60,
        "self_id": int(SELF_ID),
        "post_type": "message",
        "message_type": "group",
        "sub_type": "normal",
        "message_id": 9000001,
        "group_id": int(GROUP),
        "user_id": int(MEMBER),
        "message": [{"type": "text", "data": {"text": "被引用的历史消息"}}],
        "sender": {"user_id": int(MEMBER), "nickname": f"昵称{MEMBER}", "card": "",
                   "role": "member"},
    }
    await ob.group_segments(
        1002, MEMBER,
        [{"type": "text", "data": {"text": "回复楼上的老哥"}},
         {"type": "reply", "data": {"id": "9000001"}}],
    )
    await asyncio.sleep(0.4)
    await ob.group_segments(
        1003, "9003",
        [{"type": "at", "data": {"qq": "9004"}},
         {"type": "text", "data": {"text": " 一起冲"}}],
    )
    await asyncio.sleep(0.4)
    await ob.group_text(1004, EXCLUDED, "我不该被记录（成员在排除名单）")
    await ob.group_text(1005, ADMIN, "白名单外的群也不该记录", group=OUT_GROUP)
    await ob.private_text(1006, ADMIN, "私聊不该被记录")
    await asyncio.sleep(0.4)
    await ob.group_notice(1007, "9003")

    n = await wait_until(
        lambda: (db_one(inst.root, "SELECT COUNT(*) FROM messages") or (0,))[0] == 4,
        timeout=30,
    )
    rows = db_rows(
        inst.root,
        "SELECT message_id, group_raw_id, sender_raw_id, message_type, event_kind"
        " FROM messages ORDER BY id",
    )
    check(n is True or (rows and len(rows) == 4), f"入库 4 条（去重后），实际 {rows}")

    ids = {r[0] for r in rows}
    check("1001" in ids and "1002" in ids and "1003" in ids,
          f"普通/引用/@ 消息均已入库：{sorted(ids)}")
    dup = db_rows(inst.root, "SELECT COUNT(*) FROM messages WHERE message_id='1001'")
    check(dup and dup[0][0] == 1, "重复 message_id 被去重（1001 只存 1 条）")
    notice = db_rows(inst.root, "SELECT COUNT(*) FROM messages WHERE event_kind='notice'")
    check(notice and notice[0][0] == 1, "通知事件（group_increase）入库且标记为 notice")
    check(not db_rows(inst.root, "SELECT 1 FROM messages WHERE group_raw_id=? ", (OUT_GROUP,)),
          "白名单外的群 0 条入库")
    check(not db_rows(inst.root, "SELECT 1 FROM messages WHERE sender_raw_id=?", (EXCLUDED,)),
          "排除成员 0 条入库")
    check(not db_rows(inst.root, "SELECT 1 FROM messages WHERE message_type='friend'"),
          "私聊 0 条入库")
    seg_types = {r[0] for r in db_rows(inst.root, "SELECT seg_type FROM message_segments")}
    check({"Plain", "At", "Reply"} <= seg_types, f"消息段类型齐全：{sorted(seg_types)}")
    reply_row = db_rows(
        inst.root,
        "SELECT reply_to_message_id, reply_to_sender_id FROM messages WHERE message_id='1002'",
    )
    check(reply_row and reply_row[0][0] == "9000001",
          f"reply 段解析出被引用消息 id：{reply_row}")
    mention = db_rows(
        inst.root,
        "SELECT kind, target_raw_id FROM message_mentions WHERE message_ref="
        "(SELECT id FROM messages WHERE message_id='1003')",
    )
    check(bool(mention) and mention[0][1] == "9004", f"@ 记录落库：{mention}")
    check(ob.count("get_group_member_info") >= 1, "@ 消息触发 get_group_member_info")
    check(ob.count("get_msg") >= 1, "引用消息触发 get_msg")
    seg_rows = db_rows(inst.root, "SELECT COUNT(*) FROM message_segments")
    check(seg_rows and seg_rows[0][0] >= 5, f"消息段总数 {seg_rows}（1+2+2）")

    # -- 指令：权限 + 回复 -----------------------------------------
    t_cmd = time.time()
    await ob.group_text(2001, ADMIN, "/gs status")
    await asyncio.sleep(1.0)
    await ob.group_text(2002, MEMBER, "/gs status")          # 非管理员
    await asyncio.sleep(1.0)
    await ob.group_text(2003, ADMIN, "/gs backup")
    await asyncio.sleep(1.0)
    await ob.group_text(2004, ADMIN, f"/gs analyze {GROUP} rule")

    got = await wait_until(lambda: ob.reply_count >= 4, timeout=30)
    check(got is True, f"指令至少收到 4 次回复（实际 {ob.reply_count}）")
    check(ob.last_reply_containing("GroupScope 状态") is not None,
          "管理员 /gs status 有状态回复")
    check(ob.last_reply_containing("权限不足") is not None,
          "非管理员 /gs status 收到权限不足提示")
    check(ob.last_reply_containing("备份完成") is not None, "/gs backup 有备份完成回复")
    early = [e for e in ob.reply_log() if e["ts"] < t_cmd - 0.2]
    check(not early, "第一条指令之前没有任何回复（普通/越界/私聊消息零回复）")
    late = [e for e in ob.reply_log() if e["ts"] >= t_cmd - 0.2]
    check(len(late) == ob.reply_count, "所有回复都发生在指令注入之后")

    backups = list((inst.root / "data" / "plugin_data" /
                    PLUGIN_DIR.name / "backups").glob("*.db"))
    check(bool(backups), f"备份文件已生成：{[p.name for p in backups]}")

    # -- 规则层跑完，但 LLM 零命中 ---------------------------------
    task = await wait_until(
        lambda: (
            (db_one(
                inst.root,
                "SELECT COUNT(*) FROM analysis_tasks WHERE kind='addressing_rule'"
                " AND status='done'",
            ) or (0,))[0]
            >= 1
        ),
        timeout=60,
    )
    n_rule = db_one(
        inst.root, "SELECT COUNT(*) FROM analysis_tasks WHERE kind='addressing_rule'"
    )
    done = db_one(
        inst.root, "SELECT COUNT(*) FROM analysis_tasks WHERE status='done'"
    )
    check(bool(n_rule and n_rule[0] >= 1), f"规则任务已入队：{n_rule}")
    check(bool(done and done[0] >= 1), f"任务已完成：{done}")
    check(len(trap.hits) == 0,
          f"陷阱服务命中 {len(trap.hits)} 次（期望 0：规则层 + 非唤醒消息零 LLM）")
    check(trap.hits == [] and ob.reply_count >= 4,
          "非指令消息零回复（回复仅来自 4 条指令）")

    addr = db_rows(inst.root, "SELECT COUNT(*) FROM addressing")
    print(f"  · 规则称呼条目：{addr}")


async def round_two(inst: Instance, ob: FakeOneBot, trap: Trap) -> None:
    print("\n=== R2：开启 AI 分析，真实 LLM 链路 ===")
    inst.set_analysis(True)
    inst.start()
    inst.wait_log("Loading plugin " + PLUGIN_DIR.name, 120)
    await ob.start()

    await ob.group_text(3001, ADMIN, "最近大家都在聊爬山和新番")
    await asyncio.sleep(1.5)
    t_cmd = time.time()
    await ob.group_text(3002, ADMIN, f"/gs analyze {GROUP} llm")
    got = await wait_until(lambda: len(trap.hits) >= 1, timeout=90)
    check(got is True or len(trap.hits) >= 1,
          f"LLM 调用命中陷阱服务 {len(trap.hits)} 次：{[h['kind'] for h in trap.hits]}")

    await wait_until(
        lambda: (
            (db_one(
                inst.root,
                "SELECT COUNT(*) FROM analysis_tasks WHERE status='done'"
                " AND kind LIKE '%llm%'",
            ) or (0,))[0]
            >= 1
        ),
        timeout=90,
    )
    llm_done = db_one(
        inst.root,
        "SELECT COUNT(*) FROM analysis_tasks WHERE status='done' AND kind LIKE '%llm%'",
    )
    check(bool(llm_done and llm_done[0] >= 1), f"LLM 任务完成：{llm_done}")

    addr_llm = db_rows(
        inst.root, "SELECT address, source, confidence, evidence_message FROM addressing"
        " WHERE source='llm'"
    )
    check(bool(addr_llm), f"addressing 出现 source=llm 结论：{addr_llm}")
    if addr_llm:
        ev = addr_llm[0][3]
        linked = db_rows(inst.root, "SELECT 1 FROM messages WHERE id=?", (ev,))
        check(bool(linked), f"LLM 结论的证据指向真实消息 id={ev}")
    addr_ev_null = db_rows(
        inst.root, "SELECT COUNT(*) FROM addressing WHERE source='llm'"
        " AND (evidence_message IS NULL OR evidence_message=0)"
    )
    check(bool(addr_ev_null and addr_ev_null[0][0] == 0),
          "没有证据为空的 LLM 称呼结论")

    topics = db_rows(inst.root, "SELECT COUNT(*) FROM topics WHERE source='llm'")
    events = db_rows(inst.root, "SELECT COUNT(*) FROM events WHERE source='llm'")
    print(f"  · topics(source=llm)={topics}  events(source=llm)={events}")

    await ob.group_text(3003, ADMIN, "/gs tasks")
    tasks_reply = await wait_until(
        lambda: ob.last_reply_containing("【分析任务】"), timeout=30
    )
    check(bool(tasks_reply), "/gs tasks 有任务统计回复")
    if tasks_reply:
        print("  · /gs tasks →", tasks_reply.replace("\n", " | ")[:220])
        check("LLM: 调用" in tasks_reply, "/gs tasks 显示 LLM 调用计数")

    # Phase 6 范围参数
    yesterday = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
    today = time.strftime("%Y-%m-%d")
    # /gs 的范围提示格式是 %m-%d %H:%M（见 main._fmt_time）
    hint = time.strftime(
        "%m-%d", time.strptime(yesterday, "%Y-%m-%d")
    )
    await ob.group_text(3004, ADMIN, f"/gs analyze {GROUP} rule since={yesterday} until={today}")
    reply = await wait_until(
        lambda: ob.last_reply_containing(f"范围: {hint}"), timeout=30
    )
    check(bool(reply),
          f"since/until 范围参数生效（回复含 范围: {hint}；全部回复: {ob.replies()}）")

    await ob.group_text(3005, ADMIN, "/gs analyze 90001 rule since=2026-13-40")
    bad = await wait_until(lambda: ob.last_reply_containing("日期格式错误"), timeout=30)
    check(bool(bad), "非法日期参数被拦截并提示")

    early = [e for e in ob.reply_log() if e["ts"] < t_cmd - 0.2]
    check(not early, "R2 中普通消息依旧零回复")

    # WebUI：真实登录 Dashboard 后调用扩展 API
    await webui_check(inst)


async def webui_check(inst: Instance) -> None:
    """通过真实 Dashboard 登录态调用插件扩展 API（Phase 5 端到端）。"""
    import aiohttp

    cfg = inst.read_cmd()
    port = cfg["dashboard"]["port"]
    base = f"http://127.0.0.1:{port}"
    token = None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{base}/api/v1/auth/login",
                json={"username": cfg["dashboard"]["username"],
                      "password": DASH_PASSWORD},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                body = await r.json(content_type=None)
            b = body if isinstance(body, dict) else {}
            data = b.get("data") or {}
            data = data if isinstance(data, dict) else {}
            token = (data.get("token") or data.get("access_token")
                     or b.get("token") or b.get("access_token"))
            check(bool(token), f"Dashboard 登录成功（HTTP {r.status}）")
            if not token:
                print("  · 登录响应:", json.dumps(body, ensure_ascii=False)[:300])
                return
            hdr = {"Authorization": f"Bearer {token}"}
            async with s.get(
                f"{base}/api/v1/plugins/extensions/{PLUGIN_DIR.name}/status",
                headers=hdr, timeout=aiohttp.ClientTimeout(total=20),
            ) as r2:
                b2 = await r2.json(content_type=None)
            check(r2.status == 200 and isinstance(b2, dict) and "data" in b2,
                  f"扩展 API /status 经 Dashboard 返回 {r2.status}")
            async with s.get(
                f"{base}/api/v1/plugins/extensions/{PLUGIN_DIR.name}/messages?q=爬山",
                headers=hdr, timeout=aiohttp.ClientTimeout(total=20),
            ) as r3:
                b3 = await r3.json(content_type=None)
            items = ((b3 or {}).get("data") or {}).get("items")
            check(r3.status == 200 and isinstance(items, list),
                  f"扩展 API /messages 搜索返回 {r3.status}，命中 {len(items or [])} 条")
    except Exception as e:  # noqa: BLE001
        check(False, f"WebUI 端到端检查异常: {e}")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def run(args: argparse.Namespace) -> int:
    root = Path(args.root)
    ws_port = free_port()
    dash_port = free_port()

    async def workflow() -> None:
        trap = Trap()
        trap_port = await trap.start()
        inst = Instance(root, args.astrbot)
        ob = FakeOneBot(ws_port)
        try:
            print(f"ASTRBOT_ROOT = {root}")
            print(f"逆向 WS 端口 = {ws_port} | Dashboard = {dash_port} | 陷阱 = {trap_port}")
            inst.init()
            inst.link_plugin()
            inst.configure(
                ws_port=ws_port, dash_port=dash_port, trap_port=trap_port,
                analysis_enabled=False,
            )
            inst.start()
            await round_one(inst, ob, trap)
            await ob.close()          # 先断开假协议端，免得卡住停机
            code = inst.stop(timeout=90)
            check(code is not None and code >= 0,
                  f"R1 SIGINT 干净停机（未被 SIGKILL，exit={code}）")
            check("GroupScope 已卸载" in inst.log_text(), "R1 停机时插件被正常卸载")
            quick = sqlite3_quick_check(root)
            check(quick == "ok", f"R1 停机后 PRAGMA quick_check = {quick}")

            # ---- R2 ----
            ob2 = FakeOneBot(ws_port)
            await round_two(inst, ob2, trap)
            await ob2.close()
            code2 = inst.stop(timeout=90)
            check(code2 is not None and code2 >= 0,
                  f"R2 SIGINT 干净停机（未被 SIGKILL，exit={code2}）")
            check("GroupScope 已卸载" in inst.log_text(), "R2 停机时插件被正常卸载")
            quick2 = sqlite3_quick_check(root)
            check(quick2 == "ok", f"R2 停机后 PRAGMA quick_check = {quick2}")
        finally:
            if inst.proc is not None:
                inst.stop(timeout=20)
            await trap.stop()

    asyncio.run(workflow())

    print("\n" + "=" * 62)
    if FAILURES:
        print(f"❌ 集成测试失败 {len(FAILURES)} 项：")
        for f in FAILURES:
            print(f"   - {f}")
        print(f"实例目录: {root}")
        return 1
    print("✅ 集成测试全部断言通过（R1 零 LLM + R2 真实 LLM 链路）")
    if not args.keep:
        shutil.rmtree(root, ignore_errors=True)
    else:
        print(f"实例目录保留: {root}")
    return 0


def sqlite3_quick_check(root: Path) -> str:
    db = root / "data" / "plugin_data" / PLUGIN_DIR.name / "groupscope.db"
    if not db.exists():
        return "missing"
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
    try:
        row = con.execute("PRAGMA quick_check").fetchone()
        return str(row[0]) if row else "unknown"
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="GroupScope 端到端集成测试")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="隔离的 ASTRBOT_ROOT")
    parser.add_argument("--astrbot", default=str(PLUGIN_DIR / ".venv" / "bin" / "astrbot"),
                        help="astrbot CLI 路径")
    parser.add_argument("--keep", action="store_true", help="保留测试实例目录")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
