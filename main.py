"""GroupScope 插件入口。

职责边界：
* 事件回调只做「隐私过滤 -> 规范化 -> 入队」，全部异常隔离；
* 持久化、批量写入、备份/导出在 ``groupscope`` 包内完成；
* AI 分析不在本版本范围内，原始消息表只追加、不被推断修改。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star

from .groupscope import PLUGIN_NAME
from .groupscope.analysis import AddressingStore, LLMGateway
from .groupscope.analysis import runner as analysis_runner_mod
from .groupscope.capture.listener import MessageListener
from .groupscope.capture.service import CaptureService
from .groupscope.config import AnalysisSettings, CaptureSettings
from .groupscope.ops import backup as backup_ops
from .groupscope.ops import export as export_ops
from .groupscope.ops import archives as archives_ops
from .groupscope.paths import plugin_data_dir
from .groupscope.privacy import CapturePolicy
from .groupscope.social import profiles as social_profiles
from .groupscope.social import snapshots as social_snapshots
from .groupscope.storage import repo
from .groupscope.storage.db import Database

ADMIN = filter.PermissionType.ADMIN

_KIND_LABELS = {
    analysis_runner_mod.KIND_RULE: "规则称呼",
    analysis_runner_mod.KIND_LLM: "LLM称呼",
    analysis_runner_mod.KIND_TOPIC_RULE: "规则话题",
    analysis_runner_mod.KIND_TOPIC_LLM: "LLM话题",
    analysis_runner_mod.KIND_EVENT_RULE: "规则事件",
    analysis_runner_mod.KIND_EVENT_LLM: "LLM事件",
}


def _fmt_time(ts: int | None) -> str:
    if not ts:
        return "-"
    return time.strftime("%m-%d %H:%M", time.localtime(ts))


class Main(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context, config)
        self.conf: dict[str, Any] = config if isinstance(config, dict) else {}
        self.data_dir = plugin_data_dir(getattr(self, "name", None) or PLUGIN_NAME)
        self.settings = CaptureSettings.from_conf(self.conf)
        self.analysis_settings = AnalysisSettings.from_conf(self.conf)
        self.policy = CapturePolicy(self.settings)
        self.db = Database(self.data_dir / "groupscope.db")
        self.service: CaptureService | None = None
        self.listener: MessageListener | None = None
        self.gateway: LLMGateway | None = None
        self.runner: analysis_runner_mod.AnalysisRunner | None = None
        self._start_lock = asyncio.Lock()
        self._started = False
        self._rejected = 0
        self._init_errors = 0
        # Phase 5：注册 WebUI 扩展 API（重复注册会替换，热重载安全）
        try:
            from .groupscope.web.api import register_web_api

            register_web_api(self)
        except Exception:
            if getattr(self, "logger", None):
                self.logger.exception("注册 GroupScope Web API 失败")

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def _ensure_started(self) -> None:
        """懒启动：首次消息或指令触发时打开数据库并启动写入服务。"""
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            await self.db.open()
            self.service = CaptureService(
                self.db, self.settings, self.policy, self.data_dir, self.logger
            )
            self.listener = MessageListener(
                self.policy,
                self.service.enqueue,
                self.logger,
                rejected=self._on_rejected,
            )
            await self.service.start()
            await self._start_analysis()
            self._started = True
            self.logger.info(
                "GroupScope 已就绪：数据库 %s，白名单 %s 个群",
                self.db.path,
                len(self.settings.group_whitelist),
            )

    async def _start_analysis(self) -> None:
        """创建 LLM 网关与分析循环（Phase 3）。规则层零 LLM 成本，始终可用。"""
        a = self.analysis_settings

        def _get_provider() -> Any:
            """复用 AstrBot 已配置 Provider：优先配置指定的，否则取第一个可用。"""
            try:
                pid = a.llm_provider_id
                if pid:
                    prov = self.context.get_provider_by_id(pid)
                    if prov is not None and hasattr(prov, "text_chat"):
                        return prov
                for prov in self.context.get_all_providers():
                    if hasattr(prov, "text_chat"):
                        return prov
            except Exception:
                self.logger.exception("获取 LLM Provider 失败")
            return None

        if a.analysis_enabled:
            self.gateway = LLMGateway(
                _get_provider,
                concurrency=a.analysis_concurrency,
                interval_ms=a.analysis_interval_ms,
                daily_limit=a.analysis_daily_limit,
                logger=self.logger,
            )
        else:
            self.gateway = None
        self.runner = analysis_runner_mod.AnalysisRunner(
            self.db, a, self.gateway, self.logger
        )
        try:
            await self.runner.start()
        except Exception:
            self.logger.exception("启动分析循环失败")

    def _on_rejected(self) -> None:
        self._rejected += 1

    async def terminate(self) -> None:
        """插件卸载/停用：停分析循环、排空队列、停维护任务、关闭数据库。"""
        try:
            if self.runner is not None:
                await self.runner.stop()
        except Exception:
            self.logger.exception("停止分析循环失败")
        try:
            if self.service is not None:
                await self.service.stop()
        except Exception:
            self.logger.exception("停止采集服务失败")
        try:
            if self.db.opened:
                await self.db.close()
        except Exception:
            self.logger.exception("关闭数据库失败")
        self._started = False
        self.logger.info("GroupScope 已卸载")

    # ------------------------------------------------------------------
    # 消息采集
    # ------------------------------------------------------------------

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent) -> None:
        """记录群聊原始消息（普通协程：不 yield、不设置结果、异常隔离）。"""
        try:
            await self._ensure_started()
        except Exception:
            self._init_errors += 1
            self.logger.exception("GroupScope 启动失败，跳过本条消息")
            return
        assert self.listener is not None
        await self.listener.handle_event(event)  # 内部全量异常隔离

    # ------------------------------------------------------------------
    # 指令
    # ------------------------------------------------------------------

    @filter.command_group("gs")
    def gs(self) -> None:
        """GroupScope 群聊数字档案"""

    @staticmethod
    def _args(event: AstrMessageEvent) -> list[str]:
        parts = event.message_str.split()
        if parts and parts[0].lstrip("/") == "gs":
            parts = parts[1:]
        if parts:  # 去掉子指令名
            parts = parts[1:]
        return parts

    async def _ready(self) -> bool:
        try:
            await self._ensure_started()
            return True
        except Exception:
            self.logger.exception("GroupScope 未就绪")
            return False

    @gs.command("status")
    @filter.permission_type(ADMIN)
    async def gs_status(self, event: AstrMessageEvent):
        """查看采集状态与运行指标"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        assert self.service is not None and self.db.opened
        m = self.service.metrics
        size = await self.db.file_size()
        paused = len(self.policy.paused)
        a = self.analysis_settings
        lines = [
            "【GroupScope 状态】",
            f"总开关: {'开' if self.settings.capture_enabled else '关'}"
            f" | 白名单群: {len(self.settings.group_whitelist)}"
            f" | 已暂停群: {paused}"
            f" | AI 分析: {'开' if a.analysis_enabled else '关'}",
            f"队列深度: {self.service.queue_depth}"
            f" | 入队: {m['accepted']}"
            f" | 丢弃: {m['dropped']}"
            f" | 被策略拒绝: {self._rejected}",
            f"已写入: {m['written']} 条（新增 {m['inserted']}，批次 {m['batches']}，"
            f"写错误 {m['write_errors']}）",
            f"数据库: {self.db.path.name} ({size / 1024 / 1024:.2f} MB)",
            f"数据目录: {self.data_dir}",
        ]
        yield event.plain_result("\n".join(lines))

    @gs.command("stats")
    @filter.permission_type(ADMIN)
    async def gs_stats(self, event: AstrMessageEvent):
        """消息与成员统计"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        conn = self.db.conn
        total = await repo.count_messages(conn)
        notices = await repo.count_notices(conn)
        members = await repo.count_members(conn)
        groups = await repo.list_groups(conn)
        daily = await repo.daily_counts(conn, days=7)
        lines = [
            "【GroupScope 统计】",
            f"消息: {total}（其中通知事件 {notices}） | 成员: {members} | 群: {len(groups)}",
            "近 7 日消息: "
            + ("、".join(f"{d['day'][5:]}:{d['count']}" for d in daily) or "无"),
        ]
        for g in groups[:8]:
            lines.append(
                f"- {g['group_name'] or g['group_id']} ({g['platform_name']})"
                f" 最近活跃 {_fmt_time(g['last_seen'])}"
                f" {'[暂停]' if not g['capture_enabled'] else ''}"
            )
        yield event.plain_result("\n".join(lines))

    @gs.command("recent")
    @filter.permission_type(ADMIN)
    async def gs_recent(self, event: AstrMessageEvent):
        """查看最近消息：/gs recent [群ID] [条数]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        group_ref = None
        limit = 10
        if args and args[0].isdigit():
            limit = min(int(args[0]), 50)
        elif args:
            row = await repo.find_group(self.db.conn, args[0])
            if not row:
                yield event.plain_result(f"未找到群: {args[0]}")
                return
            group_ref = row["id"]
            if len(args) > 1 and args[1].isdigit():
                limit = min(int(args[1]), 50)
        rows = await repo.recent_messages(self.db.conn, group_ref, limit)
        if not rows:
            yield event.plain_result("暂无消息。")
            return
        lines = ["【最近消息】"]
        for r in reversed(rows):
            marker = "[通知]" if r["event_kind"] == "notice" else ""
            text = (r["text"] or "").replace("\n", " ")[:60]
            lines.append(
                f"{_fmt_time(r['sent_at'])} {marker}"
                f"[{r['sender_raw_name'] or r['sender_raw_id'] or '?'}] {text}"
            )
        yield event.plain_result("\n".join(lines))

    @gs.command("search")
    @filter.permission_type(ADMIN)
    async def gs_search(self, event: AstrMessageEvent):
        """关键词搜索：/gs search <关键词> [群ID]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result("用法: /gs search <关键词> [群ID]")
            return
        keyword = args[0]
        group_ref = None
        if len(args) > 1:
            row = await repo.find_group(self.db.conn, args[1])
            if not row:
                yield event.plain_result(f"未找到群: {args[1]}")
                return
            group_ref = row["id"]
        rows = await repo.search_messages(self.db.conn, keyword, group_ref, limit=15)
        if not rows:
            yield event.plain_result(f"未找到包含「{keyword}」的消息。")
            return
        lines = [f"【搜索: {keyword}】共 {len(rows)} 条（最多显示 15 条）"]
        for r in rows:
            text = (r["text"] or "").replace("\n", " ")[:60]
            lines.append(
                f"#{r['id']} {_fmt_time(r['sent_at'])}"
                f"[{r['sender_raw_name'] or r['sender_raw_id'] or '?'}] {text}"
            )
        yield event.plain_result("\n".join(lines))

    async def _apply_capture(self, event: AstrMessageEvent, enabled: bool) -> str:
        """按群条目切换采集开关，返回要回复的文本。"""
        if not await self._ready():
            return "GroupScope 未就绪，详见日志。"
        args = self._args(event)
        if not args:
            return f"用法: /gs {'resume' if enabled else 'pause'} <群ID|平台:群ID>"
        row = await repo.find_group(self.db.conn, args[0])
        if not row:
            return f"未找到群: {args[0]}（该群尚无消息）"
        await repo.set_group_capture(self.db.conn, row["id"], enabled)
        key = (str(row["platform_id"]), str(row["group_id"]))
        if enabled:
            self.policy.paused.discard(key)
        else:
            self.policy.paused.add(key)
        action = "恢复" if enabled else "暂停"
        return (
            f"已{action}采集群 {row['group_name'] or row['group_id']}"
            f"（{row['platform_name']}:{row['group_id']}）"
        )

    @gs.command("pause")
    @filter.permission_type(ADMIN)
    async def gs_pause(self, event: AstrMessageEvent):
        """暂停采集某个群：/gs pause <群ID|平台:群ID>"""
        yield event.plain_result(await self._apply_capture(event, enabled=False))

    @gs.command("resume")
    @filter.permission_type(ADMIN)
    async def gs_resume(self, event: AstrMessageEvent):
        """恢复采集某个群：/gs resume <群ID|平台:群ID>"""
        yield event.plain_result(await self._apply_capture(event, enabled=True))

    @gs.command("backup")
    @filter.permission_type(ADMIN)
    async def gs_backup(self, event: AstrMessageEvent):
        """立即备份数据库到 backups/ 目录"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        try:
            dest = await self.db.backup(
                self.data_dir / "backups", self.settings.backup_keep
            )
        except Exception as e:
            self.logger.exception("备份失败")
            yield event.plain_result(f"备份失败: {e}")
            return
        yield event.plain_result(f"备份完成: {dest.name}\n目录: {self.data_dir / 'backups'}")

    @gs.command("backups")
    @filter.permission_type(ADMIN)
    async def gs_backups(self, event: AstrMessageEvent):
        """列出备份文件"""
        items = backup_ops.list_backups(self.data_dir / "backups")
        if not items:
            yield event.plain_result("暂无备份。使用 /gs backup 立即备份。")
            return
        lines = ["【备份列表】（新→旧）"]
        lines += [f"- {p.name} ({p.stat().st_size / 1024 / 1024:.2f} MB)" for p in items[:10]]
        lines.append(f"恢复: /gs restore <文件名>")
        yield event.plain_result("\n".join(lines))

    @gs.command("restore")
    @filter.permission_type(ADMIN)
    async def gs_restore(self, event: AstrMessageEvent):
        """从备份恢复：/gs restore <文件名>（当前库会归档为 .pre-restore-*）"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result("用法: /gs restore <文件名>，先用 /gs backups 查看。")
            return
        backups_dir = self.data_dir / "backups"
        target = backups_dir / Path(args[0]).name
        if not target.is_file():
            yield event.plain_result(f"备份不存在: {target.name}")
            return
        assert self.service is not None
        try:
            await self.service.stop()
            await self.db.close()
            archived = await backup_ops.restore_backup(self.db, target)
            await self.db.open()
            await self.service.start()
        except Exception as e:
            self.logger.exception("恢复失败")
            try:
                if not self.db.opened:
                    await self.db.open()
                if self.service is not None:
                    await self.service.start()
            except Exception:
                pass
            yield event.plain_result(f"恢复失败: {e}")
            return
        msg = f"已从 {target.name} 恢复数据库。"
        if archived:
            msg += f"\n原库已归档: {archived.name}"
        yield event.plain_result(msg)

    @gs.command("export")
    @filter.permission_type(ADMIN)
    async def gs_export(self, event: AstrMessageEvent):
        """导出消息档案：/gs export <json|csv> [群ID]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        fmt = (args[0].lower() if args else "json")
        if fmt not in ("json", "csv"):
            yield event.plain_result("用法: /gs export <json|csv> [群ID]")
            return
        group_ref = None
        if len(args) > 1:
            row = await repo.find_group(self.db.conn, args[1])
            if not row:
                yield event.plain_result(f"未找到群: {args[1]}")
                return
            group_ref = row["id"]
        try:
            dest = await export_ops.export_messages(
                self.db, self.data_dir / "exports", fmt, group_ref
            )
        except Exception as e:
            self.logger.exception("导出失败")
            yield event.plain_result(f"导出失败: {e}")
            return
        yield event.plain_result(f"导出完成: {dest}\n({dest.stat().st_size / 1024:.1f} KB)")

    # ------------------------------------------------------------------
    # Phase 2：人物与关系
    # ------------------------------------------------------------------

    @gs.command("member")
    @filter.permission_type(ADMIN)
    async def gs_member(self, event: AstrMessageEvent):
        """成员档案：/gs member <成员ID|昵称>"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result("用法: /gs member <成员ID|昵称>")
            return
        keyword = args[0]
        profile = None
        if keyword.isdigit():
            profile = await social_profiles.member_profile(self.db.conn, int(keyword))
        if profile is None:
            hits = await social_profiles.member_search(self.db.conn, keyword, limit=5)
            if not hits:
                yield event.plain_result(f"未找到成员: {keyword}")
                return
            if len(hits) > 1:
                lines = [f"匹配到 {len(hits)} 个成员，请用更精确的 ID："]
                lines += [
                    f"- #{h['id']} {h['platform_id']}:{h['user_id']}"
                    f" {h['current_nickname'] or '-'} ({h['message_count']}条)"
                    for h in hits
                ]
                yield event.plain_result("\n".join(lines))
                return
            profile = await social_profiles.member_profile(
                self.db.conn, hits[0]["id"]
            )

        lines = [
            f"【成员档案】#{profile['id']} {profile['current_nickname'] or '-'}"
            f"（{profile['platform_id']}:{profile['user_id']}）",
            f"发言 {profile['message_count']} 条 | 首见 {_fmt_time(profile['first_seen'])}"
            f" | 最近 {_fmt_time(profile['last_seen'])}",
        ]
        for g in profile["groups"][:6]:
            lines.append(
                f"群 {g['group_name'] or g['group_id']}: {g['message_count']} 条"
                f"（最近 {_fmt_time(g['last_seen'])}）"
            )
        aliases = profile["aliases"]
        if aliases:
            lines.append(
                "别名史: "
                + "、".join(
                    f"{a['alias']}[{'人工' if a['source'] == 'manual' else '观察'}]"
                    f"×{a['seen_count']}"
                    for a in aliases[:8]
                )
            )
        out = [r for r in profile["interactions_out"] if r["kind"] != "at"][:5]
        at_out = [r for r in profile["interactions_out"] if r["kind"] == "at"][:5]
        if at_out:
            lines.append(
                "常 @: "
                + "、".join(
                    f"{r['other_nickname'] or r['other_user_id']}×{r['c']}"
                    for r in at_out
                )
            )
        if out:
            lines.append(
                "常回复: "
                + "、".join(
                    f"{r['other_nickname'] or r['other_user_id']}×{r['c']}"
                    for r in out
                )
            )
        if profile["recent_messages"]:
            lines.append("最近消息:")
            for m in profile["recent_messages"][:5]:
                text = (m["text"] or "").replace("\n", " ")[:40]
                marker = "[通知]" if m["event_kind"] == "notice" else ""
                lines.append(f"  {_fmt_time(m['sent_at'])}{marker} {text}")
        lines.append("（以上均为观察记录，不含推断）")
        yield event.plain_result("\n".join(lines))

    @gs.command("relations")
    @filter.permission_type(ADMIN)
    async def gs_relations(self, event: AstrMessageEvent):
        """关系窗口统计：/gs relations [群ID] [day|week|month|quarter]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        window = "week"
        group_ref = None
        for a in args:
            if a in social_snapshots.WINDOWS:
                window = a
            else:
                row = await repo.find_group(self.db.conn, a)
                if not row:
                    yield event.plain_result(f"未找到群: {a}")
                    return
                group_ref = row["id"]

        try:
            await social_snapshots.compute_snapshots(
                self.db.conn, group_ref=group_ref, window=window
            )
        except Exception as e:
            self.logger.exception("计算关系快照失败")
            yield event.plain_result(f"计算快照失败: {e}")
            return

        edges = await social_snapshots.snapshot_windows(
            self.db.conn, group_ref=group_ref, window=window
        )
        if not edges:
            yield event.plain_result(
                f"窗口 {social_snapshots.WINDOW_LABELS[window]}内没有足够的互动记录。"
            )
            return

        names: dict[int, str] = {}
        for e in edges:
            for key in ("from_member", "to_member"):
                if e[key] not in names:
                    cur = await self.db.conn.execute(
                        "SELECT user_id, current_nickname FROM members WHERE id=?",
                        (e[key],),
                    )
                    row = await cur.fetchone()
                    names[e[key]] = (
                        (row["current_nickname"] or row["user_id"]) if row else str(e[key])
                    )

        span = f"{_fmt_time(edges[0]['window_start'])} ~ {_fmt_time(edges[0]['window_end'])}"
        label = social_snapshots.WINDOW_LABELS[window]
        lines = [f"【关系窗口 · {label}】{span}", f"共 {len(edges)} 条有向互动边："]
        for e in edges[:20]:
            kinds = []
            if e["at_count"]:
                kinds.append(f"@×{e['at_count']}")
            if e["reply_count"]:
                kinds.append(f"回复×{e['reply_count']}")
            if e["quote_count"]:
                kinds.append(f"引用×{e['quote_count']}")
            lines.append(
                f"{names.get(e['from_member'], '?')} → {names.get(e['to_member'], '?')}: "
                f"{' '.join(kinds)} | 证据 {e['evidence_count']} 强度 {e['confidence']}"
            )
        lines.append("说明：仅统计 @/回复/引用等确定性互动与证据量，不构成亲密度评价。")
        yield event.plain_result("\n".join(lines))

    @gs.command("merge")
    @filter.permission_type(ADMIN)
    async def gs_merge(self, event: AstrMessageEvent):
        """人工合并成员身份：/gs merge <源ID> <目标ID>（先自动备份）"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if len(args) < 2 or not (args[0].isdigit() and args[1].isdigit()):
            yield event.plain_result(
                "用法: /gs merge <源成员ID> <目标成员ID>\n"
                "默认不合并身份；合并会留审计记录，执行前自动备份。"
            )
            return
        src, dst = int(args[0]), int(args[1])
        try:
            backup = await self.db.backup(
                self.data_dir / "backups", self.settings.backup_keep
            )
            message = await social_profiles.merge_members(
                self.db.conn, src, dst, operator="gs-command"
            )
        except ValueError as e:
            yield event.plain_result(str(e))
            return
        except Exception as e:
            self.logger.exception("合并成员失败")
            yield event.plain_result(f"合并失败: {e}")
            return
        yield event.plain_result(f"{message}\n合并前备份: {backup.name}")

    @gs.command("snapshot")
    @filter.permission_type(ADMIN)
    async def gs_snapshot(self, event: AstrMessageEvent):
        """重算关系快照：/gs snapshot [群ID] [窗口]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        window = "week"
        group_ref = None
        for a in args:
            if a in social_snapshots.WINDOWS:
                window = a
            else:
                row = await repo.find_group(self.db.conn, a)
                if not row:
                    yield event.plain_result(f"未找到群: {a}")
                    return
                group_ref = row["id"]
        try:
            n = await social_snapshots.compute_snapshots(
                self.db.conn, group_ref=group_ref, window=window
            )
        except Exception as e:
            self.logger.exception("计算快照失败")
            yield event.plain_result(f"计算失败: {e}")
            return
        yield event.plain_result(
            f"已计算 {n} 个 {social_snapshots.WINDOW_LABELS[window]}窗口的关系快照。"
        )

    # ------------------------------------------------------------------
    # Phase 3：称呼与语义分析
    # ------------------------------------------------------------------

    async def _find_group(self, token: str) -> Any:
        return await repo.find_group(self.db.conn, token)

    @staticmethod
    def _parse_range(args: list[str]) -> tuple[list[str], int | None, int | None]:
        """从参数中解析 ``since=YYYY-MM-DD`` / ``until=YYYY-MM-DD``。

        返回 ``(剩余参数, since_ts, until_ts)``；``until`` 含当天整天
        （+86400 秒），日期非法时抛 ``ValueError``（由调用方转成提示）。
        """
        rest: list[str] = []
        since: int | None = None
        until: int | None = None
        for a in args:
            key, sep, val = a.partition("=")
            if sep and key in ("since", "until") and val:
                try:
                    ts = int(time.mktime(time.strptime(val, "%Y-%m-%d")))
                except ValueError:
                    raise ValueError(f"日期格式错误: {val}（应为 YYYY-MM-DD）") from None
                if key == "since":
                    since = ts
                else:
                    until = ts + 86400  # 含当天
            else:
                rest.append(a)
        return rest, since, until

    @gs.command("address")
    @filter.permission_type(ADMIN)
    async def gs_address(self, event: AstrMessageEvent):
        """查询称呼体系：/gs address [群ID] [称呼关键词] [rule|llm|manual]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        group_ref = None
        address = None
        source = None
        for a in args:
            if a in ("rule", "llm", "manual"):
                source = a
                continue
            row = await self._find_group(a)
            if row:
                group_ref = row["id"]
            else:
                address = a
        store = AddressingStore(self.db.conn)
        rows = await store.query(
            group_ref=group_ref, address=address, source=source, limit=40
        )
        if not rows:
            yield event.plain_result(
                "暂无称呼记录。可用 /gs analyze <群ID> 触发分析，"
                "或 /gs alias <成员ID> <称呼> 人工补充。"
            )
            return
        lines = ["【称呼记录】（观察/推断均标注来源与置信度）"]
        for r in rows:
            frm = (
                f"{r['from_nick'] or r['from_user_id']} → "
                if r["from_user_id"]
                else "任意 → "
            )
            to = r["to_nick"] or r["to_user_id"] or "?"
            src = {"rule": "规则", "llm": "AI", "manual": "人工"}.get(
                r["source"], r["source"]
            )
            lines.append(
                f"{frm}{to}: 「{r['address']}」"
                f" [{src}] 置信 {r['confidence']:.2f} ×{r['seen_count']}"
                f" 最近 {_fmt_time(r['last_seen'])}"
            )
            if r["evidence_message"]:
                lines.append(f"  证据消息 #{r['evidence_message']}")
        yield event.plain_result("\n".join(lines))

    @gs.command("analyze")
    @filter.permission_type(ADMIN)
    async def gs_analyze(self, event: AstrMessageEvent):
        """立即入队分析：/gs analyze <群ID> [rule|llm|all]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        if self.runner is None:
            yield event.plain_result("分析循环未启动。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result(
                "用法: /gs analyze <群ID> [rule|llm|topic|event|all]"
                " [since=YYYY-MM-DD] [until=YYYY-MM-DD]"
            )
            return
        try:
            args, since, until = self._parse_range(args)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        if not args:
            yield event.plain_result("缺少群ID。")
            return
        row = await self._find_group(args[0])
        if not row:
            yield event.plain_result(f"未找到群: {args[0]}")
            return
        mode = args[1] if len(args) > 1 else "all"
        if mode not in ("rule", "llm", "all", "topic", "event"):
            yield event.plain_result("第二参数只能是 rule / llm / all / topic / event")
            return
        group_ref = int(row["id"])
        enqueued = []
        if mode in ("rule", "all"):
            if await self.runner.enqueue_rule(
                group_ref, since=since, until=until, priority=10
            ):
                enqueued.append("规则称呼")
        if mode in ("topic", "all"):
            enqueued += [
                _KIND_LABELS.get(k, k)
                for k in await self.runner.enqueue_memory(
                    group_ref,
                    kinds=(analysis_runner_mod.KIND_TOPIC_RULE,),
                    since=since,
                    until=until,
                    priority=10,
                )
            ]
            if self.gateway is not None:
                enqueued += [
                    _KIND_LABELS.get(k, k)
                    for k in await self.runner.enqueue_memory(
                        group_ref,
                        kinds=(analysis_runner_mod.KIND_TOPIC_LLM,),
                        since=since,
                        until=until,
                        priority=10,
                        with_llm=True,
                    )
                ]
        if mode in ("event", "all"):
            enqueued += [
                _KIND_LABELS.get(k, k)
                for k in await self.runner.enqueue_memory(
                    group_ref,
                    kinds=(analysis_runner_mod.KIND_EVENT_RULE,),
                    since=since,
                    until=until,
                    priority=10,
                )
            ]
            if self.gateway is not None:
                enqueued += [
                    _KIND_LABELS.get(k, k)
                    for k in await self.runner.enqueue_memory(
                        group_ref,
                        kinds=(analysis_runner_mod.KIND_EVENT_LLM,),
                        since=since,
                        until=until,
                        priority=10,
                        with_llm=True,
                    )
                ]
        if mode in ("llm", "all"):
            if self.gateway is None:
                enqueued.append("LLM(未启用/无Provider)")
            elif await self.runner.enqueue_llm(
                group_ref, since=since, until=until, priority=10
            ):
                enqueued.append("LLM称呼")
        if not enqueued:
            yield event.plain_result("该范围的分析任务已存在（或未入队）。")
            return
        rng = (
            f"\n范围: {_fmt_time(since)} → {_fmt_time(until)}"
            if (since or until)
            else "\n范围: 最近一个分析窗口"
        )
        yield event.plain_result(
            f"已入队分析任务：{'、'.join(enqueued)}（群 {row['group_name'] or row['group_id']}）"
            f"{rng}\n状态用 /gs tasks 查看。"
        )

    @gs.command("tasks")
    @filter.permission_type(ADMIN)
    async def gs_tasks(self, event: AstrMessageEvent):
        """分析任务状态：/gs tasks"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        if self.runner is None:
            yield event.plain_result("分析循环未启动。")
            return
        stats = await self.runner.queue.stats()
        g = self.gateway
        lines = [
            "【分析任务】",
            f"待处理 {stats['pending']} | 处理中 {stats['running']}"
            f" | 完成 {stats['done']} | 失败 {stats['failed']}"
            f" | 已取消 {stats['cancelled']}",
            f"规则任务 {self.runner.stats['rule_tasks']}"
            f" | LLM 任务 {self.runner.stats['llm_tasks']}"
            f" | 错误 {self.runner.stats['errors']}",
        ]
        if g is not None:
            remain = g.remaining_today
            lines.append(
                f"LLM: 调用 {g.stats['calls']} | 错误 {g.stats['errors']}"
                f" | 限流 {g.stats['throttled']} | 拒绝 {g.stats['rejected']}"
                f" | 今日剩余 {'不限' if remain < 0 else remain}"
            )
        else:
            lines.append("LLM: 未启用（仅规则层运行）")
        errors = await self.runner.queue.recent_errors(3)
        if errors:
            lines.append("最近错误:")
            lines += [
                f"- {e['kind']} 尝试{e['attempts']}: {(e['last_error'] or '')[:60]}"
                for e in errors
            ]
        yield event.plain_result("\n".join(lines))

    @gs.command("reanalyze")
    @filter.permission_type(ADMIN)
    async def gs_reanalyze(self, event: AstrMessageEvent):
        """按当前提示词版本重新分析：/gs reanalyze <群ID> [rule|llm|all]
        （先删除旧的 AI 结论并入队新任务，规则结论保留）"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        if self.runner is None:
            yield event.plain_result("分析循环未启动。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result(
                "用法: /gs reanalyze <群ID> [rule|llm|topic|event|all]"
                " [since=YYYY-MM-DD] [until=YYYY-MM-DD]"
            )
            return
        try:
            args, since, until = self._parse_range(args)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return
        if not args:
            yield event.plain_result("缺少群ID。")
            return
        row = await self._find_group(args[0])
        if not row:
            yield event.plain_result(f"未找到群: {args[0]}")
            return
        mode = args[1] if len(args) > 1 else "all"
        if mode not in ("rule", "llm", "all", "topic", "event"):
            yield event.plain_result("第二参数只能是 rule / llm / all / topic / event")
            return
        group_ref = int(row["id"])
        store = AddressingStore(self.db.conn)
        purged = 0
        purged_t = purged_e = 0
        if mode in ("llm", "all"):
            try:
                purged = await store.purge(group_ref=group_ref)
                await self.runner.queue.clear("addressing_rule")
                await self.runner.queue.clear("addressing_llm")
            except Exception as e:
                self.logger.exception("清理旧结论失败")
                yield event.plain_result(f"清理失败: {e}")
                return
        if mode in ("topic", "all", "event"):
            from .groupscope.memory.store import MemoryStore

            try:
                if mode in ("topic", "all"):
                    purged_t, _ = await MemoryStore(self.db.conn).purge(
                        group_ref=group_ref, source="llm"
                    )
                if mode in ("event", "all"):
                    _, purged_e = await MemoryStore(self.db.conn).purge(
                        group_ref=group_ref, source="llm"
                    )
                for k in (
                    analysis_runner_mod.KIND_TOPIC_RULE,
                    analysis_runner_mod.KIND_TOPIC_LLM,
                    analysis_runner_mod.KIND_EVENT_RULE,
                    analysis_runner_mod.KIND_EVENT_LLM,
                ):
                    await self.runner.queue.clear(k, status=("done", "failed", "pending"))
            except Exception as e:
                self.logger.exception("清理话题/事件失败")
                yield event.plain_result(f"清理失败: {e}")
                return
        enqueued = []
        if mode in ("rule", "all"):
            await self.runner.enqueue_rule(
                group_ref, since=since, until=until, priority=10
            )
            enqueued.append("规则称呼")
        if mode in ("topic", "all"):
            await self.runner.enqueue_memory(
                group_ref,
                kinds=(analysis_runner_mod.KIND_TOPIC_RULE,),
                since=since,
                until=until,
                priority=10,
            )
            enqueued.append("规则话题")
            if self.gateway is not None:
                await self.runner.enqueue_memory(
                    group_ref,
                    kinds=(analysis_runner_mod.KIND_TOPIC_LLM,),
                    since=since,
                    until=until,
                    priority=10,
                    with_llm=True,
                )
                enqueued.append("LLM话题")
        if mode in ("event", "all"):
            await self.runner.enqueue_memory(
                group_ref,
                kinds=(analysis_runner_mod.KIND_EVENT_RULE,),
                since=since,
                until=until,
                priority=10,
            )
            enqueued.append("规则事件")
            if self.gateway is not None:
                await self.runner.enqueue_memory(
                    group_ref,
                    kinds=(analysis_runner_mod.KIND_EVENT_LLM,),
                    since=since,
                    until=until,
                    priority=10,
                    with_llm=True,
                )
                enqueued.append("LLM事件")
        if mode in ("llm", "all"):
            if self.gateway is None:
                enqueued.append("LLM称呼(未启用)")
            else:
                await self.runner.enqueue_llm(group_ref, priority=10)
                enqueued.append("LLM称呼")
        yield event.plain_result(
            f"已清除旧结论：称呼 {purged}、话题 {purged_t}、事件 {purged_e} 条；"
            f"重新入队：{'、'.join(enqueued)}。"
            + (
                f"\n范围: {_fmt_time(since)} → {_fmt_time(until)}"
                if (since or until)
                else "\n范围: 最近一个分析窗口"
            )
            + f"\n提示词版本: {self._prompt_version()}"
        )

    @gs.command("alias")
    @filter.permission_type(ADMIN)
    async def gs_alias(self, event: AstrMessageEvent):
        """人工指定称呼：/gs alias <成员ID> <称呼> [群ID]
        （source='manual'，优先级最高，同时记入成员别名）"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if len(args) < 2 or not args[0].isdigit():
            yield event.plain_result(
                "用法: /gs alias <成员ID> <称呼> [群ID]\n"
                "成员ID 用 /gs member 查询；群ID 缺省时取最近活跃的群。"
            )
            return
        member_id = int(args[0])
        address = args[1]
        group_ref = None
        if len(args) > 2:
            row = await self._find_group(args[2])
            if not row:
                yield event.plain_result(f"未找到群: {args[2]}")
                return
            group_ref = int(row["id"])
        else:
            groups = await repo.list_groups(self.db.conn)
            if not groups:
                yield event.plain_result("尚无群数据。")
                return
            group_ref = int(groups[0]["id"])
        try:
            await AddressingStore(self.db.conn).add_manual(
                group_ref=group_ref,
                from_member=None,
                to_member=member_id,
                address=address,
                operator="gs-command",
            )
        except Exception as e:
            self.logger.exception("写入人工称呼失败")
            yield event.plain_result(f"写入失败: {e}")
            return
        yield event.plain_result(
            f"已记录人工称呼：成员 #{member_id} ← 「{address}」"
            f"\n（source=manual，之后的分析会优先参考）"
        )

    def _prompt_version(self) -> str:
        from .groupscope.analysis.prompts import PROMPTS

        return "/".join(spec["version"] for spec in PROMPTS.values())

    # ------------------------------------------------------------------
    # Phase 4：群体记忆
    # ------------------------------------------------------------------

    @gs.command("topics")
    @filter.permission_type(ADMIN)
    async def gs_topics(self, event: AstrMessageEvent):
        """话题列表：/gs topics [群ID] [条数]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        group_ref = None
        limit = 10
        for a in args:
            if a.isdigit():
                limit = min(int(a), 30)
            else:
                row = await self._find_group(a)
                if not row:
                    yield event.plain_result(f"未找到群: {a}")
                    return
                group_ref = row["id"]
        from .groupscope.memory.store import MemoryStore

        store = MemoryStore(self.db.conn)
        rows = await store.list_topics(group_ref, limit=limit)
        if not rows:
            yield event.plain_result(
                "暂无话题记录。可用 /gs analyze <群ID> topic 触发抽取。"
            )
            return
        lines = ["【群体话题】（关键词统计 + AI 归纳，均为推断）"]
        for r in rows:
            src = "规则" if r["source"] == "rule" else "AI"
            lines.append(
                f"#{r['id']} {r['title']} [{src}] 置信 {r['confidence']:.2f}"
                f" | 提及 {r['mention_count']} 次 | 证据 {r['evidence']} 条"
            )
            if r.get("summary"):
                lines.append(f"   {r['summary']}")
            lines.append(
                f"   首见 {_fmt_time(r['first_seen'])} → 最近 {_fmt_time(r['last_seen'])}"
                + (f" | 关键词: {'、'.join(r['keywords'][:6])}" if r.get("keywords") else "")
            )
        lines.append("用 /gs topic 1 <话题ID> 查看证据消息。")
        yield event.plain_result("\n".join(lines))

    @gs.command("topic")
    @filter.permission_type(ADMIN)
    async def gs_topic(self, event: AstrMessageEvent):
        """话题证据：/gs topic 1 <话题ID>"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if len(args) < 2 or not args[1].isdigit():
            yield event.plain_result("用法: /gs topic 1 <话题ID>（ID 从 /gs topics 获取）")
            return
        from .groupscope.memory.store import MemoryStore

        store = MemoryStore(self.db.conn)
        evidence = await store.topic_evidence(int(args[1]), limit=8)
        if not evidence:
            yield event.plain_result("该话题没有可回溯的证据消息。")
            return
        lines = [f"【话题 #{args[1]} 的证据消息】"]
        for m in evidence:
            text = (m["text"] or "").replace("\n", " ")[:50]
            lines.append(
                f"#{m['id']} {_fmt_time(m['sent_at'])}"
                f"[{m['sender_raw_name'] or m['sender_raw_id'] or '?'}] {text}"
            )
        lines.append("（证据可回溯到原始消息，推断不等于事实）")
        yield event.plain_result("\n".join(lines))

    @gs.command("events")
    @filter.permission_type(ADMIN)
    async def gs_events(self, event: AstrMessageEvent):
        """事件/梗列表：/gs events [群ID] [meme|event|dispute|routine]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        group_ref = None
        kind = None
        for a in args:
            if a in ("meme", "event", "dispute", "routine"):
                kind = a
            else:
                row = await self._find_group(a)
                if not row:
                    yield event.plain_result(f"未找到群: {a}")
                    return
                group_ref = row["id"]
        from .groupscope.memory.store import MemoryStore

        store = MemoryStore(self.db.conn)
        rows = await store.list_events(group_ref, event_kind=kind, limit=15)
        if not rows:
            yield event.plain_result(
                "暂无事件/梗记录。可用 /gs analyze <群ID> event 触发抽取。"
            )
            return
        kind_labels = {"meme": "梗", "event": "事件", "dispute": "争议", "routine": "日常"}
        lines = ["【群体事件 / 梗】（首现→传播→演变，均为推断）"]
        for r in rows:
            src = "规则" if r["source"] == "rule" else "AI"
            lines.append(
                f"#{r['id']} {r['title']} [{kind_labels.get(r['event_kind'], r['event_kind'])}]"
                f" [{src}] 置信 {r['confidence']:.2f}"
                f" | 出现 {r['mention_count']} 次 | 证据 {r['evidence']} 条"
            )
            if r.get("summary"):
                lines.append(f"   {r['summary']}")
            lines.append(
                f"   首现 {_fmt_time(r['first_seen'])} → 最近 {_fmt_time(r['last_seen'])}"
                + (f" | 峰值 {_fmt_time(r['peak_at'])}" if r.get("peak_at") else "")
            )
        lines.append("用 /gs event 1 <事件ID> 查看首现/传播证据。")
        yield event.plain_result("\n".join(lines))

    @gs.command("event")
    @filter.permission_type(ADMIN)
    async def gs_event(self, event: AstrMessageEvent):
        """事件证据：/gs event 1 <事件ID>"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if len(args) < 2 or not args[1].isdigit():
            yield event.plain_result("用法: /gs event 1 <事件ID>（ID 从 /gs events 获取）")
            return
        from .groupscope.memory.store import MemoryStore

        store = MemoryStore(self.db.conn)
        evidence = await store.event_evidence(int(args[1]), limit=8)
        if not evidence:
            yield event.plain_result("该事件没有可回溯的证据消息。")
            return
        role_labels = {"origin": "首现", "spread": "传播", "evolve": "演变"}
        lines = [f"【事件 #{args[1]} 的证据链】"]
        for m in evidence:
            text = (m["text"] or "").replace("\n", " ")[:50]
            lines.append(
                f"[{role_labels.get(m['role'], m['role'])}] #{m['id']}"
                f" {_fmt_time(m['sent_at'])}"
                f"[{m['sender_raw_name'] or m['sender_raw_id'] or '?'}] {text}"
            )
        lines.append("（证据可回溯到原始消息，推断不等于事实）")
        yield event.plain_result("\n".join(lines))

    @gs.command("timeline")
    @filter.permission_type(ADMIN)
    async def gs_timeline(self, event: AstrMessageEvent):
        """群体记忆时间线：/gs timeline [群ID] [条数]"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        group_ref = None
        limit = 20
        for a in args:
            if a.isdigit():
                limit = min(int(a), 50)
            else:
                row = await self._find_group(a)
                if not row:
                    yield event.plain_result(f"未找到群: {a}")
                    return
                group_ref = row["id"]
        from .groupscope.memory.store import MemoryStore

        store = MemoryStore(self.db.conn)
        rows = await store.timeline(group_ref, limit=limit)
        if not rows:
            yield event.plain_result("暂无时间线（需要先产生话题/事件）。")
            return
        lines = ["【群体记忆时间线】（新→旧）"]
        for r in rows:
            mark = "📌" if r["item_kind"] == "topic" else "🔥"
            lines.append(
                f"{_fmt_time(r['occurred_at'])} {mark} {r['label']}"
                + (f" — {r['detail']}" if r.get("detail") else "")
            )
        yield event.plain_result("\n".join(lines))

    # ---- Phase 6：归档 / 恢复 / 校验 -------------------------------

    @property
    def _archives_dir(self) -> Path:
        return self.data_dir / "archives"

    @gs.command("archive")
    @filter.permission_type(ADMIN)
    async def gs_archive(self, event: AstrMessageEvent):
        """归档某月原始消息：/gs archive <YYYY-MM> [purge]
        不带 purge 只生成归档文件（主库不动）；带 purge 才从主库删除该月消息
        （分析结论保留，证据改指归档库）。"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result(
                "用法: /gs archive <YYYY-MM> [purge]\n"
                "例: /gs archive 2026-01（仅生成归档文件）\n"
                "    /gs archive 2026-01 purge（归档并从主库移除）"
            )
            return
        month = args[0]
        do_purge = len(args) > 1 and args[1] == "purge"
        try:
            if do_purge:
                info = await archives_ops.purge_month(
                    self.db.conn, month=month, archives_dir=self._archives_dir
                )
            else:
                info = await archives_ops.create_archive(
                    self.db.conn, month=month, archives_dir=self._archives_dir
                )
        except ValueError as e:
            yield event.plain_result(str(e))
            return
        except Exception as e:
            self.logger.exception("归档失败: %s", month)
            yield event.plain_result(f"归档失败: {e}")
            return
        lines = [
            f"【归档 {month}】 → {info['file']}",
            f"消息 {info['messages']} 条 / 段 {info['segments']} / @ {info['mentions']}",
            f"sha256: {info['sha256'][:16]}…",
        ]
        if do_purge:
            lines.append(
                f"已从主库移除 {info.get('deleted', 0)} 条消息；"
                "分析结论保留，证据改指归档库（/gs verify 校验）"
            )
        else:
            lines.append("主库未改动。确认无误后执行: "
                         f"/gs archive {month} purge")
        yield event.plain_result("\n".join(lines))

    @gs.command("archives")
    @filter.permission_type(ADMIN)
    async def gs_archives(self, event: AstrMessageEvent):
        """列出归档清单"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        rows = await archives_ops.list_manifests(self.db.conn)
        if not rows:
            yield event.plain_result(
                "暂无归档。使用 /gs archive <YYYY-MM> 创建（仅当前月份之前的数据）。"
            )
            return
        lines = ["【归档清单】（新→旧）"]
        for r in rows[:12]:
            state = "已purge" if r["purged"] else "仅归档"
            restored = "，已恢复" if r.get("restored_at") else ""
            lines.append(
                f"- {r['month']} {r['file']}：{r['messages']} 条"
                f"（{state}{restored}）{_fmt_time(r['created_at'])}"
            )
        lines.append("校验: /gs verify [月份] ｜ 恢复: /gs unarchive <YYYY-MM>")
        yield event.plain_result("\n".join(lines))

    @gs.command("unarchive")
    @filter.permission_type(ADMIN)
    async def gs_unarchive(self, event: AstrMessageEvent):
        """从归档恢复某月消息：/gs unarchive <YYYY-MM>
        按 dedup_key 去重回灌，可断点续跑；恢复后自动回链证据。"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        args = self._args(event)
        if not args:
            yield event.plain_result("用法: /gs unarchive <YYYY-MM>（先 /gs archives 查看）")
            return
        try:
            info = await archives_ops.restore_month(
                self.db.conn, month=args[0], archives_dir=self._archives_dir
            )
        except ValueError as e:
            yield event.plain_result(str(e))
            return
        except Exception as e:
            self.logger.exception("恢复归档失败: %s", args[0])
            yield event.plain_result(f"恢复失败: {e}")
            return
        yield event.plain_result(
            f"【恢复 {info['month']}】新增 {info['restored']} 条"
            f"（跳过已存在 {info['skipped']} 条）\n"
            f"消息段 {info['segments']} / @ {info['mentions']}"
            f" / 证据回链 {info['evidence_relinked']} 条"
        )

    @gs.command("verify")
    @filter.permission_type(ADMIN)
    async def gs_verify(self, event: AstrMessageEvent):
        """数据完整性校验：/gs verify [YYYY-MM]
        quick_check + 归档 sha256/行数对账 + SQLite 维护。"""
        if not await self._ready():
            yield event.plain_result("GroupScope 未就绪，详见日志。")
            return
        lines: list[str] = []
        try:
            ok = await self.db.quick_check()
            lines.append(f"主库 quick_check: {ok}")
        except Exception as e:
            yield event.plain_result(f"主库完整性检查失败: {e}")
            return
        args = self._args(event)
        manifests = await archives_ops.list_manifests(self.db.conn)
        targets = [m for m in manifests if not args or m["month"] == args[0]]
        if args and not targets:
            yield event.plain_result(
                f"未找到 {args[0]} 的归档记录。" + "\n".join(lines)
            )
            return
        bad = 0
        for m in targets:
            try:
                r = await archives_ops.verify_manifest(
                    self.db.conn, month=m["month"], archives_dir=self._archives_dir
                )
            except Exception as e:
                lines.append(f"- {m['month']}: 校验异常 {e}")
                bad += 1
                continue
            if r["ok"]:
                lines.append(
                    f"- {m['month']}: OK（{r['messages']} 条，sha256 匹配，quick_check ok）"
                )
            else:
                bad += 1
                lines.append(
                    f"- {m['month']}: 异常 {r.get('error') or r}"
                )
        await self.db.maintain()
        size = await self.db.file_size()
        lines.append(f"已执行维护（PRAGMA optimize + wal_checkpoint），库 {size / 1024 / 1024:.2f} MB")
        if bad:
            lines.append(f"⚠️ {bad} 个归档异常，请检查文件是否被移动/损坏。")
        yield event.plain_result("\n".join(lines))

    @gs.command("reload")
    @filter.permission_type(ADMIN)
    async def gs_reload(self, event: AstrMessageEvent):
        """重新读取配置（白名单/黑名单/开关等，无需重载插件）"""
        self.settings = CaptureSettings.from_conf(self.conf)
        self.analysis_settings = AnalysisSettings.from_conf(self.conf)
        paused = set(self.policy.paused)
        self.policy = CapturePolicy(self.settings)
        self.policy.paused = paused
        if self.service is not None:
            self.service.settings = self.settings
            self.service.policy = self.policy
        if self.listener is not None:
            self.listener.policy = self.policy
        yield event.plain_result(
            f"配置已重新加载：总开关={'开' if self.settings.capture_enabled else '关'}，"
            f"白名单 {len(self.settings.group_whitelist)} 个群，"
            f"AI 分析={'开' if self.analysis_settings.analysis_enabled else '关'}"
            "\n（LLM 限流参数在下次插件重载后生效）"
        )
