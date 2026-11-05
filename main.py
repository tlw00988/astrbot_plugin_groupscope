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
from .groupscope.capture.listener import MessageListener
from .groupscope.capture.service import CaptureService
from .groupscope.config import CaptureSettings
from .groupscope.ops import backup as backup_ops
from .groupscope.ops import export as export_ops
from .groupscope.paths import plugin_data_dir
from .groupscope.privacy import CapturePolicy
from .groupscope.social import profiles as social_profiles
from .groupscope.social import snapshots as social_snapshots
from .groupscope.storage import repo
from .groupscope.storage.db import Database

ADMIN = filter.PermissionType.ADMIN


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
        self.policy = CapturePolicy(self.settings)
        self.db = Database(self.data_dir / "groupscope.db")
        self.service: CaptureService | None = None
        self.listener: MessageListener | None = None
        self._start_lock = asyncio.Lock()
        self._started = False
        self._rejected = 0
        self._init_errors = 0

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
            self._started = True
            self.logger.info(
                "GroupScope 已就绪：数据库 %s，白名单 %s 个群",
                self.db.path,
                len(self.settings.group_whitelist),
            )

    def _on_rejected(self) -> None:
        self._rejected += 1

    async def terminate(self) -> None:
        """插件卸载/停用：排空队列、停维护任务、关闭数据库。"""
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
        lines = [
            "【GroupScope 状态】",
            f"总开关: {'开' if self.settings.capture_enabled else '关'}"
            f" | 白名单群: {len(self.settings.group_whitelist)}"
            f" | 已暂停群: {paused}",
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

    @gs.command("reload")
    @filter.permission_type(ADMIN)
    async def gs_reload(self, event: AstrMessageEvent):
        """重新读取配置（白名单/黑名单/开关等，无需重载插件）"""
        self.settings = CaptureSettings.from_conf(self.conf)
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
            f"白名单 {len(self.settings.group_whitelist)} 个群"
        )
