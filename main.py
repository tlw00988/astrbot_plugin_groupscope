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
