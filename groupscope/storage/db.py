"""SQLite 连接管理：PRAGMA、迁移、事务、备份、一致性检查。"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import aiosqlite

from .migrations import run_migrations


class DatabaseError(RuntimeError):
    pass


class Database:
    """单连接（WAL）SQLite 封装。

    采用 ``isolation_level=None``（自动提交关闭），写入方通过显式
    ``BEGIN IMMEDIATE``/``COMMIT``/``ROLLBACK`` 控制事务，读取方在 WAL
    模式下不阻塞。aiosqlite 的连接内部是单线程队列，天然串行化，避免
    跨任务并发写冲突。
    """

    def __init__(self, db_path: Path) -> None:
        self.path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise DatabaseError("数据库尚未打开")
        return self._conn

    @property
    def opened(self) -> bool:
        return self._conn is not None

    async def open(self) -> None:
        if self._conn is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(
            self.path,
            timeout=5.0,
            isolation_level=None,  # 手动管理事务
        )
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("PRAGMA temp_store=MEMORY")
        self._conn = conn
        try:
            await run_migrations(conn)
            await self.quick_check()
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        worker = getattr(conn, "_thread", None)
        if worker is not None and not worker.is_alive():
            # aiosqlite 工作线程已死（异常路径），任何 await 都不会返回，
            # 这里只能放弃优雅关闭，交由 GC 处理，绝不让停机流程挂死。
            return
        try:
            await asyncio.wait_for(
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"), 5.0
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(conn.close(), 5.0)
        except Exception:
            pass

    async def quick_check(self) -> str:
        cur = await self.conn.execute("PRAGMA quick_check(1)")
        row = await cur.fetchone()
        result = str(row[0]) if row else "unknown"
        if result != "ok":
            raise DatabaseError(f"SQLite 完整性检查失败: {result}")
        return result

    async def file_size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    # ---- 状态键值 -------------------------------------------------

    async def set_state(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO plugin_state(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
            "updated_at=excluded.updated_at",
            (key, value, int(time.time())),
        )

    async def get_state(self, key: str, default: str | None = None) -> str | None:
        cur = await self.conn.execute(
            "SELECT value FROM plugin_state WHERE key=?", (key,)
        )
        row = await cur.fetchone()
        return row["value"] if row else default

    # ---- 备份 -----------------------------------------------------

    async def backup(self, dest_dir: Path, keep: int = 7) -> Path:
        """``VACUUM INTO`` 生成一致性在线备份，并按 keep 轮转。"""
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        dest = dest_dir / f"groupscope-{stamp}.db"
        counter = 0
        while dest.exists():  # 同秒多次备份
            counter += 1
            dest = dest_dir / f"groupscope-{stamp}-{counter}.db"
        try:
            await self.conn.execute("VACUUM INTO ?", (str(dest),))
        except aiosqlite.OperationalError:
            # 某些 SQLite 构建不允许绑定参数，转义后内联
            escaped = str(dest).replace("'", "''")
            await self.conn.execute(f"VACUUM INTO '{escaped}'")
        await _prune_backups(dest_dir, keep)
        return dest


async def _prune_backups(dest_dir: Path, keep: int) -> None:
    backups = sorted(dest_dir.glob("groupscope-*.db"))
    excess = len(backups) - max(1, keep)
    for stale in backups[:max(0, excess)]:
        try:
            stale.unlink()
        except OSError:
            pass


async def verify_backup(path: Path) -> None:
    """恢复前校验备份文件完整性（独立只读连接）。"""
    if not Path(path).is_file():
        raise DatabaseError(f"备份不存在: {path}")
    conn = await aiosqlite.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        cur = await conn.execute("PRAGMA quick_check(1)")
        row = await cur.fetchone()
        result = str(row[0]) if row else "unknown"
        if result != "ok":
            raise DatabaseError(f"备份完整性检查失败: {result}")
        cur = await conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name='schema_migrations'"
        )
        row = await cur.fetchone()
        if not row or int(row[0]) != 1:
            raise DatabaseError("备份缺少 schema_migrations，不是 GroupScope 数据库")
    finally:
        await conn.close()
