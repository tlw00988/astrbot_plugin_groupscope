"""数据库迁移：版本链 + 内嵌 SQL 脚本，启动时按序升级。

原则：只增不改；破坏性重构必须通过新增迁移版本实现，
并且每一步都可在备份之上重放验证。
"""

from __future__ import annotations

import time
from pathlib import Path

import aiosqlite

_SCHEMA_DIR = Path(__file__).parent


def _load(version: int) -> str:
    return (_SCHEMA_DIR / f"schema_v{version}.sql").read_text(encoding="utf-8")


MIGRATIONS: list[tuple[int, str]] = [
    (1, _load(1)),
    (2, _load(2)),  # Phase 2：人物与关系
    (3, _load(3)),  # Phase 3：称呼与语义分析
    (4, _load(4)),  # Phase 4：群体记忆
]

LATEST_VERSION = max(version for version, _ in MIGRATIONS)


async def current_version(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    )
    if await cur.fetchone() is None:
        return 0
    cur = await conn.execute("SELECT MAX(version) FROM schema_migrations")
    row = await cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


async def run_migrations(conn: aiosqlite.Connection) -> int:
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version INTEGER PRIMARY KEY,"
        " applied_at INTEGER NOT NULL,"
        " note TEXT)"
    )
    applied = await current_version(conn)
    for version, sql in MIGRATIONS:
        if version <= applied:
            continue
        await conn.executescript(sql)
        await conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at, note) "
            "VALUES(?,?,?)",
            (version, int(time.time()), f"migration v{version}"),
        )
        applied = version
    return applied
