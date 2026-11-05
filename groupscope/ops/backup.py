"""备份与恢复。

备份：``VACUUM INTO`` 在线生成一致性副本（不阻塞读写）。
恢复：校验 → 归档当前库 → 复制备份为主库（由调用方负责停写/重开）。
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from ..storage.db import Database, verify_backup


async def create_backup(db: Database, dest_dir: Path, keep: int = 7) -> Path:
    return await db.backup(dest_dir, keep)


async def restore_backup(db: Database, backup_path: Path) -> Path:
    """把 ``backup_path`` 恢复为主数据库。

    调用前提：写入服务已停止、``db`` 已 ``close()``。
    返回被归档的旧库路径（若存在）。
    """
    backup_path = Path(backup_path)
    await verify_backup(backup_path)

    stamp = time.strftime("%Y%m%d-%H%M%S")
    archived: Path | None = None
    if db.path.exists():
        archived = db.path.with_name(f"{db.path.name}.pre-restore-{stamp}")
        db.path.replace(archived)
    for suffix in ("-wal", "-shm"):
        leftover = db.path.with_suffix(db.path.suffix + suffix)
        if leftover.exists():
            leftover.unlink()
    shutil.copy2(backup_path, db.path)
    return archived  # type: ignore[return-value]


def list_backups(dest_dir: Path) -> list[Path]:
    dest_dir = Path(dest_dir)
    if not dest_dir.is_dir():
        return []
    return sorted(dest_dir.glob("groupscope-*.db"), reverse=True)
