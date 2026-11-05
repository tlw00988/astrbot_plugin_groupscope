"""存储层：迁移幂等、批量写入去重、查询、保留期清理、备份恢复、导出。"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path

from conftest import make_event, make_message, run
from groupscope.capture.serializer import normalize_event
from groupscope.ops import backup as backup_ops
from groupscope.ops import export as export_ops
from groupscope.storage import migrations, repo
from groupscope.storage.db import Database, verify_backup


async def add(db: Database, **kwargs) -> int:
    """规范化一条合成消息并写入，返回新增条数。"""
    item = normalize_event(make_event(make_message(**kwargs)))
    return await repo.insert_batch(db.conn, [item])


# ---------------------------------------------------------------------
# 迁移
# ---------------------------------------------------------------------


def test_migrations_idempotent(db: Database):
    assert run(migrations.current_version(db.conn)) == migrations.LATEST_VERSION

    async def reopen():
        await db.close()
        await db.open()
        return await migrations.current_version(db.conn)

    assert run(reopen()) == migrations.LATEST_VERSION
    assert run(db.quick_check()) == "ok"


# ---------------------------------------------------------------------
# 写入
# ---------------------------------------------------------------------


def test_insert_dedup_and_counts(db: Database):
    assert run(add(db, message_id="1", text="a")) == 1
    assert run(add(db, message_id="1", text="a")) == 0  # 同 message_id 去重
    assert run(add(db, message_id="2", text="b")) == 1
    assert run(repo.count_messages(db.conn)) == 2
    assert run(repo.count_members(db.conn)) == 1
    assert run(repo.count_notices(db.conn)) == 0

    cur_row = run(_fetch_one(db, "SELECT message_count, current_nickname FROM members"))
    assert cur_row["message_count"] == 2
    assert cur_row["current_nickname"] == "Alice"

    group = run(_fetch_one(db, "SELECT message_count, first_seen, last_seen FROM member_group"))
    assert group["message_count"] == 2


def _fetch_one(db: Database, sql: str, params: tuple = ()):
    async def _go():
        cur = await db.conn.execute(sql, params)
        return await cur.fetchone()

    return _go()


def test_alias_history_recorded(db: Database):
    run(add(db, message_id="1", nickname="Alice"))
    run(add(db, message_id="2", nickname="Alice2"))
    run(add(db, message_id="3", nickname="Alice2"))
    got = run(_fetch_all(db, "SELECT alias, seen_count FROM member_aliases ORDER BY alias"))
    assert [(r["alias"], r["seen_count"]) for r in got] == [
        ("Alice", 1),
        ("Alice2", 2),
    ]


def _fetch_all(db: Database, sql: str, params: tuple = ()):
    async def _go():
        cur = await db.conn.execute(sql, params)
        return await cur.fetchall()

    return _go()


def test_notice_does_not_count_as_speech(db: Database):
    run(add(db, message_id="1", text="正常发言"))
    notice = {
        "post_type": "notice",
        "notice_type": "group_recall",
        "group_id": 900001,
        "time": 1700000000,
    }
    run(
        add(
            db,
            message_id="",
            text="",
            segments=[],
            notice_raw=notice,
        )
    )
    assert run(repo.count_messages(db.conn)) == 2
    assert run(repo.count_notices(db.conn)) == 1
    row = run(_fetch_one(db, "SELECT message_count FROM members"))
    assert row["message_count"] == 1  # 通知不计入发言数


def test_segments_and_mentions_persist(db: Database):
    from astrbot.core.message.components import At, Plain, Reply

    msg = make_message(
        message_id="7",
        segments=[Plain(text="hi "), At(qq=1001), Reply(id=123)],
        text="hi ",
    )
    run(add(db, message_id="7", segments=msg.message, text="hi "))
    # 先让 @ 目标成为成员
    run(add(db, message_id="8", user_id="1001", nickname="Bob", text="我是被@的"))

    row = run(_fetch_one(db, "SELECT id FROM messages WHERE message_id='7'"))
    segs = run(_fetch_all(db, "SELECT * FROM message_segments WHERE message_ref=? ORDER BY seg_index", (row["id"],)))
    assert [s["seg_type"] for s in segs] == ["Plain", "At", "Reply"]
    assert json.loads(segs[1]["payload_json"])["qq"] == 1001

    mentions = run(
        _fetch_all(db, "SELECT * FROM message_mentions WHERE message_ref=?", (row["id"],))
    )
    assert mentions[0]["kind"] == "at"
    assert mentions[0]["target_raw_id"] == "1001"
    assert mentions[0]["target_ref"] is not None  # 关联到 members

    ctx = run(repo.message_context(db.conn, row["id"], before=2, after=2))
    assert [c["message_id"] for c in ctx] == ["7", "8"]


# ---------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------


def test_queries_recent_search_group(db: Database):
    now = int(time.time())
    for i in range(5):
        run(add(db, message_id=str(i), text=f"消息{i} 关键词X" if i == 2 else f"消息{i}", timestamp=now - i))

    groups = run(repo.list_groups(db.conn))
    assert len(groups) == 1
    assert groups[0]["group_id"] == "900001"
    assert groups[0]["group_name"] == "测试群"

    found = run(repo.find_group(db.conn, "900001"))
    assert found and found["id"] == groups[0]["id"]
    assert run(repo.find_group(db.conn, "aiocqhttp:900001"))["id"] == groups[0]["id"]
    assert run(repo.find_group(db.conn, "nope")) is None

    recent = run(repo.recent_messages(db.conn, None, 3))
    assert [r["message_id"] for r in recent] == ["4", "3", "2"]

    hits = run(repo.search_messages(db.conn, "关键词X", None))
    assert len(hits) == 1 and hits[0]["message_id"] == "2"

    daily = run(repo.daily_counts(db.conn, days=1))
    assert sum(d["count"] for d in daily) == 5

    scoped = run(repo.recent_messages(db.conn, groups[0]["id"], 10))
    assert len(scoped) == 5


def test_pause_resume_persisted(db: Database):
    run(add(db, message_id="1"))
    groups = run(repo.list_groups(db.conn))
    ref = groups[0]["id"]
    run(repo.set_group_capture(db.conn, ref, False))
    assert run(repo.paused_groups(db.conn)) == [
        (groups[0]["platform_id"], groups[0]["group_id"])
    ]
    run(repo.set_group_capture(db.conn, ref, True))
    assert run(repo.paused_groups(db.conn)) == []


def test_retention_purge(db: Database):
    run(add(db, message_id="old", text="旧消息"))
    # 把这条消息的时间改到 100 天前
    old = int(time.time()) - 100 * 86400

    async def backdate():
        await db.conn.execute("UPDATE messages SET received_at=?", (old,))
        await db.conn.execute("UPDATE groups SET last_seen=?", (old,))

    run(backdate())
    run(add(db, message_id="new", text="新消息"))

    deleted = run(repo.retention_purge(db.conn, 30))
    assert deleted == 1
    assert run(repo.count_messages(db.conn)) == 1
    # 级联：被删消息的段与 @ 记录一并删除，新消息的段保留
    left = run(_fetch_all(db, "SELECT m.message_id AS mid FROM message_segments s"
                              " JOIN messages m ON m.id = s.message_ref"))
    assert [r["mid"] for r in left] == ["new"]
    assert run(repo.retention_purge(db.conn, 0)) == 0  # 0 表示不清理


# ---------------------------------------------------------------------
# 备份 / 恢复 / 导出
# ---------------------------------------------------------------------


def test_backup_verify_and_restore(db: Database, tmp_path: Path):
    run(add(db, message_id="1", text="备份前"))
    run(add(db, message_id="2", text="备份前2"))

    dest = run(db.backup(tmp_path / "backups", keep=2))
    assert dest.is_file()
    run(verify_backup(dest))

    # 备份后继续写入
    run(add(db, message_id="3", text="备份后"))
    assert run(repo.count_messages(db.conn)) == 3
    run(db.close())

    # 恢复：以备份覆盖主库
    restored = Database(db.path)
    archived = run(backup_ops.restore_backup(restored, dest))
    assert archived is not None and archived.is_file()
    run(restored.open())
    assert run(repo.count_messages(restored.conn)) == 2
    run(restored.close())

    # 恢复前的库被归档保留
    assert Path(archived).is_file()

    # 备份轮转：keep=2 时最多保留 2 个
    run(db.open())
    run(db.backup(tmp_path / "backups", keep=2))
    run(db.backup(tmp_path / "backups", keep=2))
    assert len(backup_ops.list_backups(tmp_path / "backups")) <= 2


def test_export_json_and_csv(db: Database, tmp_path: Path):
    from astrbot.core.message.components import At, Plain

    run(
        add(
            db,
            message_id="1",
            segments=[Plain(text="你好"), At(qq=1001)],
            text="你好",
        )
    )
    run(add(db, message_id="2", text="第二条"))

    json_path = run(export_ops.export_messages(db, tmp_path / "exports", "json"))
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(data) == 2
    assert data[0]["segments"][0]["type"] == "Plain"
    assert data[0]["text"] == "你好"
    assert data[0]["group_raw_id"] == "900001"

    csv_path = run(export_ops.export_messages(db, tmp_path / "exports", "csv"))
    with csv_path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 2
    assert rows[0]["message_id"] == "1"
    assert json.loads(rows[0]["segments"])[0]["type"] == "Plain"
