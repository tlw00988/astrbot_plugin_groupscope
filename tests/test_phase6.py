"""Phase 6：月度归档/恢复/校验、任务退避与滞留回收、周期维护。"""

from __future__ import annotations

import time
from pathlib import Path

from conftest import run
from groupscope.analysis.tasks import (
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    TaskQueue,
)
from groupscope.ops import archives as archive_ops
from groupscope.storage.db import Database

# 2026-01 / 2026-02 的本地时区时间戳
JAN = 1_767_283_200  # 2026-01-02
FEB = 1_769_961_600  # 2026-02-02


async def _add(db: Database, *, ts: int, i: int) -> int:
    """直接写一条消息（不依赖事件构造），返回内部 id。"""
    dedup = f"p6-{ts}-{i}"
    cur = await db.conn.execute(
        "INSERT INTO messages(platform_id,platform_name,group_ref,group_raw_id,"
        "message_id,dedup_key,sender_ref,sender_raw_id,sender_raw_name,event_kind,"
        "sent_at,received_at,message_type,text,inserted_at)"
        " VALUES('aiocqhttp','QQ',1,'900001',?,?,1,'1001','Alice','message',"
        "?,?,'text',?,?)",
        (dedup, dedup, ts, ts, f"消息{i}", ts),
    )
    await cur.fetchall()  # 让游标执行完
    cur2 = await db.conn.execute("SELECT last_insert_rowid()")
    msg_id = int((await cur2.fetchone())[0])
    await db.conn.execute(
        "INSERT INTO message_segments(message_ref,seg_index,seg_type,payload_json,text)"
        " VALUES(?,0,'text','{}',?)",
        (msg_id, f"消息{i}"),
    )
    await db.conn.commit()
    return msg_id


async def _seed_group_member(db: Database) -> None:
    await db.conn.execute(
        "INSERT INTO groups(platform_id,platform_name,group_id,group_name,"
        " capture_enabled,first_seen,last_seen) VALUES('aiocqhttp','QQ','900001',"
        "'测试群',1,?,?)",
        (JAN, FEB),
    )
    await db.conn.execute(
        "INSERT INTO members(platform_id,user_id,current_nickname,first_seen,"
        "last_seen,message_count) VALUES('aiocqhttp','1001','Alice',?,?,10)",
        (JAN, FEB),
    )
    await db.conn.commit()


async def _count(db: Database, sql: str, *p) -> int:
    cur = await db.conn.execute(sql, p)
    return int((await cur.fetchone())[0])


def test_validate_month_rejects_bad_input():
    assert archive_ops.validate_month("2026-01")[0] < archive_ops.validate_month("2026-01")[1]
    for bad in ("2026-1", "2026-13", "01-2026", "abc", ""):
        try:
            archive_ops.validate_month(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad} 应被拒绝")


def test_create_purge_restore_roundtrip(db: Database, tmp_path: Path):
    arch_dir = tmp_path / "archives"

    async def flow():
        await _seed_group_member(db)
        jan_ids = [await _add(db, ts=JAN + i, i=i) for i in range(5)]
        await _add(db, ts=FEB, i=99)  # 2 月消息应保留
        # 分析结论 + 证据（指向 1 月消息）
        now = JAN
        await db.conn.execute(
            "INSERT INTO topics(group_ref,title,summary,keywords_json,first_seen,"
            "last_seen,mention_count,source,confidence,created_at)"
            " VALUES(1,'话题A','s','[]',?,?,2,'rule',0.5,?)",
            (now, now, now),
        )
        await db.conn.execute(
            "INSERT INTO events(group_ref,title,summary,event_kind,first_seen,"
            "last_seen,mention_count,source,confidence,created_at)"
            " VALUES(1,'梗A','s','meme',?,?,2,'rule',0.5,?)",
            (now, now, now),
        )
        await db.conn.execute(
            "INSERT INTO addressing(group_ref,from_member,to_member,address,source,"
            "confidence,created_at,last_seen,seen_count,reviewed)"
            " VALUES(1,1,1,'阿A','rule',0.8,?,?,2,1)",
            (now, now),
        )
        await db.conn.execute(
            "INSERT INTO topic_mentions(topic_id,message_ref,weight,created_at)"
            " VALUES(1,?,1,?)",
            (jan_ids[0], now),
        )
        await db.conn.execute(
            "INSERT INTO event_evidence(event_id,message_ref,role,note,created_at)"
            " VALUES(1,?, 'origin','首现',?)",
            (jan_ids[1], now),
        )
        await db.conn.execute(
            "UPDATE addressing SET evidence_message=? WHERE id=1", (jan_ids[2],)
        )
        await db.conn.commit()

        # 1) 创建归档（不动主库）
        info = await archive_ops.create_archive(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert info["messages"] == 5 and info["segments"] == 5 and not info["purged"]
        assert (arch_dir / info["file"]).is_file()
        assert await _count(db, "SELECT COUNT(*) FROM messages") == 6

        # 重复创建应报错（幂等保护）
        try:
            await archive_ops.create_archive(db.conn, month="2026-01", archives_dir=arch_dir)
        except Exception:
            pass
        else:
            raise AssertionError("重复归档应报错")

        # 2) 校验
        v = await archive_ops.verify_manifest(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert v["ok"] and v["sha256_match"] and v["quick_check"] == "ok", v

        # 3) purge：证据搬家、消息删除、分析结论保留
        p = await archive_ops.purge_month(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert p["purged"] and p["deleted"] == 5
        assert await _count(db, "SELECT COUNT(*) FROM messages") == 1
        # 1 月的段级联删除，2 月那条的段保留
        assert await _count(db, "SELECT COUNT(*) FROM message_segments") == 1
        assert await _count(db, "SELECT COUNT(*) FROM archived_evidence") == 3
        assert await _count(db, "SELECT COUNT(*) FROM topics") == 1
        assert await _count(db, "SELECT COUNT(*) FROM events") == 1
        assert await _count(db, "SELECT COUNT(*) FROM addressing") == 1
        assert await _count(db, "SELECT COUNT(*) FROM topic_mentions") == 0
        assert await _count(db, "SELECT COUNT(*) FROM event_evidence") == 0
        assert await _count(
            db, "SELECT COUNT(*) FROM addressing WHERE evidence_message IS NULL"
        ) == 1

        # 幂等：再 purge 不炸、不重复搬证据
        p2 = await archive_ops.purge_month(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert p2["deleted"] == 0
        assert await _count(db, "SELECT COUNT(*) FROM archived_evidence") == 3

        # 4) 恢复：回灌 + 证据回链
        r = await archive_ops.restore_month(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert r["restored"] == 5 and r["skipped"] == 0
        assert r["segments"] == 5 and r["evidence_relinked"] == 3, r
        assert await _count(db, "SELECT COUNT(*) FROM messages") == 6
        assert await _count(db, "SELECT COUNT(*) FROM topic_mentions") == 1
        assert await _count(db, "SELECT COUNT(*) FROM event_evidence") == 1
        assert await _count(
            db, "SELECT COUNT(*) FROM addressing WHERE evidence_message IS NOT NULL"
        ) == 1

        # 断点续跑：再恢复全部跳过且不重复
        r2 = await archive_ops.restore_month(
            db.conn, month="2026-01", archives_dir=arch_dir
        )
        assert r2["restored"] == 0 and r2["skipped"] == 5
        assert await _count(db, "SELECT COUNT(*) FROM messages") == 6

        # 5) 篡改归档文件 → 拒绝恢复
        path = arch_dir / info["file"]
        with path.open("r+b") as f:
            f.seek(200)
            f.write(b"Z" * 8)
        try:
            await archive_ops.restore_month(
                db.conn, month="2026-01", archives_dir=arch_dir
            )
        except Exception:
            pass
        else:
            raise AssertionError("损坏归档应被拒绝恢复")

        # 6) 清单
        lst = await archive_ops.list_manifests(db.conn)
        assert [m["month"] for m in lst] == ["2026-01"]

    try:
        run(flow())
    finally:
        run(db.close()) if db.opened else None


def test_task_backoff_and_stale_requeue(db: Database):
    async def flow():
        q = TaskQueue(db.conn)
        assert await q.enqueue("rule", "g1:w1", payload={"group_ref": 1})
        # 重复入队被去重
        assert not await q.enqueue("rule", "g1:w1", payload={"group_ref": 1})

        t1 = await q.claim_kinds(("rule",), 1)
        assert len(t1) == 1 and t1[0]["status"] == STATUS_RUNNING
        # 同类型没有第二条可认领
        assert await q.claim_kinds(("rule",), 1) == []

        # 失败 1：退回 pending，带 30s 退避，且此时认领不到
        await q.fail(t1[0], "boom-1")
        cur = await db.conn.execute(
            "SELECT status, not_before FROM analysis_tasks WHERE id=?", (t1[0]["id"],)
        )
        status, not_before = await cur.fetchone()
        assert status == STATUS_PENDING and not_before and not_before > time.time()
        assert await q.claim_kinds(("rule",), 1) == []

        # 模拟退避到期后可再次认领
        await db.conn.execute(
            "UPDATE analysis_tasks SET not_before=? WHERE id=?",
            (int(time.time()) - 1, t1[0]["id"]),
        )
        t2 = await q.claim_kinds(("rule",), 1)
        assert t2 and t2[0]["id"] == t1[0]["id"]
        await q.fail(t2[0], "boom-2")
        cur = await db.conn.execute(
            "SELECT not_before FROM analysis_tasks WHERE id=?", (t1[0]["id"],)
        )
        nb2 = (await cur.fetchone())[0]
        assert 55 <= nb2 - int(time.time()) <= 65  # 指数退避到 60s

        # 用尽重试 → failed
        await db.conn.execute(
            "UPDATE analysis_tasks SET not_before=NULL, attempts=2 WHERE id=?",
            (t1[0]["id"],),
        )
        t3 = await q.claim_kinds(("rule",), 1)
        await q.fail(t3[0], "boom-3")
        cur = await db.conn.execute(
            "SELECT status FROM analysis_tasks WHERE id=?", (t1[0]["id"],)
        )
        assert (await cur.fetchone())[0] == STATUS_FAILED

        # 滞留 running 被回收
        assert await q.enqueue("rule", "g1:stale", payload={"group_ref": 1})
        ts = await q.claim_kinds(("rule",), 1)
        await db.conn.execute(
            "UPDATE analysis_tasks SET claimed_at=? WHERE id=?",
            (int(time.time()) - 1200, ts[0]["id"]),
        )
        assert await q.requeue_stale(600) == 1
        cur = await db.conn.execute(
            "SELECT status, last_error FROM analysis_tasks WHERE id=?", (ts[0]["id"],)
        )
        st, err = await cur.fetchone()
        assert st == STATUS_PENDING and "滞留" in (err or "")

    try:
        run(flow())
    finally:
        run(db.close()) if db.opened else None


def test_maintain_and_quick_check(db: Database):
    async def flow():
        await db.maintain()
        assert await db.quick_check() == "ok"
        # 维护可重复执行
        await db.maintain()
        assert await db.quick_check() == "ok"

    try:
        run(flow())
    finally:
        run(db.close()) if db.opened else None
