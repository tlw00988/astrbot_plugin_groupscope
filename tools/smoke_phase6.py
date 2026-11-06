"""Phase 6 冒烟：归档全链路 + 任务退避/回收 + 维护。

链路：造数 → create_archive（主库不动）→ 校验 sha256/行数 →
purge（证据入 archived_evidence，消息删除，分析结论保留）→
restore（去重回灌 + 证据回链）→ verify；另测任务退避认领与 requeue_stale。
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
import time

PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR.parent))   # 使 astrbot_plugin_groupscope 可导入
os.environ.setdefault("ASTRBOT_ROOT", tempfile.mkdtemp(prefix="gs-phase6-"))

from astrbot_plugin_groupscope.groupscope.ops import archives as A  # noqa: E402
from astrbot_plugin_groupscope.groupscope.storage.db import Database  # noqa: E402
from astrbot_plugin_groupscope.groupscope.analysis.tasks import (  # noqa: E402
    STATUS_PENDING,
    STATUS_RUNNING,
    TaskQueue,
)

_ROOT = os.environ["ASTRBOT_ROOT"]
DATA = str(Path(_ROOT) / "data" / "plugin_data" / "astrbot_plugin_groupscope")
ARCHIVES = f"{DATA}/archives"


async def seed(db: Database) -> int:
    """造 2026-01 的消息 + 分析结论与证据，返回消息数。"""
    c = db.conn
    # 两个月份：2026-01（要归档）与 2026-02（保留）
    await c.execute(
        "INSERT INTO groups(platform_id,platform_name,group_id,group_name,"
        " capture_enabled,first_seen,last_seen) VALUES('aiocqhttp','QQ','123',"
        "'测试群',1,?,?)",
        (1767196800, 1770000000),
    )
    for uid, nick in (("1001", "王大佬"), ("1002", "小明")):
        await c.execute(
            "INSERT INTO members(platform_id,user_id,current_nickname,first_seen,"
            "last_seen,message_count) VALUES('aiocqhttp',?,?,?, ?,10)",
            (uid, nick, 1767196800, 1770000000),
        )

    n = 0
    # 2026-01 共 20 条
    for i in range(20):
        ts = 1767283200 + i * 60  # 2026-01-02 内
        dedup = f"jan-{i}"
        await c.execute(
            "INSERT INTO messages(platform_id,platform_name,group_ref,group_raw_id,"
            "message_id,dedup_key,sender_ref,sender_raw_id,sender_raw_name,event_kind,"
            "sent_at,received_at,message_type,text,inserted_at)"
            " VALUES('aiocqhttp','QQ',1,'123',?,?,1,? ,?,'message',?,?,'text',?,?)",
            (dedup, dedup, "1002", "小明", ts, ts, f"一月消息{i}", ts),
        )
        cur = await c.execute("SELECT last_insert_rowid()")
        mid = (await cur.fetchone())[0]
        await c.execute(
            "INSERT INTO message_segments(message_ref,seg_index,seg_type,payload_json,text)"
            " VALUES(?,0,'text','{}',?)",
            (mid, f"一月消息{i}"),
        )
        if i % 5 == 0:
            await c.execute(
                "INSERT INTO message_mentions(message_ref,kind,target_raw_id,target_ref)"
                " VALUES(?,'at','1001',1)",
                (mid,),
            )
        n += 1
    # 2026-02 共 3 条（不归档）
    for i in range(3):
        ts = 1769875200 + i * 60  # 2026-02-01
        dedup = f"feb-{i}"
        await c.execute(
            "INSERT INTO messages(platform_id,platform_name,group_ref,group_raw_id,"
            "message_id,dedup_key,sender_ref,sender_raw_id,sender_raw_name,event_kind,"
            "sent_at,received_at,message_type,text,inserted_at)"
            " VALUES('aiocqhttp','QQ',1,'123',?,?,1,? ,?,'message',?,?,'text',?,?)",
            (dedup, dedup, "1001", "王大佬", ts, ts, f"二月消息{i}", ts),
        )
        n += 1

    # 分析结论 + 证据（指向 1 月消息）
    now = 1767283200
    await c.execute(
        "INSERT INTO topics(group_ref,title,summary,keywords_json,first_seen,last_seen,"
        "mention_count,source,confidence,created_at)"
        " VALUES(1,'一月话题','x','[]',?,?,5,'rule',0.6,?)",
        (now, now + 1000, now),
    )
    await c.execute(
        "INSERT INTO events(group_ref,title,summary,event_kind,first_seen,last_seen,"
        "mention_count,source,confidence,created_at) VALUES(1,'一月梗','y','meme',"
        "?,?,4,'rule',0.5,?)",
        (now, now + 1000, now),
    )
    await c.execute(
        "INSERT INTO addressing(group_ref,from_member,to_member,address,source,"
        "confidence,created_at,last_seen,seen_count,reviewed)"
        " VALUES(1,2,1,'王哥','rule',0.9,?,?,3,1)",
        (now, now),
    )
    # 证据：话题证据=消息1，事件证据=消息2（origin），称呼证据=消息3
    await c.execute(
        "INSERT INTO topic_mentions(topic_id,message_ref,weight,created_at)"
        " VALUES(1,1,1,?)",
        (now,),
    )
    await c.execute(
        "INSERT INTO event_evidence(event_id,message_ref,role,note,created_at)"
        " VALUES(1,2,'origin','首现',?)",
        (now,),
    )
    await c.execute("UPDATE addressing SET evidence_message=3 WHERE id=1")
    await c.commit()
    return n


async def count(db: Database, sql: str, *p) -> int:
    cur = await db.conn.execute(sql, p)
    return int((await cur.fetchone())[0])


async def main() -> None:
    db = Database(f"{DATA}/groupscope.db")
    await db.open()
    q = TaskQueue(db.conn)
    try:
        total = await seed(db)
        assert total == 23, total
        print(f"seed: {total} messages")

        # ---- 1) create_archive（不动主库） ----
        info = await A.create_archive(
            db.conn, month="2026-01", archives_dir=ARCHIVES
        )
        assert info["messages"] == 20 and info["segments"] == 20, info
        assert info["mentions"] == 4 and not info["purged"], info
        assert await count(db, "SELECT COUNT(*) FROM messages") == 23
        print(f"create_archive: {info['messages']} msgs, sha256 {info['sha256'][:12]}…")

        # 幂等：重复创建应报错
        try:
            await A.create_archive(db.conn, month="2026-01", archives_dir=ARCHIVES)
            raise AssertionError("重复归档应报错")
        except Exception as e:
            print(f"duplicate archive rejected: {type(e).__name__}")

        # ---- 2) verify（未 purge） ----
        v = await A.verify_manifest(
            db.conn, month="2026-01", archives_dir=ARCHIVES
        )
        assert v["ok"], v
        print(f"verify: ok={v['ok']} quick_check={v['quick_check']}")

        # ---- 3) purge ----
        p = await A.purge_month(db.conn, month="2026-01", archives_dir=ARCHIVES)
        assert p["purged"] and p["deleted"] == 20, p
        assert await count(db, "SELECT COUNT(*) FROM messages") == 3, "2月消息应保留"
        assert await count(db, "SELECT COUNT(*) FROM message_segments") == 0
        assert await count(db, "SELECT COUNT(*) FROM message_mentions") == 0
        # 分析结论保留
        assert await count(db, "SELECT COUNT(*) FROM topics") == 1
        assert await count(db, "SELECT COUNT(*) FROM events") == 1
        assert await count(db, "SELECT COUNT(*) FROM addressing") == 1
        # 证据指针搬家
        assert await count(db, "SELECT COUNT(*) FROM archived_evidence") == 3
        # 原证据表被级联清理 / 置空
        assert await count(db, "SELECT COUNT(*) FROM topic_mentions") == 0
        assert await count(db, "SELECT COUNT(*) FROM event_evidence") == 0
        assert await count(
            db, "SELECT COUNT(*) FROM addressing WHERE evidence_message IS NULL"
        ) == 1
        print("purge: 20 msgs removed, 3 evidence pointers archived, analysis kept")

        # 幂等：再 purge 一次不应炸
        p2 = await A.purge_month(db.conn, month="2026-01", archives_dir=ARCHIVES)
        assert p2["deleted"] == 0
        assert await count(db, "SELECT COUNT(*) FROM archived_evidence") == 3
        print("purge idempotent: OK")

        # ---- 4) restore ----
        r = await A.restore_month(
            db.conn, month="2026-01", archives_dir=ARCHIVES
        )
        assert r["restored"] == 20 and r["skipped"] == 0, r
        assert r["segments"] == 20 and r["mentions"] == 4, r
        assert r["evidence_relinked"] == 3, r
        assert await count(db, "SELECT COUNT(*) FROM messages") == 23
        # 回链后的证据可查
        assert await count(db, "SELECT COUNT(*) FROM topic_mentions") == 1
        assert await count(db, "SELECT COUNT(*) FROM event_evidence") == 1
        assert await count(
            db, "SELECT COUNT(*) FROM addressing WHERE evidence_message IS NOT NULL"
        ) == 1
        # manifest 复位
        m = await A.get_manifest(db.conn, "2026-01")
        assert m and not m["purged"] and m["restored_at"], m
        print(
            f"restore: {r['restored']} msgs, {r['segments']} segs, "
            f"{r['evidence_relinked']} evidence relinked"
        )

        # 断点续跑：再恢复一次应全部跳过且不炸
        r2 = await A.restore_month(
            db.conn, month="2026-01", archives_dir=ARCHIVES
        )
        assert r2["restored"] == 0 and r2["skipped"] == 20, r2
        assert await count(db, "SELECT COUNT(*) FROM messages") == 23
        print(f"restore resume: skipped={r2['skipped']}（无重复）")

        # ---- 5) 损坏的归档文件应被拒绝 ----
        arch_path = f"{ARCHIVES}/messages-2026-01.db"
        with open(arch_path, "r+b") as f:
            f.seek(100)
            f.write(b"X" * 16)
        try:
            await A.restore_month(db.conn, month="2026-01", archives_dir=ARCHIVES)
            raise AssertionError("损坏文件应被拒绝")
        except Exception as e:
            print(f"corrupt archive rejected: {type(e).__name__}: {e}")
        # 还原：重新生成归档
        import os as _os

        _os.unlink(arch_path)
        _os.unlink(arch_path + "-wal", ) if _os.path.exists(arch_path + "-wal") else None
        _os.unlink(arch_path + "-shm") if _os.path.exists(arch_path + "-shm") else None
        await A.create_archive(db.conn, month="2026-01", archives_dir=ARCHIVES)
        v2 = await A.verify_manifest(
            db.conn, month="2026-01", archives_dir=ARCHIVES
        )
        assert v2["ok"], v2
        print("archive rebuilt + verify ok")

        # ---- 6) 任务退避 ----
        await q.enqueue("rule", "g1:w1", payload={"group_ref": 1})
        task = (await q.claim_kinds(("rule",), 1))[0]
        assert task["status"] == STATUS_RUNNING
        await q.fail(task, "模拟失败1")
        row = await db.conn.execute(
            "SELECT status, not_before, attempts FROM analysis_tasks WHERE id=?",
            (task["id"],),
        )
        st, nb, att = (await row.fetchone())
        assert st == STATUS_PENDING and nb and nb > time.time(), (st, nb)
        # 未到期：认领不到
        got = await q.claim_kinds(("rule",), 1)
        assert got == [], got
        print(f"backoff: pending, not_before=+{nb - int(time.time())}s, claim blocked")

        # 到期后可再认领
        await db.conn.execute(
            "UPDATE analysis_tasks SET not_before=? WHERE id=?",
            (int(time.time()) - 1, task["id"]),
        )
        task2 = (await q.claim_kinds(("rule",), 1))[0]
        assert task2["id"] == task["id"]
        await q.fail(task2, "模拟失败2")
        row = await db.conn.execute(
            "SELECT not_before FROM analysis_tasks WHERE id=?", (task["id"],)
        )
        nb2 = (await row.fetchone())[0]
        assert nb2 > time.time()
        span = nb2 - int(time.time())
        assert 55 <= span <= 65, span  # 第二次退避 = 60s
        print(f"backoff growth: 2nd wait {span}s (expect ~60)")

        # 超过 max_attempts → failed
        await db.conn.execute(
            "UPDATE analysis_tasks SET not_before=NULL, attempts=2 WHERE id=?",
            (task["id"],),
        )
        task3 = (await q.claim_kinds(("rule",), 1))[0]
        await q.fail(task3, "第三次失败")
        row = await db.conn.execute(
            "SELECT status FROM analysis_tasks WHERE id=?", (task["id"],)
        )
        assert (await row.fetchone())[0] == "failed"
        print("max_attempts reached -> failed: OK")

        # ---- 7) requeue_stale ----
        await q.enqueue("rule", "g1:stale", payload={"group_ref": 1})
        t = (await q.claim_kinds(("rule",), 1))[0]
        await db.conn.execute(
            "UPDATE analysis_tasks SET claimed_at=? WHERE id=?",
            (int(time.time()) - 1200, t["id"]),
        )
        n = await q.requeue_stale(600)
        assert n == 1, n
        row = await db.conn.execute(
            "SELECT status, last_error FROM analysis_tasks WHERE id=?", (t["id"],)
        )
        st, err = (await row.fetchone())
        assert st == STATUS_PENDING and "滞留" in (err or ""), (st, err)
        print(f"requeue_stale: {n} task recovered")

        # ---- 8) maintain ----
        await db.maintain()
        await db.quick_check()
        print("maintain + quick_check: OK")

        # ---- 9) 列表 ----
        lst = await A.list_manifests(db.conn)
        assert len(lst) == 1 and lst[0]["month"] == "2026-01", lst
        print(f"list_manifests: {[x['month'] for x in lst]}")

        print("PHASE6 SMOKE OK")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
