"""事务串行化：并发协程在事务体内让出控制权时不得出现嵌套 BEGIN。

这是一条回归测试：aiosqlite 只串行化单条语句，事务体内一旦 ``await``
（回查成员、回查证据），另一个协程就可能在事务未提交时再次
``BEGIN IMMEDIATE``，线上表现为偶发的
``cannot start a transaction within a transaction``（采集 write_errors /
分析任务失败重试）。
"""

from __future__ import annotations

import asyncio

from conftest import run
from groupscope.storage import tx
from groupscope.storage.db import Database


def _put(key: str, value: str, ts: int):
    return (
        "INSERT INTO plugin_state(key,value,updated_at) VALUES(?,?,?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
        " updated_at=excluded.updated_at",
        (key, value, ts),
    )


def test_concurrent_transactions_are_serialized(db: Database):
    async def flow() -> list[BaseException]:
        errors: list[BaseException] = []

        async def writer(i: int) -> None:
            for n in range(25):
                try:
                    await tx.begin(db.conn)
                except BaseException as e:  # noqa: BLE001
                    errors.append(e)
                    return
                try:
                    sql, params = _put(f"k{i}", str(n), n)
                    await db.conn.execute(sql, params)
                    await asyncio.sleep(0)  # 事务体内让出 → 诱发交错
                    await db.conn.execute(sql, params)
                except BaseException as e:  # noqa: BLE001
                    errors.append(e)
                    await tx.rollback(db.conn)
                    return
                await tx.commit(db.conn)

        await asyncio.gather(*(writer(i) for i in range(4)))
        cur = await db.conn.execute("SELECT COUNT(*) FROM plugin_state")
        count = int((await cur.fetchone())[0])
        # 锁必须已全部释放（后续事务还能开）
        await tx.begin(db.conn)
        await tx.commit(db.conn)
        assert count == 4, f"四把钥匙都应写入，实际 {count}"
        return errors

    errors = run(flow())
    assert not errors, f"并发事务报错: {errors}"
    assert run(db.quick_check()) == "ok"


def test_begin_releases_lock_on_failure(db: Database):
    async def flow() -> None:
        # 先人为制造一个“遗留事务”，begin() 应自愈清掉再开新事务
        await db.conn.execute("BEGIN IMMEDIATE")
        await tx.begin(db.conn)
        try:
            await db.conn.execute(*_put("heal", "1", 1))
        finally:
            await tx.commit(db.conn)
        # 锁已释放：可以立即再次 begin
        await tx.begin(db.conn)
        await tx.commit(db.conn)

    run(flow())
    assert run(db.quick_check()) == "ok"


def test_rollback_releases_lock(db: Database):
    async def flow() -> None:
        await tx.begin(db.conn)
        try:
            await db.conn.execute(*_put("boom", "1", 1))
            raise RuntimeError("模拟失败")
        except RuntimeError:
            await tx.rollback(db.conn)
        # 回滚后锁已释放
        await tx.begin(db.conn)
        await tx.commit(db.conn)

    try:
        run(flow())
    except RuntimeError:
        raise AssertionError("rollback 后不应再抛出")
    cur_rows = run(_read(db))
    assert cur_rows == 0, "回滚的内容不应落库"
    assert run(db.quick_check()) == "ok"


async def _read(db: Database) -> int:
    cur = await db.conn.execute(
        "SELECT COUNT(*) FROM plugin_state WHERE key='boom'"
    )
    return int((await cur.fetchone())[0])
