"""单连接写事务串行化（防止 ``cannot start a transaction within a transaction``）。

背景
----
插件只用**一个** aiosqlite 连接（``isolation_level=None``），写事务靠显式
``BEGIN IMMEDIATE``/``COMMIT``/``ROLLBACK``。aiosqlite 只把**单条语句**排进队列，
并不会把“多条语句组成的事务”当成一个原子块——而事务体内存在 ``await``
（回查成员主键、回查证据消息、更新关联行），执行到这些 await 时控制权会让给
其它协程。此时采集写入循环（``repo.insert_batch``）或分析写入（称呼/话题/事件）
完全可能在上一个事务还没 COMMIT 时再次 ``BEGIN IMMEDIATE``，于是报：

    sqlite3.OperationalError: cannot start a transaction within a transaction

后果是偶发的写入失败（采集侧表现为 write_errors、分析侧表现为任务失败重试）。

解决
----
给**每个连接**配一把 ``asyncio.Lock``，把 ``BEGIN → COMMIT/ROLLBACK`` 整段串行化：

* ``begin(conn)``      先拿锁，再 ``BEGIN IMMEDIATE``（拿锁失败则不占锁）；
* ``commit(conn)``     ``COMMIT`` 后释放锁；
* ``rollback(conn)``   ``ROLLBACK`` 后释放锁（即使 ROLLBACK 报错也释放，避免死锁）。

自愈：拿锁后若发现连接上还有残留事务（历史代码路径漏 COMMIT），先 ``ROLLBACK``
清掉再开新事务，避免一次遗留把后续所有写入卡死。

约束：**事务体内不得再次调用 ``begin()``**（同一把锁不可重入会死锁）。当前代码库
中所有事务体内只有 SELECT/INSERT/UPDATE/DELETE，不含其它事务函数调用。
"""

from __future__ import annotations

import asyncio

_LOCK_ATTR = "_groupscope_tx_lock"


def _lock(conn):
    lock = getattr(conn, _LOCK_ATTR, None)
    if lock is None:
        lock = asyncio.Lock()
        try:
            setattr(conn, _LOCK_ATTR, lock)
        except AttributeError:  # pragma: no cover - 极少数 __slots__ 实现
            raise RuntimeError("连接对象无法持有事务锁") from None
    return lock


async def begin(conn) -> None:
    """开启写事务并占锁；异常时会把锁还回去。"""
    lock = _lock(conn)
    await lock.acquire()
    try:
        if bool(getattr(conn, "in_transaction", False)):
            # 自愈：上次遗留的未提交事务，先清掉
            try:
                await conn.execute("ROLLBACK")
            except BaseException:
                pass
        await conn.execute("BEGIN IMMEDIATE")
    except BaseException:
        lock.release()
        raise


async def commit(conn) -> None:
    """提交事务并释放锁。"""
    lock = _lock(conn)
    try:
        await conn.execute("COMMIT")
    finally:
        lock.release()


async def rollback(conn) -> None:
    """回滚事务并释放锁（回滚本身失败也释放锁，防止死锁）。"""
    lock = _lock(conn)
    try:
        try:
            await conn.execute("ROLLBACK")
        except BaseException:
            # 事务可能已被 SQLite 自动回滚；再试一次，仍失败就放行
            try:
                await conn.execute("ROLLBACK")
            except BaseException:
                pass
    finally:
        lock.release()
