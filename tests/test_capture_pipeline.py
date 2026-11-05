"""采集管线：隐私策略、异常隔离、队列丢弃、批量写入与停机排空。"""

from __future__ import annotations

import asyncio
import logging

from conftest import make_event, make_message, run
from groupscope.capture.listener import MessageListener
from groupscope.capture.models import NormalizedMessage
from groupscope.capture.service import CaptureService
from groupscope.capture.serializer import normalize_event
from groupscope.privacy import CapturePolicy
from groupscope.storage import repo

LOG = logging.getLogger("groupscope-test")


def _policy(**overrides) -> CapturePolicy:
    from conftest import make_settings

    return CapturePolicy(make_settings(**overrides))


def _allow(policy: CapturePolicy, *, group="900001", sender="1001", kind="message", **kw):
    return policy.allow(
        platform_name="aiocqhttp",
        platform_id="aiocqhttp-1",
        group_raw_id=group,
        sender_raw_id=sender,
        is_notice=(kind == "notice"),
        **kw,
    )


# ---------------------------------------------------------------------
# 策略
# ---------------------------------------------------------------------


def test_empty_whitelist_captures_nothing():
    policy = _policy(group_whitelist=[])
    assert _allow(policy) is False


def test_whitelist_and_platform_scoping():
    policy = _policy(group_whitelist=["aiocqhttp:900001"])
    assert _allow(policy) is True
    assert _allow(policy, group="900002") is False  # 不在白名单
    assert _allow(policy, group=None) is False  # 私聊不采集

    # 平台限定：其它平台同群号不放行
    policy2 = _policy(group_whitelist=["telegram:900001"])
    assert policy2.allow(
        platform_name="aiocqhttp",
        platform_id="aiocqhttp-1",
        group_raw_id="900001",
        sender_raw_id="1001",
        is_notice=False,
    ) is False


def test_blacklist_overrides_whitelist():
    policy = _policy(
        group_whitelist=["900001", "900002"],
        group_blacklist=["900001"],
    )
    assert _allow(policy, group="900001") is False
    assert _allow(policy, group="900002") is True


def test_excluded_users_and_master_switch():
    policy = _policy(excluded_users=["1001"])
    assert _allow(policy, sender="1001") is False
    assert _allow(policy, sender="1002") is True

    off = _policy(capture_enabled=False)
    assert _allow(off) is False


def test_notice_switch_and_pause():
    policy = _policy(record_notices=False)
    assert _allow(policy, kind="notice") is False
    assert _allow(policy, kind="message") is True

    on = _policy()
    on.paused.add(("aiocqhttp-1", "900001"))
    assert _allow(on) is False


# ---------------------------------------------------------------------
# 监听器
# ---------------------------------------------------------------------


def test_listener_rejects_out_of_scope_without_enqueue():
    policy = _policy(group_whitelist=["900002"])  # 不含 900001
    enqueued: list = []
    rejected = {"n": 0}
    listener = MessageListener(policy, enqueued.append, LOG, rejected=lambda: rejected.__setitem__("n", rejected["n"] + 1))

    assert run(listener.handle_event(make_event())) is False
    assert enqueued == [] and rejected["n"] == 1

    def collect(item) -> bool:
        enqueued.append(item)
        return True

    ok = MessageListener(_policy(), collect, LOG)
    assert run(ok.handle_event(make_event())) is True
    assert len(enqueued) == 1 and isinstance(enqueued[0], NormalizedMessage)


def test_listener_swallows_all_exceptions():
    """任何异常都必须被吞掉——AstrBot 会因 handler 异常停止事件传播。"""

    class ExplodingEvent:
        @property
        def message_obj(self):
            raise RuntimeError("平台适配器炸了")

    listener = MessageListener(_policy(), lambda item: True, LOG)
    # 不抛异常即为通过
    assert run(listener.handle_event(ExplodingEvent())) is False

    # normalize 阶段异常同样隔离
    policy = _policy()

    class BadNormalize:
        message_obj = make_message()

        def get_platform_id(self):
            raise ValueError("boom")

        def get_platform_name(self):
            return "aiocqhttp"

        def get_group_id(self):
            return "900001"

    assert run(listener.handle_event(BadNormalize())) is False


# ---------------------------------------------------------------------
# 服务：队列 / 批量写入 / 停机
# ---------------------------------------------------------------------


def _service(db, settings, policy, tmp_path) -> CaptureService:
    return CaptureService(db, settings, policy, tmp_path, LOG)


def test_queue_full_drops_not_blocks(db, tmp_path):
    from conftest import make_settings

    settings = make_settings(queue_maxsize=16, batch_max_rows=1000)
    svc = _service(db, settings, _policy(), tmp_path)  # 未 start：写者不消费

    accepted = 0
    for i in range(40):
        item = normalize_event(make_event(make_message(message_id=str(i + 1))))
        if svc.enqueue(item):
            accepted += 1
    assert accepted == 16  # 队列上限
    assert svc.metrics["dropped"] == 24
    assert svc.queue_depth == 16  # 非阻塞，回调立即返回


def test_writer_batches_and_persists(db, tmp_path):
    from conftest import make_settings

    settings = make_settings(batch_max_rows=5, batch_max_interval_ms=20)
    svc = _service(db, settings, _policy(), tmp_path)

    # 注意：start / enqueue / stop 必须在同一个事件循环里——
    # 后台任务绑定创建它的 loop，跨 run() 会让任务随旧 loop 销毁。
    async def flow():
        await svc.start()
        try:
            for i in range(12):
                svc.enqueue(
                    normalize_event(make_event(make_message(message_id=str(i), text=f"m{i}")))
                )
            ok = await svc.wait_idle(timeout=5)
            count = await repo.count_messages(db.conn)
            return ok, count
        finally:
            await svc.stop()

    ok, count = run(flow())
    assert ok is True
    assert count == 12
    assert svc.metrics["written"] == 12
    assert svc.metrics["inserted"] == 12
    assert svc.metrics["batches"] >= 2  # 达到批量阈值分多批
    assert svc.metrics["write_errors"] == 0
    assert svc.queue_depth == 0  # 停机后排空


def test_stop_drains_pending_queue(db, tmp_path):
    from conftest import make_settings

    settings = make_settings(batch_max_rows=3, batch_max_interval_ms=50)
    svc = _service(db, settings, _policy(), tmp_path)

    async def flow():
        await svc.start()
        for i in range(7):
            svc.enqueue(normalize_event(make_event(make_message(message_id=str(i)))))
        await svc.stop()  # 停机前排空队列
        return await repo.count_messages(db.conn)

    assert run(flow()) == 7
    assert svc.queue_depth == 0


def test_service_loads_paused_groups_from_db(db, tmp_path):
    from conftest import make_settings

    item = normalize_event(make_event(make_message(message_id="1")))
    run(repo.insert_batch(db.conn, [item]))
    groups = run(repo.list_groups(db.conn))
    run(repo.set_group_capture(db.conn, groups[0]["id"], False))

    svc = _service(db, make_settings(), _policy(), tmp_path)

    async def flow():
        await svc.start()
        try:
            return set(svc.policy.paused)
        finally:
            await svc.stop()

    assert run(flow()) == {("aiocqhttp-1", "900001")}


def test_write_error_is_isolated(db, tmp_path):
    """单批写失败只计数，不杀写入循环，后续批次照常。"""
    from conftest import make_settings

    settings = make_settings(batch_max_rows=2, batch_max_interval_ms=20)
    svc = _service(db, settings, _policy(), tmp_path)

    bad = normalize_event(make_event(make_message(message_id="bad")))
    bad.platform_id = None  # 违反 NOT NULL → 整批失败

    async def flow():
        await svc.start()
        try:
            svc.enqueue(bad)
            svc.enqueue(bad)  # 凑满一批触发写入
            await asyncio.sleep(0.15)
            before = svc.metrics["write_errors"]
            svc.enqueue(normalize_event(make_event(make_message(message_id="good1"))))
            svc.enqueue(normalize_event(make_event(make_message(message_id="good2"))))
            ok = await svc.wait_idle(timeout=5)
            return before, ok, await repo.count_messages(db.conn)
        finally:
            await svc.stop()

    errors, ok, count = run(flow())
    assert errors >= 1  # 坏批次被记录
    assert ok is True
    assert count == 2  # 好消息仍然落库
    assert svc.metrics["write_errors"] == errors
