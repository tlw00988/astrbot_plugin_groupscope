"""Phase 6：月度归档（冷存储）与恢复。

设计要点：
* 归档文件是**独立 SQLite 库**（``archives/messages-YYYY-MM.db``），含该月
  ``messages`` / ``message_segments`` / ``message_mentions`` 与元信息，可单独打开查验；
* 主库删除（``purge``）前先把分析结论的证据指针搬到 ``archived_evidence``，
  分析行本身（topics/events/addressing）**不删**，证据改指归档文件内消息 id；
* 恢复（``unarchive``）按 ``dedup_key`` 去重回灌：群/成员缺失时重建最小行，
  并尽量把 ``archived_evidence`` 的证据链接回主库；
* 每一步都写 ``archive_manifest``（行数 + sha256），可校验、可回放。
"""

from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from ..storage.db import DatabaseError

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

_ARCHIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id                  INTEGER PRIMARY KEY,
  platform_id         TEXT NOT NULL,
  platform_name       TEXT NOT NULL,
  group_ref           INTEGER,
  group_raw_id        TEXT,
  message_id          TEXT NOT NULL,
  dedup_key           TEXT NOT NULL,
  sender_ref          INTEGER,
  sender_raw_id       TEXT,
  sender_raw_name     TEXT,
  event_kind          TEXT NOT NULL DEFAULT 'message',
  notice_type         TEXT,
  sent_at             INTEGER NOT NULL,
  received_at         INTEGER NOT NULL,
  message_type        TEXT NOT NULL,
  text                TEXT,
  reply_to_message_id TEXT,
  reply_to_sender_id  TEXT,
  raw_json            TEXT,
  inserted_at         INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_arch_messages_dedup ON messages(dedup_key);
CREATE INDEX IF NOT EXISTS arch_idx_messages_sent ON messages(sent_at);

CREATE TABLE IF NOT EXISTS message_segments (
  id           INTEGER PRIMARY KEY,
  message_ref  INTEGER NOT NULL,
  seg_index    INTEGER NOT NULL,
  seg_type     TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  text         TEXT,
  UNIQUE (message_ref, seg_index)
);

CREATE TABLE IF NOT EXISTS message_mentions (
  id            INTEGER PRIMARY KEY,
  message_ref   INTEGER NOT NULL,
  kind          TEXT NOT NULL,
  target_raw_id TEXT,
  target_ref    INTEGER,
  UNIQUE (message_ref, kind, target_raw_id)
);

CREATE TABLE IF NOT EXISTS archive_meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);
"""


def validate_month(month: str) -> tuple[int, int]:
    """校验 ``YYYY-MM``，返回该月在本地时区的 ``[start, end)`` 秒级时间戳。"""
    month = (month or "").strip()
    if not _MONTH_RE.match(month):
        raise ValueError("月份格式必须是 YYYY-MM，例如 2026-01")
    year, mon = int(month[:4]), int(month[5:7])
    start = datetime(year, mon, 1)
    if mon == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, mon + 1, 1)
    return int(start.timestamp()), int(end.timestamp())


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


async def _counts(conn: aiosqlite.Connection, start: int, end: int) -> tuple[int, int, int]:
    cur = await conn.execute(
        "SELECT COUNT(*) FROM messages WHERE sent_at >= ? AND sent_at < ?",
        (start, end),
    )
    msgs = int((await cur.fetchone())[0])
    cur = await conn.execute(
        "SELECT COUNT(*) FROM message_segments WHERE message_ref IN"
        " (SELECT id FROM messages WHERE sent_at >= ? AND sent_at < ?)",
        (start, end),
    )
    segs = int((await cur.fetchone())[0])
    cur = await conn.execute(
        "SELECT COUNT(*) FROM message_mentions WHERE message_ref IN"
        " (SELECT id FROM messages WHERE sent_at >= ? AND sent_at < ?)",
        (start, end),
    )
    mentions = int((await cur.fetchone())[0])
    return msgs, segs, mentions


async def create_archive(
    conn: aiosqlite.Connection, *, month: str, archives_dir: Path
) -> dict[str, Any]:
    """把某月原始消息复制到独立归档库（不动主库），返回清单信息。"""
    start, end = validate_month(month)
    archives_dir = Path(archives_dir)
    archives_dir.mkdir(parents=True, exist_ok=True)
    dest = archives_dir / f"messages-{month}.db"
    if dest.exists():
        raise DatabaseError(f"归档文件已存在: {dest.name}（如需重建请先删除该文件）")

    msgs, segs, mentions = await _counts(conn, start, end)
    if msgs == 0:
        raise ValueError(f"{month} 没有可归档的消息")

    arch = await aiosqlite.connect(str(dest), isolation_level=None)
    arch.row_factory = aiosqlite.Row
    source_path = await archive_source(conn)
    try:
        await arch.execute("PRAGMA journal_mode=WAL")
        await arch.executescript(_ARCHIVE_SCHEMA)
        # ATTACH 主库（必须在事务外），随后集合式拷贝
        escaped = str(source_path).replace("'", "''")
        await arch.execute(f"ATTACH DATABASE '{escaped}' AS main_arch")
        await arch.execute("BEGIN IMMEDIATE")
        cur = await arch.execute(
            "INSERT INTO messages(id, platform_id, platform_name, group_ref,"
            " group_raw_id, message_id, dedup_key, sender_ref, sender_raw_id,"
            " sender_raw_name, event_kind, notice_type, sent_at, received_at,"
            " message_type, text, reply_to_message_id, reply_to_sender_id,"
            " raw_json, inserted_at)"
            " SELECT id, platform_id, platform_name, group_ref, group_raw_id,"
            " message_id, dedup_key, sender_ref, sender_raw_id, sender_raw_name,"
            " event_kind, notice_type, sent_at, received_at, message_type, text,"
            " reply_to_message_id, reply_to_sender_id, raw_json, inserted_at"
            " FROM main_arch.messages WHERE sent_at >= ? AND sent_at < ?"
            " ORDER BY id",
            (start, end),
        )
        copied_msgs = getattr(cur, "rowcount", 0) or 0
        cur = await arch.execute(
            "INSERT INTO message_segments(message_ref, seg_index, seg_type,"
            " payload_json, text) SELECT s.message_ref, s.seg_index, s.seg_type,"
            " s.payload_json, s.text FROM main_arch.message_segments s"
            " WHERE s.message_ref IN (SELECT id FROM messages)",
        )
        copied_segs = getattr(cur, "rowcount", 0) or 0
        cur = await arch.execute(
            "INSERT INTO message_mentions(message_ref, kind, target_raw_id,"
            " target_ref) SELECT m.message_ref, m.kind, m.target_raw_id,"
            " m.target_ref FROM main_arch.message_mentions m"
            " WHERE m.message_ref IN (SELECT id FROM messages)",
        )
        copied_mentions = getattr(cur, "rowcount", 0) or 0
        await arch.execute(
            "INSERT INTO archive_meta(key, value) VALUES('month',?),"
            " ('created_at',?), ('source', ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (month, str(int(time.time())), str(source_path)),
        )
        await arch.execute("COMMIT")
        await arch.execute("DETACH DATABASE main_arch")
    except BaseException:
        try:
            await arch.execute("ROLLBACK")
        except Exception:
            pass
        try:
            await arch.execute("DETACH DATABASE main_arch")
        except Exception:
            pass
        await arch.close()
        dest.unlink(missing_ok=True)
        raise
    else:
        await arch.close()

    # 与清单对账：不一致绝不入库
    if (copied_msgs, copied_segs, copied_mentions) != (msgs, segs, mentions):
        dest.unlink(missing_ok=True)
        raise DatabaseError(
            f"归档行数不一致：期望 {(msgs, segs, mentions)}，"
            f"实际 {(copied_msgs, copied_segs, copied_mentions)}，已删除半成品"
        )

    digest = _sha256(dest)
    now = int(time.time())
    await conn.execute(
        "INSERT INTO archive_manifest(month, file, messages, segments, mentions,"
        " purged, sha256, created_at) VALUES(?,?,?,?,?,0,?,?)"
        " ON CONFLICT(month) DO UPDATE SET file=excluded.file,"
        " messages=excluded.messages, segments=excluded.segments,"
        " mentions=excluded.mentions, sha256=excluded.sha256,"
        " created_at=excluded.created_at",
        (month, dest.name, msgs, segs, mentions, digest, now),
    )
    return {
        "month": month,
        "file": dest.name,
        "messages": msgs,
        "segments": segs,
        "mentions": mentions,
        "sha256": digest,
        "purged": False,
    }


async def archive_source(conn: aiosqlite.Connection) -> Path:
    """拿到主库文件路径（``PRAGMA database_list`` 的 main 库路径）。"""
    cur = await conn.execute("PRAGMA database_list")
    rows = await cur.fetchall()
    for r in rows:
        # (seq, name, file)
        name = r[1] if len(r) > 1 else None
        path = r[2] if len(r) > 2 else None
        if name == "main" and path:
            return Path(str(path))
    raise DatabaseError("无法确定主库路径")


async def purge_month(
    conn: aiosqlite.Connection, *, month: str, archives_dir: Path
) -> dict[str, Any]:
    """归档并从主库删除该月原始消息（先存证后删除，分析结论保留）。"""
    start, end = validate_month(month)
    archives_dir = Path(archives_dir)
    manifest = await get_manifest(conn, month)
    if manifest is None:
        manifest = await create_archive(conn, month=month, archives_dir=archives_dir)
    elif not (archives_dir / manifest["file"]).is_file():
        raise DatabaseError(f"归档文件缺失: {manifest['file']}（先执行 /gs archive {month}）")

    now = int(time.time())
    await conn.execute("BEGIN IMMEDIATE")
    try:
        # 1) 先把证据指针搬到 archived_evidence（分析行不删；OR IGNORE 幂等）
        await conn.execute(
            "INSERT OR IGNORE INTO archived_evidence(archive_month, item_kind,"
            " item_id, archive_message_id, role, note, created_at)"
            " SELECT ?, 'topic_mention', tm.topic_id, tm.message_ref, NULL, NULL, ?"
            " FROM topic_mentions tm JOIN messages m ON m.id = tm.message_ref"
            " WHERE m.sent_at >= ? AND m.sent_at < ?",
            (month, now, start, end),
        )
        await conn.execute(
            "INSERT OR IGNORE INTO archived_evidence(archive_month, item_kind,"
            " item_id, archive_message_id, role, note, created_at)"
            " SELECT ?, 'event_evidence', ee.event_id, ee.message_ref, ee.role,"
            " ee.note, ? FROM event_evidence ee JOIN messages m"
            " ON m.id = ee.message_ref"
            " WHERE m.sent_at >= ? AND m.sent_at < ?",
            (month, now, start, end),
        )
        await conn.execute(
            "INSERT OR IGNORE INTO archived_evidence(archive_month, item_kind,"
            " item_id, archive_message_id, role, note, created_at)"
            " SELECT ?, 'addressing', a.id, a.evidence_message, NULL, NULL, ?"
            " FROM addressing a JOIN messages m ON m.id = a.evidence_message"
            " WHERE m.sent_at >= ? AND m.sent_at < ?",
            (month, now, start, end),
        )
        # 2) 再删消息（段/@/互动/话题证据按既有外键级联；称呼证据置空）
        cur = await conn.execute(
            "DELETE FROM messages WHERE sent_at >= ? AND sent_at < ?", (start, end)
        )
        deleted = getattr(cur, "rowcount", 0) or 0
        await conn.execute(
            "UPDATE archive_manifest SET purged=1 WHERE month=?", (month,)
        )
        await conn.execute("COMMIT")
    except BaseException:
        try:
            await conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    manifest["purged"] = True
    manifest["deleted"] = deleted
    return manifest


async def restore_month(
    conn: aiosqlite.Connection, *, month: str, archives_dir: Path
) -> dict[str, Any]:
    """把归档消息回灌主库（按 dedup_key 去重），并尽量回链证据。"""
    start, end = validate_month(month)
    manifest = await get_manifest(conn, month)
    if manifest is None:
        raise ValueError(f"{month} 没有归档记录（/gs archives 查看）")
    dest = Path(archives_dir) / manifest["file"]
    if not dest.is_file():
        raise DatabaseError(f"归档文件缺失: {manifest['file']}")
    if manifest.get("sha256") and _sha256(dest) != manifest["sha256"]:
        raise DatabaseError("归档文件 sha256 校验失败，拒绝恢复（文件可能损坏）")

    arch = await aiosqlite.connect(f"file:{dest}?mode=ro", uri=True, timeout=5.0)
    arch.row_factory = aiosqlite.Row
    try:
        cur = await arch.execute("PRAGMA quick_check(1)")
        row = await cur.fetchone()
        if not row or str(row[0]) != "ok":
            raise DatabaseError(f"归档文件完整性检查失败: {row[0] if row else '?'}")

        cur = await arch.execute("SELECT * FROM messages ORDER BY id")
        rows = await cur.fetchall()
        mapping: dict[int, int] = {}
        plat_of: dict[int, str] = {}
        restored = skipped = 0

        for r in rows:
            cur2 = await conn.execute(
                "SELECT id FROM messages WHERE dedup_key = ?", (r["dedup_key"],)
            )
            exist = await cur2.fetchone()
            if exist is not None:
                # 已存在：记录映射，便于段/@/证据在重跑时补齐（恢复可断点续跑）
                skipped += 1
                mapping[int(r["id"])] = int(exist["id"])
                plat_of[int(r["id"])] = str(r["platform_id"])
                continue
            group_ref = None
            if r["group_raw_id"]:
                await conn.execute(
                    "INSERT INTO groups(platform_id, platform_name, group_id,"
                    " group_name, capture_enabled, first_seen, last_seen)"
                    " VALUES(?,?,?,?,0,?,?)"
                    " ON CONFLICT DO NOTHING",
                    (
                        r["platform_id"],
                        r["platform_name"],
                        r["group_raw_id"],
                        None,
                        r["sent_at"],
                        r["sent_at"],
                    ),
                )
                cur3 = await conn.execute(
                    "SELECT id FROM groups WHERE platform_id=? AND group_id=?",
                    (r["platform_id"], r["group_raw_id"]),
                )
                grow = await cur3.fetchone()
                group_ref = int(grow["id"]) if grow else None
            sender_ref = None
            if r["sender_raw_id"]:
                await conn.execute(
                    "INSERT INTO members(platform_id, user_id, current_nickname,"
                    " first_seen, last_seen, message_count) VALUES(?,?,?,?,?,0)"
                    " ON CONFLICT DO NOTHING",
                    (
                        r["platform_id"],
                        r["sender_raw_id"],
                        r["sender_raw_name"],
                        r["sent_at"],
                        r["sent_at"],
                    ),
                )
                cur3 = await conn.execute(
                    "SELECT id FROM members WHERE platform_id=? AND user_id=?",
                    (r["platform_id"], r["sender_raw_id"]),
                )
                mrow = await cur3.fetchone()
                sender_ref = int(mrow["id"]) if mrow else None

            try:
                cur4 = await conn.execute(
                    "INSERT INTO messages(id, platform_id, platform_name, group_ref,"
                    " group_raw_id, message_id, dedup_key, sender_ref, sender_raw_id,"
                    " sender_raw_name, event_kind, notice_type, sent_at, received_at,"
                    " message_type, text, reply_to_message_id, reply_to_sender_id,"
                    " raw_json, inserted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " RETURNING id",
                    (
                        r["id"],
                        r["platform_id"],
                        r["platform_name"],
                        group_ref,
                        r["group_raw_id"],
                        r["message_id"],
                        r["dedup_key"],
                        sender_ref,
                        r["sender_raw_id"],
                        r["sender_raw_name"],
                        r["event_kind"],
                        r["notice_type"],
                        r["sent_at"],
                        r["received_at"],
                        r["message_type"],
                        r["text"],
                        r["reply_to_message_id"],
                        r["reply_to_sender_id"],
                        r["raw_json"],
                        r["inserted_at"],
                    ),
                )
                new_id = int((await cur4.fetchone())["id"])
            except aiosqlite.IntegrityError:
                # id 被占用（不常见）：交给自增
                cur4 = await conn.execute(
                    "INSERT INTO messages(platform_id, platform_name, group_ref,"
                    " group_raw_id, message_id, dedup_key, sender_ref, sender_raw_id,"
                    " sender_raw_name, event_kind, notice_type, sent_at, received_at,"
                    " message_type, text, reply_to_message_id, reply_to_sender_id,"
                    " raw_json, inserted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " RETURNING id",
                    (
                        r["platform_id"],
                        r["platform_name"],
                        group_ref,
                        r["group_raw_id"],
                        r["message_id"],
                        r["dedup_key"],
                        sender_ref,
                        r["sender_raw_id"],
                        r["sender_raw_name"],
                        r["event_kind"],
                        r["notice_type"],
                        r["sent_at"],
                        r["received_at"],
                        r["message_type"],
                        r["text"],
                        r["reply_to_message_id"],
                        r["reply_to_sender_id"],
                        r["raw_json"],
                        r["inserted_at"],
                    ),
                )
                new_id = int((await cur4.fetchone())["id"])
            mapping[int(r["id"])] = new_id
            plat_of[int(r["id"])] = str(r["platform_id"])
            restored += 1

        # 段与 @ 记录（用映射后的新 id）
        segs = mentions = 0
        if mapping:
            placeholders = ",".join("?" for _ in mapping)
            cur5 = await arch.execute(
                f"SELECT * FROM message_segments WHERE message_ref IN ({placeholders})",
                tuple(mapping),
            )
            for s in await cur5.fetchall():
                await conn.execute(
                    "INSERT OR IGNORE INTO message_segments(message_ref, seg_index,"
                    " seg_type, payload_json, text) VALUES(?,?,?,?,?)",
                    (mapping[int(s["message_ref"])], s["seg_index"], s["seg_type"],
                     s["payload_json"], s["text"]),
                )
                segs += 1
            cur5 = await arch.execute(
                f"SELECT * FROM message_mentions WHERE message_ref IN ({placeholders})",
                tuple(mapping),
            )
            for m in await cur5.fetchall():
                target_ref = None
                if m["target_raw_id"]:
                    cur6 = await conn.execute(
                        "SELECT id FROM members WHERE platform_id=? AND user_id=?",
                        (
                            plat_of.get(int(m["message_ref"]), ""),
                            m["target_raw_id"],
                        ),
                    )
                    trow = await cur6.fetchone()
                    target_ref = int(trow["id"]) if trow else None
                await conn.execute(
                    "INSERT OR IGNORE INTO message_mentions(message_ref, kind,"
                    " target_raw_id, target_ref) VALUES(?,?,?,?)",
                    (mapping[int(m["message_ref"])], m["kind"], m["target_raw_id"],
                     target_ref),
                )
                mentions += 1
    finally:
        await arch.close()

    # 回链证据（分析行还在、消息回来了就补上）
    relinked = 0
    cur = await conn.execute(
        "SELECT * FROM archived_evidence WHERE archive_month=?", (month,)
    )
    for e in await cur.fetchall():
        old_msg = int(e["archive_message_id"])
        new_msg = mapping.get(old_msg)
        if new_msg is None:
            continue
        if e["item_kind"] == "topic_mention":
            cur2 = await conn.execute("SELECT 1 FROM topics WHERE id=?", (e["item_id"],))
            if await cur2.fetchone() is None:
                continue
            cur2 = await conn.execute(
                "INSERT OR IGNORE INTO topic_mentions(topic_id, message_ref, weight,"
                " created_at) VALUES(?,?,1,?)",
                (e["item_id"], new_msg, int(time.time())),
            )
            relinked += getattr(cur2, "rowcount", 0) or 0
        elif e["item_kind"] == "event_evidence":
            cur2 = await conn.execute("SELECT 1 FROM events WHERE id=?", (e["item_id"],))
            if await cur2.fetchone() is None:
                continue
            cur2 = await conn.execute(
                "INSERT OR IGNORE INTO event_evidence(event_id, message_ref, role,"
                " note, created_at) VALUES(?,?,?,?,?)",
                (e["item_id"], new_msg, e["role"] or "spread", e["note"],
                 int(time.time())),
            )
            relinked += getattr(cur2, "rowcount", 0) or 0
        elif e["item_kind"] == "addressing":
            cur2 = await conn.execute(
                "UPDATE addressing SET evidence_message=? WHERE id=?"
                " AND evidence_message IS NULL",
                (new_msg, e["item_id"]),
            )
            relinked += getattr(cur2, "rowcount", 0) or 0

    await conn.execute(
        "UPDATE archive_manifest SET restored_at=?, purged=0 WHERE month=?",
        (int(time.time()), month),
    )
    return {
        "month": month,
        "restored": restored,
        "skipped": skipped,
        "segments": segs,
        "mentions": mentions,
        "evidence_relinked": relinked,
        "range": (start, end),
    }


async def get_manifest(conn: aiosqlite.Connection, month: str) -> dict[str, Any] | None:
    cur = await conn.execute(
        "SELECT * FROM archive_manifest WHERE month=?", (month,)
    )
    row = await cur.fetchone()
    return dict(row) if row else None


async def list_manifests(conn: aiosqlite.Connection) -> list[dict[str, Any]]:
    cur = await conn.execute(
        "SELECT * FROM archive_manifest ORDER BY month DESC"
    )
    return [dict(r) for r in await cur.fetchall()]


async def verify_manifest(
    conn: aiosqlite.Connection, *, month: str, archives_dir: Path
) -> dict[str, Any]:
    """校验归档：文件存在、sha256 匹配、归档库 quick_check、行数对账。"""
    manifest = await get_manifest(conn, month)
    if manifest is None:
        raise ValueError(f"{month} 没有归档记录")
    dest = Path(archives_dir) / manifest["file"]
    out: dict[str, Any] = {"month": month, "file": manifest["file"], "ok": True}
    if not dest.is_file():
        out["ok"] = False
        out["error"] = "文件不存在"
        return out
    digest = _sha256(dest)
    out["sha256_match"] = digest == manifest["sha256"]
    arch = await aiosqlite.connect(f"file:{dest}?mode=ro", uri=True, timeout=5.0)
    try:
        cur = await arch.execute("PRAGMA quick_check(1)")
        row = await cur.fetchone()
        out["quick_check"] = str(row[0]) if row else "unknown"
        cur = await arch.execute("SELECT COUNT(*) FROM messages")
        out["messages"] = int((await cur.fetchone())[0])
        cur = await arch.execute("SELECT COUNT(*) FROM message_segments")
        out["segments"] = int((await cur.fetchone())[0])
    finally:
        await arch.close()
    out["counts_match"] = (
        out["messages"] == manifest["messages"] and out["segments"] == manifest["segments"]
    )
    out["ok"] = bool(out["sha256_match"] and out["quick_check"] == "ok" and out["counts_match"])
    return out
