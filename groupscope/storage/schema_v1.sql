-- GroupScope schema v1（Phase 1）
-- 约定：
--   * 所有 unix 时间均为秒（INTEGER）
--   * raw_id 字段保存平台原始 ID 字符串，ref 字段为本库外键
--   * 原始消息只追加，AI 分析结果不得写入本表

-- 群组
CREATE TABLE IF NOT EXISTS groups (
  id              INTEGER PRIMARY KEY,
  platform_id     TEXT NOT NULL,
  platform_name   TEXT NOT NULL,
  group_id        TEXT NOT NULL,
  group_name      TEXT,
  first_seen      INTEGER NOT NULL,
  last_seen       INTEGER NOT NULL,
  member_count    INTEGER,
  capture_enabled INTEGER NOT NULL DEFAULT 1,
  meta_json       TEXT,
  UNIQUE (platform_id, group_id)
);
CREATE INDEX IF NOT EXISTS idx_groups_last_seen ON groups(last_seen);

-- 平台内成员身份（不跨平台、不跨账号合并）
CREATE TABLE IF NOT EXISTS members (
  id               INTEGER PRIMARY KEY,
  platform_id      TEXT NOT NULL,
  user_id          TEXT NOT NULL,
  current_nickname TEXT,
  first_seen       INTEGER NOT NULL,
  last_seen        INTEGER NOT NULL,
  message_count    INTEGER NOT NULL DEFAULT 0,
  UNIQUE (platform_id, user_id)
);

-- 成员在单个群中的档案单元
CREATE TABLE IF NOT EXISTS member_group (
  id            INTEGER PRIMARY KEY,
  member_id     INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  group_id      INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  current_nick  TEXT,
  first_seen    INTEGER NOT NULL,
  last_seen     INTEGER NOT NULL,
  message_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (member_id, group_id)
);

-- 历史昵称/群名片（观察所得，非推断）
CREATE TABLE IF NOT EXISTS member_aliases (
  id         INTEGER PRIMARY KEY,
  member_id  INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  source     TEXT NOT NULL,
  first_seen INTEGER NOT NULL,
  last_seen  INTEGER NOT NULL,
  seen_count INTEGER NOT NULL DEFAULT 1,
  UNIQUE (member_id, alias, source)
);
CREATE INDEX IF NOT EXISTS idx_aliases_alias ON member_aliases(alias);

-- 原始消息（含通知类可观测事件）
CREATE TABLE IF NOT EXISTS messages (
  id                  INTEGER PRIMARY KEY,
  platform_id         TEXT NOT NULL,
  platform_name       TEXT NOT NULL,
  group_ref           INTEGER REFERENCES groups(id),
  group_raw_id        TEXT,
  message_id          TEXT NOT NULL,
  dedup_key           TEXT NOT NULL,
  sender_ref          INTEGER REFERENCES members(id),
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
CREATE UNIQUE INDEX IF NOT EXISTS ux_messages_dedup ON messages(dedup_key);
CREATE INDEX IF NOT EXISTS idx_messages_group_time  ON messages(group_ref, sent_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender_time ON messages(sender_ref, sent_at);
CREATE INDEX IF NOT EXISTS idx_messages_reply
  ON messages(reply_to_message_id) WHERE reply_to_message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_received ON messages(received_at);

-- 消息段（完整结构，绝不降级为纯文本）
CREATE TABLE IF NOT EXISTS message_segments (
  id           INTEGER PRIMARY KEY,
  message_ref  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  seg_index    INTEGER NOT NULL,
  seg_type     TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  text         TEXT,
  UNIQUE (message_ref, seg_index)
);
CREATE INDEX IF NOT EXISTS idx_segments_type ON message_segments(seg_type);

-- @ 关系（从 At 段确定性抽取）
CREATE TABLE IF NOT EXISTS message_mentions (
  id            INTEGER PRIMARY KEY,
  message_ref   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,
  target_raw_id TEXT,
  target_ref    INTEGER REFERENCES members(id),
  UNIQUE (message_ref, kind, target_raw_id)
);
CREATE INDEX IF NOT EXISTS idx_mentions_target ON message_mentions(target_raw_id);

-- 插件运行状态
CREATE TABLE IF NOT EXISTS plugin_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
