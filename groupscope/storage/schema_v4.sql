-- GroupScope schema v4（Phase 4 — 群体记忆）
-- 原则：话题/事件是**推断**，必须能回溯到消息证据；梗的演变按角色标注（首现/传播/演变）

-- 话题：一段持续出现的讨论主题
CREATE TABLE IF NOT EXISTS topics (
  id              INTEGER PRIMARY KEY,
  group_ref       INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  title           TEXT NOT NULL,
  summary         TEXT,
  keywords_json   TEXT,
  first_seen      INTEGER NOT NULL,
  last_seen       INTEGER NOT NULL,
  mention_count   INTEGER NOT NULL DEFAULT 0,
  source          TEXT NOT NULL,             -- rule / llm
  confidence      REAL NOT NULL DEFAULT 1.0,
  prompt_version  TEXT,
  reviewed        INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  UNIQUE (group_ref, title, source)
);
CREATE INDEX IF NOT EXISTS idx_topics_group ON topics(group_ref, last_seen);

-- 话题 ↔ 消息证据
CREATE TABLE IF NOT EXISTS topic_mentions (
  id           INTEGER PRIMARY KEY,
  topic_id     INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
  message_ref  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  weight       REAL NOT NULL DEFAULT 1.0,
  created_at   INTEGER NOT NULL,
  UNIQUE (topic_id, message_ref)
);
CREATE INDEX IF NOT EXISTS idx_topic_mentions_msg ON topic_mentions(message_ref);

-- 群体事件 / 梗：首现 → 传播 → 演变
CREATE TABLE IF NOT EXISTS events (
  id              INTEGER PRIMARY KEY,
  group_ref       INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  title           TEXT NOT NULL,
  summary         TEXT,
  event_kind      TEXT NOT NULL DEFAULT 'event',  -- meme / event / dispute / routine
  first_seen      INTEGER NOT NULL,
  last_seen       INTEGER NOT NULL,
  peak_at         INTEGER,
  mention_count   INTEGER NOT NULL DEFAULT 0,
  source          TEXT NOT NULL,                  -- rule / llm
  confidence      REAL NOT NULL DEFAULT 1.0,
  prompt_version  TEXT,
  reviewed        INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  UNIQUE (group_ref, title, event_kind, source)
);
CREATE INDEX IF NOT EXISTS idx_events_group ON events(group_ref, last_seen);

-- 事件证据（role: origin=首现 / spread=传播 / evolve=演变）
CREATE TABLE IF NOT EXISTS event_evidence (
  id           INTEGER PRIMARY KEY,
  event_id     INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  message_ref  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  role         TEXT NOT NULL DEFAULT 'spread',
  note         TEXT,
  created_at   INTEGER NOT NULL,
  UNIQUE (event_id, message_ref, role)
);
CREATE INDEX IF NOT EXISTS idx_event_evidence_msg ON event_evidence(message_ref);

-- 时间线（供 WebUI 事件轴渲染；由事件表派生）
CREATE TABLE IF NOT EXISTS memory_timeline (
  id           INTEGER PRIMARY KEY,
  group_ref    INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  item_kind    TEXT NOT NULL,        -- topic / event
  item_id      INTEGER NOT NULL,
  occurred_at  INTEGER NOT NULL,
  label        TEXT NOT NULL,
  detail       TEXT,
  UNIQUE (group_ref, item_kind, item_id, occurred_at)
);
CREATE INDEX IF NOT EXISTS idx_timeline_group ON memory_timeline(group_ref, occurred_at);
