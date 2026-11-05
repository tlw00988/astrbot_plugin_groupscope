-- GroupScope schema v3（Phase 3 — 称呼与语义分析）
-- 原则：
--   * AI 产出与观察事实分离：source='llm' 的行永远带 prompt_version 与 confidence
--   * 每条推断保留证据消息，可回跳原消息上下文
--   * 任务队列本地落盘，可断点续跑、可重放

-- 称呼关系：谁用什么称呼叫谁
CREATE TABLE IF NOT EXISTS addressing (
  id                INTEGER PRIMARY KEY,
  group_ref         INTEGER REFERENCES groups(id) ON DELETE CASCADE,
  from_member       INTEGER REFERENCES members(id) ON DELETE SET NULL,
  from_raw_id       TEXT,
  to_member         INTEGER REFERENCES members(id) ON DELETE SET NULL,
  to_raw_id         TEXT,
  address           TEXT NOT NULL,          -- 被使用的称呼原文
  source            TEXT NOT NULL,          -- rule / llm / manual
  confidence        REAL NOT NULL DEFAULT 1.0,
  evidence_message  INTEGER REFERENCES messages(id) ON DELETE SET NULL,
  prompt_version    TEXT,
  created_at        INTEGER NOT NULL,
  last_seen         INTEGER NOT NULL,
  seen_count        INTEGER NOT NULL DEFAULT 1,
  reviewed          INTEGER NOT NULL DEFAULT 0
);
-- 用 IFNULL 表达式索引：from/to 可能为空（未识别到对象），SQLite 的 NULL
-- 在唯一约束里互不相等，直接用列会导致重复行。
CREATE UNIQUE INDEX IF NOT EXISTS ux_addressing
  ON addressing(group_ref, IFNULL(from_member,-1), IFNULL(to_member,-1), address, source);
CREATE INDEX IF NOT EXISTS idx_addressing_to     ON addressing(to_member, address);
CREATE INDEX IF NOT EXISTS idx_addressing_group  ON addressing(group_ref, last_seen);
CREATE INDEX IF NOT EXISTS idx_addressing_addr   ON addressing(address);

-- 分析任务队列（异步、可重试、可重新分析）
CREATE TABLE IF NOT EXISTS analysis_tasks (
  id              INTEGER PRIMARY KEY,
  kind            TEXT NOT NULL,            -- addressing / topic / event / snapshot
  dedup_key       TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'pending',  -- pending/running/done/failed/cancelled
  priority        INTEGER NOT NULL DEFAULT 0,
  payload_json    TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  last_error      TEXT,
  prompt_version  TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  claimed_at      INTEGER,
  UNIQUE (kind, dedup_key)
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON analysis_tasks(status, priority, created_at);

-- 分析批次：一次重新分析/一次 LLM 调用的留痕（提示词版本化）
CREATE TABLE IF NOT EXISTS analysis_runs (
  id              INTEGER PRIMARY KEY,
  kind            TEXT NOT NULL,
  prompt_version  TEXT NOT NULL,
  provider_id     TEXT,
  scope_json      TEXT,                      -- 分析范围（群/时间窗/消息区间）
  tasks_total     INTEGER NOT NULL DEFAULT 0,
  tasks_done      INTEGER NOT NULL DEFAULT 0,
  tasks_failed    INTEGER NOT NULL DEFAULT 0,
  started_at      INTEGER NOT NULL,
  finished_at     INTEGER,
  operator        TEXT
);
