-- GroupScope schema v2（Phase 2 — 人物与关系）
-- 原则：
--   * 只记录**确定性观察事实**（@ / 回复 / 引用），不做亲密度推断
--   * 关系有向：from_member -> to_member，带时间窗口
--   * 身份默认不跨平台、不跨账号合并；合并必须是显式人工操作且留审计

-- 确定性互动记录（@ / 回复 / 引用）
CREATE TABLE IF NOT EXISTS interactions (
  id           INTEGER PRIMARY KEY,
  group_ref    INTEGER REFERENCES groups(id) ON DELETE CASCADE,
  from_member  INTEGER REFERENCES members(id) ON DELETE SET NULL,
  from_raw_id  TEXT,
  to_member    INTEGER REFERENCES members(id) ON DELETE SET NULL,
  to_raw_id    TEXT,
  kind         TEXT NOT NULL,          -- at / reply / quote
  message_ref  INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_inter_from_to  ON interactions(from_member, to_member, created_at);
CREATE INDEX IF NOT EXISTS idx_inter_to       ON interactions(to_member, created_at);
CREATE INDEX IF NOT EXISTS idx_inter_group    ON interactions(group_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_inter_message  ON interactions(message_ref);
CREATE INDEX IF NOT EXISTS idx_inter_kind     ON interactions(kind, created_at);

-- 关系窗口快照（按日/周/月/季聚合，只存计数与证据量，不存“亲密分”）
CREATE TABLE IF NOT EXISTS relationship_snapshots (
  id              INTEGER PRIMARY KEY,
  group_ref       INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  window          TEXT NOT NULL,        -- day / week / month / quarter
  window_start    INTEGER NOT NULL,
  window_end      INTEGER NOT NULL,
  from_member     INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  to_member       INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  at_count        INTEGER NOT NULL DEFAULT 0,
  reply_count     INTEGER NOT NULL DEFAULT 0,
  quote_count     INTEGER NOT NULL DEFAULT 0,
  -- 背景量：双方在该窗口的发言数（仅作参照，禁止据此排序“亲密度”）
  from_msgs       INTEGER NOT NULL DEFAULT 0,
  to_msgs         INTEGER NOT NULL DEFAULT 0,
  evidence_count  INTEGER NOT NULL DEFAULT 0,
  confidence      REAL NOT NULL DEFAULT 1.0,   -- 证据量折算的强度，非情感值
  computed_at     INTEGER NOT NULL,
  UNIQUE (group_ref, window, window_start, from_member, to_member)
);
CREATE INDEX IF NOT EXISTS idx_snap_window ON relationship_snapshots(window, window_start);

-- 人工身份合并审计（默认不合并；每次合并必须留痕，可回溯）
CREATE TABLE IF NOT EXISTS member_merges (
  id          INTEGER PRIMARY KEY,
  src_member  INTEGER NOT NULL,
  dst_member  INTEGER NOT NULL,
  operator    TEXT,
  note        TEXT,
  created_at  INTEGER NOT NULL,
  undone_at   INTEGER
);

-- 人工别名（管理员显式指定的称呼，优先于观察到的昵称）
CREATE TABLE IF NOT EXISTS member_manual_aliases (
  id         INTEGER PRIMARY KEY,
  member_id  INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  operator   TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE (member_id, alias)
);
