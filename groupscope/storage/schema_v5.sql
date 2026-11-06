-- GroupScope schema v5（Phase 6 — 长期运行：月度归档与调度调优）
-- 原则：
--   * 归档只搬原始消息；分析结论（话题/事件/称呼）保留在主库，
--     其消息级证据改记到 archived_evidence（指向归档库内消息 id），
--     因此“证据可回溯”在归档后依然成立；
--   * 主库删除消息依赖既有 ON DELETE CASCADE/SET NULL 约束，先存证再删。

-- 分析任务退避：此时间戳之前不认领（失败指数退避，避免坏任务空转）
ALTER TABLE analysis_tasks ADD COLUMN not_before INTEGER;

-- 月度归档清单
CREATE TABLE IF NOT EXISTS archive_manifest (
  id           INTEGER PRIMARY KEY,
  month        TEXT NOT NULL UNIQUE,      -- YYYY-MM
  file         TEXT NOT NULL,             -- archives/ 目录下的文件名
  messages     INTEGER NOT NULL DEFAULT 0,
  segments     INTEGER NOT NULL DEFAULT 0,
  mentions     INTEGER NOT NULL DEFAULT 0,
  purged       INTEGER NOT NULL DEFAULT 0, -- 是否已从主库移除
  sha256       TEXT,
  created_at   INTEGER NOT NULL,
  restored_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_archive_month ON archive_manifest(month);

-- 归档时被移走的证据指针（分析结论保留，证据位置改指到归档文件）
CREATE TABLE IF NOT EXISTS archived_evidence (
  id                 INTEGER PRIMARY KEY,
  archive_month      TEXT NOT NULL,
  item_kind          TEXT NOT NULL,   -- topic_mention / event_evidence / addressing
  item_id            INTEGER NOT NULL, -- 话题/事件/称呼记录的主键
  archive_message_id INTEGER NOT NULL, -- 归档库 messages.id
  role               TEXT,            -- 事件证据角色（origin/spread/evolve）
  note               TEXT,
  created_at         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_arch_evi_item
  ON archived_evidence(item_kind, item_id);
CREATE INDEX IF NOT EXISTS idx_arch_evi_month ON archived_evidence(archive_month);
CREATE UNIQUE INDEX IF NOT EXISTS ux_arch_evi
  ON archived_evidence(archive_month, item_kind, item_id, archive_message_id,
                       IFNULL(role, ''));
