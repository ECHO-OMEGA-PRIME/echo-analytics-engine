-- echo-analytics-engine schema
-- D1 SQLite

CREATE TABLE IF NOT EXISTS metrics_snapshots (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp   TEXT    NOT NULL DEFAULT (datetime('now')),
  source      TEXT    NOT NULL,
  metric_name TEXT    NOT NULL,
  metric_value REAL   NOT NULL,
  metadata    TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_snapshots_ts     ON metrics_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_source ON metrics_snapshots(source);
CREATE INDEX IF NOT EXISTS idx_snapshots_name   ON metrics_snapshots(metric_name);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS daily_rollups (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  date        TEXT    NOT NULL,
  source      TEXT    NOT NULL,
  metric_name TEXT    NOT NULL,
  min_val     REAL    NOT NULL DEFAULT 0,
  max_val     REAL    NOT NULL DEFAULT 0,
  avg_val     REAL    NOT NULL DEFAULT 0,
  count       INTEGER NOT NULL DEFAULT 0,
  metadata    TEXT    NOT NULL DEFAULT '{}',
  UNIQUE(date, source, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_rollups_date   ON daily_rollups(date);
CREATE INDEX IF NOT EXISTS idx_rollups_source ON daily_rollups(source);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS growth_tracking (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  date             TEXT    NOT NULL UNIQUE,
  engines_count    INTEGER NOT NULL DEFAULT 0,
  doctrines_count  INTEGER NOT NULL DEFAULT 0,
  knowledge_docs   INTEGER NOT NULL DEFAULT 0,
  brain_messages   INTEGER NOT NULL DEFAULT 0,
  brain_instances  INTEGER NOT NULL DEFAULT 0,
  fleet_score      REAL    NOT NULL DEFAULT 0,
  bot_posts_total  INTEGER NOT NULL DEFAULT 0,
  queries_total    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_growth_date ON growth_tracking(date);
