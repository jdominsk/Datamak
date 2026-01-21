#!/usr/bin/env bash
set -euo pipefail

db_path="${1:-batch_database.db}"

sqlite3 "$db_path" <<'SQL'
CREATE TABLE IF NOT EXISTS gk_run (
  id INTEGER PRIMARY KEY,
  gk_input_id INTEGER,
  input_folder TEXT,
  job_folder TEXT,
  archive_folder TEXT,
  input_name TEXT,
  nb_nodes INTEGER,
  status TEXT,
  nb_restart INTEGER NOT NULL DEFAULT 0
);
SQL

echo "Created/verified $db_path with table gk_run."
