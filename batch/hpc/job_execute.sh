#!/bin/bash

set -euo pipefail

DB_PATH="${1:-$PWD/batch_database_20260109_143859.db}"
NODES="${2:-${NODES:-4}}"
TOTAL_GPUS=$((NODES * 4))
OUTPUT_DIR="${OUTPUT_DIR:-$PWD}"

if [[ ! -f "$DB_PATH" ]]; then
  echo "Batch database not found: $DB_PATH"
  exit 1
fi

python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "gk_run" not in tables:
        raise SystemExit(f"Table gk_run not found in {db_path}")
finally:
    conn.close()
PY

while true; do
  result="$(python3 - "$DB_PATH" "$OUTPUT_DIR" <<'PY'
import os
import sqlite3
import sys

db_path = sys.argv[1]
output_dir = sys.argv[2]

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
try:
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT id, gk_input_id, input_content "
        "FROM gk_run WHERE status = 'TORUN' ORDER BY id LIMIT 1"
    ).fetchone()
    if row is None:
        print("NO_TORUN")
        conn.commit()
        sys.exit(2)
    run_id = int(row["id"])
    gk_input_id = int(row["gk_input_id"]) if row["gk_input_id"] is not None else 0
    filename = f"input_id{run_id}_gkinputid{gk_input_id}.in"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as handle:
        handle.write(row["input_content"])
    conn.execute(
        "UPDATE gk_run SET status = 'RUNNING', input_name = ?, job_id = ? WHERE id = ?",
        (filename, os.environ.get("SLURM_JOB_ID", ""), run_id),
    )
    conn.commit()
    print(f"{run_id}\t{filepath}")
finally:
    conn.close()
PY
)"

  if [[ "$result" == "NO_TORUN" || -z "$result" ]]; then
    echo "No TORUN entries found in $DB_PATH."
    break
  fi

  IFS=$'\t' read -r run_id input_path <<<"$result"
  if [[ -z "$run_id" || -z "$input_path" ]]; then
    echo "Failed to parse run id and input path from database."
    exit 1
  fi

  export GX_INPUT="$input_path"

  tasks_per_node="$(python3 - "$GX_INPUT" "$NODES" <<'PY'
import re
import sys

input_path = sys.argv[1]
nodes = int(sys.argv[2])
nspecies = None
pattern = re.compile(r"^\s*nspecies\s*=\s*(\d+)\s*$", re.IGNORECASE)
with open(input_path, "r", encoding="utf-8") as handle:
    for line in handle:
        match = pattern.match(line.strip())
        if match:
            nspecies = int(match.group(1))
            break

if nspecies is None or nspecies <= 0:
    print(4)
    sys.exit(0)

for tpn in (4, 3, 2, 1):
    if (nodes * tpn) % nspecies == 0:
        print(tpn)
        sys.exit(0)

print(1)
PY
)"

  set +e
  srun --nodes="${NODES}" --ntasks-per-node="${tasks_per_node}" --gpus="${TOTAL_GPUS}" --gpus-per-node=4 "${GX_PATH}/gx" "${GX_INPUT}"
  srun_status=$?
  set -e

  if [[ $srun_status -ne 0 ]]; then
    python3 - "$DB_PATH" "$run_id" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "UPDATE gk_run SET status = 'CRASHED' WHERE id = ?",
        (run_id,),
    )
    conn.commit()
finally:
    conn.close()
PY
    continue
  fi

  python3 - "$DB_PATH" "$run_id" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "UPDATE gk_run SET status = 'SUCCESS' WHERE id = ?",
        (run_id,),
    )
    conn.commit()
finally:
    conn.close()
PY
done
