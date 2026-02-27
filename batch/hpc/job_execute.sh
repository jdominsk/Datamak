#!/bin/bash

set -euo pipefail

ANALYZE_ONLY=0
FORCE_RUNNING_TORUN=0
ANALYZE_ALL_NC=0
DB_PATH=""
NODES="${NODES:-4}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --analyze-only)
      ANALYZE_ONLY=1
      shift
      ;;
    --force-running-torun)
      FORCE_RUNNING_TORUN=1
      shift
      ;;
    --analyze-all-nc)
      ANALYZE_ALL_NC=1
      shift
      ;;
    *)
      if [[ -z "$DB_PATH" ]]; then
        DB_PATH="$1"
      else
        NODES="$1"
      fi
      shift
      ;;
  esac
done

if [[ -z "$DB_PATH" ]]; then
  newest_db="$(ls -t "$PWD"/batch_database_*.db 2>/dev/null | head -n 1 || true)"
  if [[ -z "$newest_db" ]]; then
    echo "Batch database not found in $PWD"
    exit 1
  fi
  DB_PATH="$newest_db"
fi

if [[ -f "$HOME/GX/gx_next6/module.sh" ]]; then
  # Match the GX module environment for analysis runs.
  source "$HOME/GX/gx_next6/module.sh"
fi
TOTAL_GPUS=$((NODES * 4))
OUTPUT_DIR="${OUTPUT_DIR:-$PWD}"

echo "Using DB: $DB_PATH"
echo "Working dir: $PWD"
echo "Nodes: $NODES  Total GPUs: $TOTAL_GPUS"

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
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
    if "gk_batch_id" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN gk_batch_id INTEGER NOT NULL DEFAULT 0")
    if "synced" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN synced INTEGER NOT NULL DEFAULT 0")
    if "t_max_initial" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN t_max_initial REAL NOT NULL DEFAULT 0")
    if "t_max" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN t_max REAL NOT NULL DEFAULT 0")
    if "nb_restart" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN nb_restart INTEGER NOT NULL DEFAULT 0")
    if "restart_keep_tmax" not in columns:
        conn.execute(
            "ALTER TABLE gk_run ADD COLUMN restart_keep_tmax INTEGER NOT NULL DEFAULT 0"
        )
    conn.commit()
finally:
    conn.close()
PY

if [[ $FORCE_RUNNING_TORUN -eq 1 ]]; then
  python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys
import re

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    rows = conn.execute(
        "SELECT id, input_content FROM gk_run WHERE status = 'RUNNING'"
    ).fetchall()
    updated = 0
    for run_id, content in rows:
        lines = content.splitlines()
        restart_re = re.compile(r"^(\s*restart\s*=\s*)(.+)$", re.IGNORECASE)
        restart_updated = False
        for i, line in enumerate(lines):
            m = restart_re.match(line)
            if m:
                lines[i] = f"{m.group(1)}true"
                restart_updated = True
                break
        if not restart_updated:
            lines.append("restart = true")
        new_content = "\n".join(lines) + "\n"
        conn.execute(
            "UPDATE gk_run "
            "SET status = 'TORUN', input_content = ?, synced = 0, nb_restart = nb_restart + 1 "
            "WHERE id = ?",
            (new_content, run_id),
        )
        updated += 1
    conn.commit()
    print(f"Force-updated RUNNING -> TORUN: {updated}")
finally:
    conn.close()
PY
fi

analyze_jobs="$(python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, input_name FROM gk_run WHERE status = 'SUCCESS' ORDER BY id"
    ).fetchall()
    for row in rows:
        run_id = row["id"]
        input_name = row["input_name"] or ""
        print(f"{run_id}\t{input_name}")
finally:
    conn.close()
PY
)"

run_gx_analyze() {
  local jobs="$1"
  if [[ -z "$jobs" ]]; then
    return 0
  fi
  echo "Analyzing SUCCESS rows."
  while IFS=$'\t' read -r run_id input_name; do
    if [[ -z "$input_name" ]]; then
      echo "gx_analyze skipped for run_id=$run_id; missing input_name."
      continue
    fi
    nc_path="${OUTPUT_DIR}/${input_name%.in}.out.nc"
    if [[ -f "$nc_path" ]]; then
      echo "Running gx_analyze on $nc_path"
      python3 "$(dirname "$0")/gx_analyze.py" "$DB_PATH" "$run_id" "$nc_path" --save-plot || \
        echo "gx_analyze failed for run_id=$run_id"
      if [[ -f "$(dirname "$0")/ky_growth_rates.py" ]]; then
        python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save || \
          echo "ky_growth_rates failed for run_id=$run_id"
      fi
    else
      echo "gx_analyze skipped; output not found at $nc_path"
    fi
  done <<<"$jobs"
}

run_gx_analyze "$analyze_jobs"

if [[ $ANALYZE_ONLY -eq 1 && $ANALYZE_ALL_NC -eq 1 ]]; then
  shopt -s nullglob
  nc_files=("${OUTPUT_DIR}"/*.out.nc)
  shopt -u nullglob
  if [[ ${#nc_files[@]} -eq 0 ]]; then
    echo "No .out.nc files found in ${OUTPUT_DIR}."
  else
    echo "Analyzing all .out.nc files in ${OUTPUT_DIR}."
    for nc_path in "${nc_files[@]}"; do
      run_id="0"
      if [[ "$nc_path" =~ input_batchid[0-9]+_gkinputid[0-9]+_runid([0-9]+)\.out\.nc$ ]]; then
        run_id="${BASH_REMATCH[1]}"
      fi
      echo "Running gx_analyze on $nc_path (run_id=$run_id)"
      if [[ "$run_id" == "0" ]]; then
        python3 "$(dirname "$0")/gx_analyze.py" "$DB_PATH" "$run_id" "$nc_path" --save-plot --no-db || \
          echo "gx_analyze failed for $nc_path"
      else
        python3 "$(dirname "$0")/gx_analyze.py" "$DB_PATH" "$run_id" "$nc_path" --save-plot || \
          echo "gx_analyze failed for $nc_path"
      fi
      if [[ -f "$(dirname "$0")/ky_growth_rates.py" ]]; then
        python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save || \
          echo "ky_growth_rates failed for $nc_path"
      fi
    done
  fi
  exit 0
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
    torun = conn.execute(
        "SELECT COUNT(*) FROM gk_run WHERE status = 'TORUN'"
    ).fetchone()[0]
    running = conn.execute(
        "SELECT COUNT(*) FROM gk_run WHERE status = 'RUNNING'"
    ).fetchone()[0]
    restart = conn.execute(
        "SELECT COUNT(*) FROM gk_run WHERE status = 'RESTART'"
    ).fetchone()[0]
    print(f"gk_run rows: TORUN={torun}, RUNNING={running}, RESTART={restart}")
finally:
    conn.close()
PY

while true; do
  analyze_state="$(python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    torun = conn.execute(
        "SELECT COUNT(*) FROM gk_run WHERE status = 'TORUN'"
    ).fetchone()[0]
    success = conn.execute(
        "SELECT COUNT(*) FROM gk_run WHERE status = 'SUCCESS'"
    ).fetchone()[0]
    if torun == 0 and success > 0:
        print("ANALYZE")
finally:
    conn.close()
PY
)"
  if [[ "$analyze_state" == "ANALYZE" ]]; then
    analyze_jobs="$(python3 - "$DB_PATH" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, input_name FROM gk_run WHERE status = 'SUCCESS' ORDER BY id"
    ).fetchall()
    for row in rows:
        run_id = row["id"]
        input_name = row["input_name"] or ""
        print(f"{run_id}\t{input_name}")
finally:
    conn.close()
PY
)"
    run_gx_analyze "$analyze_jobs"
    continue
  fi

  result="$(python3 - "$DB_PATH" "$OUTPUT_DIR" <<'PY'
import os
import re
import sqlite3
import sys

db_path = sys.argv[1]
output_dir = sys.argv[2]

def update_restart_input(content: str) -> str:
    lines = content.splitlines()
    restart_updated = False
    restart_re = re.compile(r"^(\s*restart\s*=\s*)(.+)$", re.IGNORECASE)
    for i, line in enumerate(lines):
        m = restart_re.match(line)
        if m:
            lines[i] = f"{m.group(1)}true"
            restart_updated = True
            continue
    if not restart_updated:
        lines.append("restart = true")
    return "\n".join(lines) + "\n"

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
try:
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT id, gk_input_id, gk_batch_id, input_content, t_max_initial, t_max, restart_keep_tmax "
        "FROM gk_run WHERE status = 'TORUN' ORDER BY id LIMIT 1"
    ).fetchone()
    if row is None:
        restart_rows = conn.execute(
            "SELECT id, input_content "
            "FROM gk_run WHERE status = 'RESTART' ORDER BY id"
        ).fetchall()
        if restart_rows:
            for run_id, content in restart_rows:
                updated_content = update_restart_input(content)
                conn.execute(
                    "UPDATE gk_run SET input_content = ?, status = 'TORUN', synced = 0 WHERE id = ?",
                    (updated_content, int(run_id)),
                )
            row = conn.execute(
                "SELECT id, gk_input_id, gk_batch_id, input_content, t_max_initial, t_max "
                "FROM gk_run WHERE status = 'TORUN' ORDER BY id LIMIT 1"
            ).fetchone()
    if row is None:
        print("NO_TORUN")
        conn.commit()
        sys.exit(2)
    run_id = int(row["id"])
    gk_input_id = int(row["gk_input_id"]) if row["gk_input_id"] is not None else 0
    gk_batch_id = int(row["gk_batch_id"]) if row["gk_batch_id"] is not None else 0
    content = row["input_content"]
    tmax_initial = float(row["t_max_initial"] or 0)
    tmax_current = float(row["t_max"] or 0)

    tmax_re = re.compile(r"(\bt_max\s*=\s*)([-+0-9.eE]+)", re.IGNORECASE)

    match = tmax_re.search(content)
    if match is None:
        raise ValueError("t_max not found in input_content for initial capture.")
    tmax_in_content = float(match.group(2))
    if tmax_initial == 0:
        tmax_initial = tmax_in_content
    if tmax_current == 0:
        tmax_current = tmax_in_content
    else:
        tmax_current = max(tmax_current, tmax_in_content)

    restart_true = bool(re.search(r"^\s*restart\s*=\s*true\b", content, re.IGNORECASE | re.MULTILINE))
    keep_tmax = int(row["restart_keep_tmax"] or 0)
    if restart_true and not keep_tmax:
        tmax_current += tmax_initial
    tmax_str = f"{tmax_current:.1f}"
    def repl(m):
        return f"{m.group(1)}{tmax_str}"

    content, count = tmax_re.subn(repl, content, count=1)
    if count == 0:
        raise ValueError("t_max not found in input_content for update.")

    filename = f"input_batchid{gk_batch_id}_gkinputid{gk_input_id}_runid{run_id}.in"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8") as handle:
        handle.write(content)
    conn.execute(
        "UPDATE gk_run "
        "SET status = 'RUNNING', input_name = ?, job_id = ?, synced = 0, "
        "input_content = ?, t_max_initial = ?, t_max = ?, restart_keep_tmax = 0 "
        "WHERE id = ?",
        (
            filename,
            os.environ.get("SLURM_JOB_ID", ""),
            content,
            tmax_initial,
            tmax_current,
            run_id,
        ),
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
  echo "Launching run_id=$run_id with input $GX_INPUT"

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

  if [[ $ANALYZE_ONLY -eq 1 ]]; then
    echo "Analyze-only mode; skipping srun for run_id=$run_id."
    srun_status=0
  else
    echo "Using ntasks-per-node=${tasks_per_node}"
    log_base="${OUTPUT_DIR}/${GX_INPUT##*/}"
    log_base="${log_base%.in}"
    stdout_log="${log_base}.log"
    stderr_log="${log_base}.err"
    set +e
    srun --nodes="${NODES}" --ntasks-per-node="${tasks_per_node}" --gpus="${TOTAL_GPUS}" --gpus-per-node=4 \
      --output="${stdout_log}" --error="${stderr_log}" \
      "${GX_PATH}/gx" "${GX_INPUT}"
    srun_status=$?
    set -e
  fi

  if [[ $srun_status -ne 0 ]]; then
    status="CRASHED"
    if [[ -f "$stderr_log" ]]; then
      if grep -qiE "time limit|due to time limit|cancelled at|job .*cancelled" "$stderr_log"; then
        status="INTERRUPTED"
      fi
    fi
    if [[ "$srun_status" -eq 137 || "$srun_status" -eq 143 ]]; then
      status="INTERRUPTED"
    fi
    echo "srun failed for run_id=$run_id (exit $srun_status); marking ${status}."
    python3 - "$DB_PATH" "$run_id" "$status" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])
status = sys.argv[3]

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "UPDATE gk_run SET status = ? WHERE id = ?",
        (status, run_id),
    )
    conn.commit()
finally:
    conn.close()
PY
    continue
  fi

  echo "srun completed for run_id=$run_id; marking SUCCESS."
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

  nc_path="${OUTPUT_DIR}/${input_path##*/}"
  nc_path="${nc_path%.in}.out.nc"
  if [[ -f "$nc_path" ]]; then
    echo "Running gx_analyze on $nc_path"
    python3 "$(dirname "$0")/gx_analyze.py" "$DB_PATH" "$run_id" "$nc_path" --save-plot || \
      echo "gx_analyze failed for run_id=$run_id"
    if [[ -f "$(dirname "$0")/ky_growth_rates.py" ]]; then
      python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save || \
        echo "ky_growth_rates failed for run_id=$run_id"
    fi
  else
    echo "gx_analyze skipped; output not found at $nc_path"
  fi
done
