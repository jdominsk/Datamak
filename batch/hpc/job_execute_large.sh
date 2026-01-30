#!/bin/bash

set -euo pipefail

ANALYZE_ONLY=0
FORCE_RUNNING_TORUN=0
ANALYZE_ALL_NC=0
DB_PATH=""
NODES="${NODES:-4}"
WORKERS="${WORKERS:-1}"
MIN_TIME_LEFT_SEC="${MIN_TIME_LEFT_SEC:-600}"
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
    --workers)
      WORKERS="$2"
      shift 2
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

parse_slurm_time() {
  local value="$1"
  if [[ -z "$value" || "$value" == "UNLIMITED" || "$value" == "NOT_SET" ]]; then
    echo "-1"
    return 0
  fi
  local days=0
  local time_part="$value"
  if [[ "$value" == *-* ]]; then
    days="${value%%-*}"
    time_part="${value#*-}"
  fi
  local h=0 m=0 s=0
  IFS=':' read -r a b c <<<"$time_part"
  if [[ -n "$c" ]]; then
    h="$a"
    m="$b"
    s="$c"
  else
    m="$a"
    s="$b"
  fi
  echo $((days*86400 + h*3600 + m*60 + s))
}

time_left_sec() {
  if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "-1"
    return 0
  fi
  local remaining
  remaining="$(squeue -h -j "$SLURM_JOB_ID" -o %L 2>/dev/null | head -n 1 || true)"
  if [[ -z "$remaining" ]]; then
    echo "-1"
    return 0
  fi
  parse_slurm_time "$remaining"
}

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
if [[ $ANALYZE_ONLY -eq 1 ]]; then
  NODES=1
  WORKERS=1
  TOTAL_GPUS=0
fi
echo "Nodes: $NODES  Total GPUs: $TOTAL_GPUS"
echo "Workers: $WORKERS"

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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_scheduler (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_run_id INTEGER NOT NULL,
            worker_id INTEGER NOT NULL,
            event TEXT NOT NULL,
            nb_restart INTEGER NOT NULL DEFAULT 0,
            t_max_initial REAL,
            t_max REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    scheduler_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(gk_scheduler)").fetchall()
    }
    if "nb_restart" not in scheduler_columns:
        conn.execute(
            "ALTER TABLE gk_scheduler ADD COLUMN nb_restart INTEGER NOT NULL DEFAULT 0"
        )
    if "t_max_initial" not in scheduler_columns:
        conn.execute("ALTER TABLE gk_scheduler ADD COLUMN t_max_initial REAL")
    if "t_max" not in scheduler_columns:
        conn.execute("ALTER TABLE gk_scheduler ADD COLUMN t_max REAL")
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
        ky_log="${nc_path%.out.nc}.ky.log"
        ky_err="${nc_path%.out.nc}.ky.err"
        python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save >>"$ky_log" 2>>"$ky_err" || \
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
        ky_log="${nc_path%.out.nc}.ky.log"
        ky_err="${nc_path%.out.nc}.ky.err"
        python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save >>"$ky_log" 2>>"$ky_err" || \
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

if [[ "$WORKERS" -lt 1 ]]; then
  echo "Workers must be >= 1."
  exit 1
fi
if (( NODES % WORKERS != 0 )); then
  echo "Nodes ($NODES) must be divisible by workers ($WORKERS)."
  exit 1
fi
NODES_PER_WORKER=$((NODES / WORKERS))

run_worker() {
  local worker_id="$1"
  local nodes_per_worker="$2"
  local gpus_per_worker=$((nodes_per_worker * 4))
  while true; do
    remaining_sec="$(time_left_sec)"
    if [[ "$remaining_sec" -ge 0 && "$remaining_sec" -le "$MIN_TIME_LEFT_SEC" ]]; then
      echo "Worker ${worker_id}: only ${remaining_sec}s left in allocation; stopping new launches."
      break
    fi
    result="$(python3 "$(dirname "$0")/claim_next_run.py" "$DB_PATH" "$OUTPUT_DIR")"
    if [[ "$result" == "NO_TORUN" || -z "$result" ]]; then
      echo "Worker ${worker_id}: no TORUN entries remaining."
      break
    fi

    IFS=$'\t' read -r run_id input_path <<<"$result"
    if [[ -z "$run_id" || -z "$input_path" ]]; then
      echo "Worker ${worker_id}: failed to parse run id and input path."
      break
    fi

    info="$(python3 - "$DB_PATH" "$run_id" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])

conn = sqlite3.connect(db_path)
try:
    row = conn.execute(
        "SELECT nb_restart, t_max_initial, t_max FROM gk_run WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        print("0\t0\t0")
    else:
        nb_restart = int(row[0] or 0)
        t_max_initial = float(row[1] or 0)
        t_max = float(row[2] or 0)
        print(f"{nb_restart}\t{t_max_initial}\t{t_max}")
finally:
    conn.close()
PY
)"
    IFS=$'\t' read -r nb_restart t_max_initial t_max <<<"$info"
    is_restart=0
    if [[ "${nb_restart:-0}" -gt 0 ]]; then
      is_restart=1
    fi

    export GX_INPUT="$input_path"
    echo "Worker ${worker_id}: launching run_id=$run_id (restart=$is_restart, t_max=${t_max}) with input $GX_INPUT"
    python3 - "$DB_PATH" "$run_id" "$worker_id" "${nb_restart:-0}" "${t_max_initial:-0}" "${t_max:-0}" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])
worker_id = int(sys.argv[3])
nb_restart = int(float(sys.argv[4]))
t_max_initial = float(sys.argv[5])
t_max = float(sys.argv[6])

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "INSERT INTO gk_scheduler (gk_run_id, worker_id, event, nb_restart, t_max_initial, t_max) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, worker_id, "START", nb_restart, t_max_initial, t_max),
    )
    conn.commit()
finally:
    conn.close()
PY

    tasks_per_node="$(python3 - "$GX_INPUT" "$nodes_per_worker" <<'PY'
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
      echo "Worker ${worker_id}: analyze-only mode; skipping srun for run_id=$run_id."
      srun_status=0
    else
      start_ts="$(date '+%Y-%m-%d %H:%M:%S')"
      echo "Worker ${worker_id}: starting srun for run_id=$run_id (t_max=${t_max}, nb_restart=${nb_restart}, ntasks-per-node=${tasks_per_node}, time=${start_ts})"
      log_base="${OUTPUT_DIR}/${input_path##*/}"
      log_base="${log_base%.in}"
      stdout_log="${log_base}.log"
      stderr_log="${log_base}.err"
      set +e
      srun --exclusive --nodes="${nodes_per_worker}" --ntasks-per-node="${tasks_per_node}" \
        --gpus="${gpus_per_worker}" --gpus-per-node=4 \
        --output="${stdout_log}" --error="${stderr_log}" --open-mode=append \
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
        if grep -qiE "job/step already completing|job step already completing|job step already completed" "$stderr_log"; then
          echo "Worker ${worker_id}: allocation is completing; stopping new launches."
          status="INTERRUPTED"
        fi
      fi
      if [[ "$srun_status" -eq 137 || "$srun_status" -eq 143 ]]; then
        status="INTERRUPTED"
      fi
      echo "Worker ${worker_id}: srun failed for run_id=$run_id (exit $srun_status); marking ${status}."
      python3 - "$DB_PATH" "$run_id" "$status" "$worker_id" "${nb_restart:-0}" "${t_max_initial:-0}" "${t_max:-0}" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])
status = sys.argv[3]
worker_id = int(sys.argv[4])
nb_restart = int(float(sys.argv[5]))
t_max_initial = float(sys.argv[6])
t_max = float(sys.argv[7])

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "UPDATE gk_run SET status = ? WHERE id = ?",
        (status, run_id),
    )
    conn.execute(
        "INSERT INTO gk_scheduler (gk_run_id, worker_id, event, nb_restart, t_max_initial, t_max) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, worker_id, status, nb_restart, t_max_initial, t_max),
    )
    conn.commit()
finally:
    conn.close()
PY
      if [[ -f "$stderr_log" ]] && grep -qiE "job/step already completing|job step already completing|job step already completed" "$stderr_log"; then
        break
      fi
      continue
    fi

    echo "Worker ${worker_id}: srun completed for run_id=$run_id; marking SUCCESS."
    python3 - "$DB_PATH" "$run_id" "$worker_id" "${nb_restart:-0}" "${t_max_initial:-0}" "${t_max:-0}" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
run_id = int(sys.argv[2])
worker_id = int(sys.argv[3])
nb_restart = int(float(sys.argv[4]))
t_max_initial = float(sys.argv[5])
t_max = float(sys.argv[6])

conn = sqlite3.connect(db_path)
try:
    conn.execute(
        "UPDATE gk_run SET status = 'SUCCESS' WHERE id = ?",
        (run_id,),
    )
    conn.execute(
        "INSERT INTO gk_scheduler (gk_run_id, worker_id, event, nb_restart, t_max_initial, t_max) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, worker_id, "SUCCESS", nb_restart, t_max_initial, t_max),
    )
    conn.commit()
finally:
    conn.close()
PY

    nc_path="${OUTPUT_DIR}/${input_path##*/}"
    nc_path="${nc_path%.in}.out.nc"
    if [[ -f "$nc_path" ]]; then
      echo "Worker ${worker_id}: running gx_analyze on $nc_path"
      python3 "$(dirname "$0")/gx_analyze.py" "$DB_PATH" "$run_id" "$nc_path" --save-plot || \
        echo "gx_analyze failed for run_id=$run_id"
      if [[ -f "$(dirname "$0")/ky_growth_rates.py" ]]; then
        ky_log="${nc_path%.out.nc}.ky.log"
        ky_err="${nc_path%.out.nc}.ky.err"
        python3 "$(dirname "$0")/ky_growth_rates.py" "$nc_path" --save >>"$ky_log" 2>>"$ky_err" || \
          echo "ky_growth_rates failed for run_id=$run_id"
      fi
    else
      echo "Worker ${worker_id}: gx_analyze skipped; output not found at $nc_path"
    fi
  done
}

echo "Launching ${WORKERS} workers with ${NODES_PER_WORKER} nodes each."
workers=()
for worker_id in $(seq 1 "$WORKERS"); do
  run_worker "$worker_id" "$NODES_PER_WORKER" &
  workers+=("$!")
done

for pid in "${workers[@]}"; do
  wait "$pid"
done
