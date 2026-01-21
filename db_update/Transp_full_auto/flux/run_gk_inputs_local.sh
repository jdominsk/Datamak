#!/bin/bash
set -euo pipefail

DB_PATH="${1:?Usage: run_gk_inputs_local.sh /path/to/flux_equil_inputs.db [total_rows] [chunk_rows]}"
TOTAL_ROWS="${2:-1000}"
CHUNK_ROWS="${3:-100}"
MAX_MEM_GB="${MAX_MEM_GB:-4}"
PYTHON_BIN="${PYTHON_BIN:-/u/jdominsk/pyrokinetics/.venv/bin/python}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

total_done=0
while [ "${total_done}" -lt "${TOTAL_ROWS}" ]; do
  remaining=$((TOTAL_ROWS - total_done))
  if [ "${remaining}" -lt "${CHUNK_ROWS}" ]; then
    chunk="${remaining}"
  else
    chunk="${CHUNK_ROWS}"
  fi

  pending=$(sqlite3 "${DB_PATH}" \
    "SELECT COUNT(*) FROM gk_input WHERE status='NEW' AND (content IS NULL OR content='');")
  if [ "${pending}" -eq 0 ]; then
    echo "No NEW gk_input rows found; stopping."
    break
  fi

  echo "Running chunk ${chunk} (pending=${pending}, done=${total_done}/${TOTAL_ROWS})"
  "${PYTHON_BIN}" \
    "${SCRIPT_DIR}/run_flux_gk_inputs.py" \
    --db "${DB_PATH}" \
    --max-rows "${chunk}" \
    --batch-size "${chunk}" \
    --max-mem-gb "${MAX_MEM_GB}"

  total_done=$((total_done + chunk))
done

echo "Done. Generated up to ${total_done} rows."
