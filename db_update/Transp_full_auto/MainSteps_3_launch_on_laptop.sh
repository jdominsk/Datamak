#!/bin/bash
set -euo pipefail

DTWIN_ROOT="${DTWIN_ROOT:-/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin}"
MAIN_DB="${DTWIN_ROOT}/gyrokinetic_simulations.db"
LOCAL_DIR="${DTWIN_ROOT}/transp_full_auto"
REMOTE_DIR="/u/jdominsk/DTwin/transp_full_auto"
REMOTE_HOST="jdominsk@flux"

FLUX_DB_LOCAL="${2:-}"
REMOTE_DB_PATH="${1:-}"
REMOTE_DB_HOST="${REMOTE_HOST}"
REMOTE_DB_DIR="${REMOTE_DIR}"
REMOTE_DB_NAME=""

if [[ -z "${REMOTE_DB_PATH}" ]]; then
  META=()
  while IFS= read -r line; do
    META+=("${line}")
  done < <(
    MAIN_DB="${MAIN_DB}" python3 - <<'PY'
import os
import sqlite3

db_path = os.environ["MAIN_DB"]
conn = sqlite3.connect(db_path)
try:
    row = conn.execute(
        """
        SELECT flux_db_name, remote_host, remote_dir
        FROM flux_action_log
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if row:
        print(row[0])
        print(row[1])
        print(row[2])
finally:
    conn.close()
PY
  )
  if [[ "${#META[@]}" -ge 3 && -n "${META[0]}" ]]; then
    REMOTE_DB_NAME="${META[0]}"
    REMOTE_DB_HOST="${META[1]}"
    REMOTE_DB_DIR="${META[2]}"
    REMOTE_DB_PATH="${REMOTE_DB_DIR}/${REMOTE_DB_NAME}"
  fi
fi

if [[ -z "${REMOTE_DB_NAME}" && -n "${REMOTE_DB_PATH}" ]]; then
  REMOTE_DB_NAME="$(basename "${REMOTE_DB_PATH}")"
fi

if [[ -z "${FLUX_DB_LOCAL}" ]]; then
  if [[ -n "${REMOTE_DB_NAME}" ]]; then
    FLUX_DB_LOCAL="${LOCAL_DIR}/${REMOTE_DB_NAME}"
  else
    FLUX_DB_LOCAL="$(ls -t "${LOCAL_DIR}/flux_equil_inputs_"*.db | head -n 1)"
  fi
fi

mkdir -p "${LOCAL_DIR}"

RSYNC_SSH_OPTS=(
  -o ControlMaster=auto
  -o ControlPersist=10m
  -o ControlPath="${HOME}/.ssh/cm-%r@%h:%p"
)
RSYNC_SSH="ssh ${RSYNC_SSH_OPTS[*]}"

# Open a control connection so you can complete 2FA once, then reuse it for rsync.
ssh "${RSYNC_SSH_OPTS[@]}" -t "${REMOTE_DB_HOST}" "true"
if [[ -n "${REMOTE_DB_NAME}" ]]; then
  rsync -av -e "$RSYNC_SSH" "${REMOTE_DB_HOST}:${REMOTE_DB_DIR}/${REMOTE_DB_NAME}" "${LOCAL_DIR}/"
else
  rsync -av -e "$RSYNC_SSH" "${REMOTE_DB_HOST}:${REMOTE_DB_DIR}/flux_equil_inputs_"*.db "${LOCAL_DIR}/"
fi

python3 "${DTWIN_ROOT}/db_update/Transp_full_auto/sync_flux_equil_inputs_to_main.py" \
  --main-db "${MAIN_DB}" \
  --flux-db "${FLUX_DB_LOCAL}"

python3 "${DTWIN_ROOT}/db_update/backfill_gk_input_physics.py" \
  --db "${MAIN_DB}"
