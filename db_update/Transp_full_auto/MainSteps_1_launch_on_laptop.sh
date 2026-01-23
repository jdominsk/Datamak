#!/bin/bash
set -euo pipefail

DTWIN_ROOT="${DTWIN_ROOT:-/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin}"
LOCAL_DIR="${DTWIN_ROOT}/transp_full_auto"
MAIN_DB="${DTWIN_ROOT}/gyrokinetic_simulations.db"
REMOTE_DIR="/u/jdominsk/DTwin/transp_full_auto"
REMOTE_HOST="jdominsk@flux"
ORIGIN_NAME="${ORIGIN_NAME:-Alexei Transp 09 (full-auto)}"

mkdir -p "${LOCAL_DIR}/templates"

python3 "${DTWIN_ROOT}/db_update/Transp_full_auto/build_flux_equil_inputs.py" \
  --db "${MAIN_DB}" \
  --out-dir "${LOCAL_DIR}"

rsync -av "${DTWIN_ROOT}/pyrokinetics"/*.in "${LOCAL_DIR}/templates/"

rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/MainSteps_2_launch_on_flux.sh" "${LOCAL_DIR}/"
rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/build_flux_equil_inputs.py" "${LOCAL_DIR}/"
rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/flux" "${LOCAL_DIR}/"

RSYNC_SSH_OPTS=(
  -o ControlMaster=auto
  -o ControlPersist=10m
  -o ControlPath="${HOME}/.ssh/cm-%r@%h:%p"
)
RSYNC_SSH="ssh ${RSYNC_SSH_OPTS[*]}"

# Open a control connection so you can complete 2FA once, then reuse it for rsync.
ssh "${RSYNC_SSH_OPTS[@]}" -t "${REMOTE_HOST}" "true"
rsync -av -e "$RSYNC_SSH" "${LOCAL_DIR}/" "${REMOTE_HOST}:${REMOTE_DIR}/"

echo "Uploaded temp DB + templates to ${REMOTE_HOST}:${REMOTE_DIR}"

FLUX_DB_LOCAL="$(ls -t "${LOCAL_DIR}/flux_equil_inputs_"*.db | head -n 1)"
FLUX_DB_NAME="$(basename "${FLUX_DB_LOCAL}")"

MAIN_DB_PATH="${MAIN_DB}" \
ORIGIN_NAME="${ORIGIN_NAME}" \
FLUX_DB_NAME="${FLUX_DB_NAME}" \
REMOTE_HOST="${REMOTE_HOST}" \
REMOTE_DIR="${REMOTE_DIR}" \
python3 - <<'PY'
import os
import sqlite3

main_db = os.environ["MAIN_DB_PATH"]
origin_name = os.environ.get("ORIGIN_NAME", "")
flux_db_name = os.environ["FLUX_DB_NAME"]
remote_host = os.environ["REMOTE_HOST"]
remote_dir = os.environ["REMOTE_DIR"]

conn = sqlite3.connect(main_db)
try:
    row = conn.execute(
        "SELECT id FROM data_origin WHERE name = ?",
        (origin_name,),
    ).fetchone()
    data_origin_id = int(row[0]) if row else None
    conn.execute(
        """
        INSERT INTO flux_action_log (
            data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (data_origin_id, origin_name, flux_db_name, remote_host, remote_dir),
    )
    conn.commit()
finally:
    conn.close()
PY
