#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DTWIN_ROOT="${DTWIN_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
LOCAL_DIR="${DTWIN_ROOT}/transp_full_auto"
MAIN_DB="${DTWIN_ROOT}/gyrokinetic_simulations.db"
ORIGIN_ID="${ORIGIN_ID:-}"
ORIGIN_NAME="${ORIGIN_NAME:-Transp 09 (full-auto)}"
eval "$(python3 "${DTWIN_ROOT}/tools/resolve_dtwin_env.py" --profile flux --format shell)"

REMOTE_DIR="${DTWIN_FLUX_BASE_DIR:-}"
REMOTE_HOST="${DTWIN_FLUX_REMOTE:-}"
if [[ -z "${REMOTE_DIR}" || -z "${REMOTE_HOST}" ]]; then
  echo "Missing Flux runtime settings. Configure Flux host/base dir in Datamak settings."
  exit 1
fi

mkdir -p "${LOCAL_DIR}/templates"

python3 "${DTWIN_ROOT}/db_update/Transp_full_auto/build_flux_equil_inputs.py" \
  --db "${MAIN_DB}" \
  --out-dir "${LOCAL_DIR}"

rsync -av "${DTWIN_ROOT}/pyrokinetics"/*.in "${LOCAL_DIR}/templates/"

rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/MainSteps_2_launch_on_flux.sh" "${LOCAL_DIR}/"
rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/build_flux_equil_inputs.py" "${LOCAL_DIR}/"
rsync -av "${DTWIN_ROOT}/db_update/Transp_full_auto/flux" "${LOCAL_DIR}/"
python3 "${DTWIN_ROOT}/tools/resolve_dtwin_env.py" --profile flux --format shell > "${LOCAL_DIR}/datamak_runtime.env"

RSYNC_SSH_OPTS=(
  -o ControlMaster=auto
  -o ControlPersist=10m
  -o ControlPath="${HOME}/.ssh/cm-%r@%h:%p"
)
RSYNC_SSH="ssh ${RSYNC_SSH_OPTS[*]}"

# Open a control connection so you can complete 2FA once, then reuse it for rsync.
python3 "${DTWIN_ROOT}/tools/ssh_with_duo.py" \
  --duo-option "${DTWIN_FLUX_DUO_OPTION:-}" \
  -- \
  ssh "${RSYNC_SSH_OPTS[@]}" -t "${REMOTE_HOST}" "true"
rsync -av -e "$RSYNC_SSH" "${LOCAL_DIR}/" "${REMOTE_HOST}:${REMOTE_DIR}/"

echo "Uploaded temp DB + templates to ${REMOTE_HOST}:${REMOTE_DIR}"

FLUX_DB_LOCAL="$(ls -t "${LOCAL_DIR}/flux_equil_inputs_"*.db | head -n 1)"
FLUX_DB_NAME="$(basename "${FLUX_DB_LOCAL}")"

MAIN_DB_PATH="${MAIN_DB}" \
ORIGIN_ID="${ORIGIN_ID}" \
ORIGIN_NAME="${ORIGIN_NAME}" \
FLUX_DB_NAME="${FLUX_DB_NAME}" \
REMOTE_HOST="${REMOTE_HOST}" \
REMOTE_DIR="${REMOTE_DIR}" \
python3 - <<'PY'
import os
import sqlite3

main_db = os.environ["MAIN_DB_PATH"]
origin_id_raw = os.environ.get("ORIGIN_ID", "").strip()
origin_name = os.environ.get("ORIGIN_NAME", "")
flux_db_name = os.environ["FLUX_DB_NAME"]
remote_host = os.environ["REMOTE_HOST"]
remote_dir = os.environ["REMOTE_DIR"]

conn = sqlite3.connect(main_db)
try:
    data_origin_id = None
    if origin_id_raw.isdigit():
        row = conn.execute(
            "SELECT id, name FROM data_origin WHERE id = ?",
            (int(origin_id_raw),),
        ).fetchone()
        if row:
            data_origin_id = int(row[0])
            origin_name = str(row[1] or origin_name)
    elif origin_name:
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
