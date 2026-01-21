#!/bin/bash
set -euo pipefail

DTWIN_ROOT="${DTWIN_ROOT:-/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin}"
MAIN_DB="${DTWIN_ROOT}/gyrokinetic_simulations.db"
LOCAL_DIR="${DTWIN_ROOT}/transp_full_auto"
REMOTE_DIR="/u/jdominsk/DTwin/transp_full_auto"
REMOTE_HOST="jdominsk@flux"

FLUX_DB_LOCAL="${2:-}"
if [[ -z "${FLUX_DB_LOCAL}" ]]; then
  if [[ -n "${1:-}" ]]; then
    base_name="$(basename "${1}")"
    FLUX_DB_LOCAL="${LOCAL_DIR}/${base_name}"
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
ssh "${RSYNC_SSH_OPTS[@]}" -t "${REMOTE_HOST}" "true"
rsync -av -e "$RSYNC_SSH" "${REMOTE_HOST}:${REMOTE_DIR}/flux_equil_inputs_"*.db "${LOCAL_DIR}/"

python3 "${DTWIN_ROOT}/db_update/Transp_full_auto/sync_flux_equil_inputs_to_main.py" \
  --main-db "${MAIN_DB}" \
  --flux-db "${FLUX_DB_LOCAL}"

python3 "${DTWIN_ROOT}/db_update/backfill_gk_input_physics.py" \
  --db "${MAIN_DB}"
