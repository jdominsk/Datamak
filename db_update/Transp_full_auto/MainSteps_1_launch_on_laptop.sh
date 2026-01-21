#!/bin/bash
set -euo pipefail

DTWIN_ROOT="${DTWIN_ROOT:-/Users/jdominsk/Documents/Projects/AIML_database/Digital_twin}"
LOCAL_DIR="${DTWIN_ROOT}/transp_full_auto"
MAIN_DB="${DTWIN_ROOT}/gyrokinetic_simulations.db"
REMOTE_DIR="/u/jdominsk/DTwin/transp_full_auto"
REMOTE_HOST="jdominsk@flux"

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
