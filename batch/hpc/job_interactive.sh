#!/bin/bash
set -euo pipefail

GX_PATH="${DTWIN_GX_PATH:-$HOME/GX/gx_next6}"
source ${GX_PATH}/module.sh
module load cray-python/3.11.7
WORK_DIR="${PWD}"

cd "$WORK_DIR"
echo "Running from: $WORK_DIR"

if [[ $# -ge 1 ]]; then
  DB_PATH="$1"
else
  newest_db="$(ls -t "$PWD"/batch_database_*.db 2>/dev/null | head -n 1 || true)"
  if [[ -z "$newest_db" ]]; then
    echo "No batch_database_*.db found in $PWD"
    exit 1
  fi
  DB_PATH="$newest_db"
fi
NODES="${2:-${SLURM_JOB_NUM_NODES:-4}}"

export GX_PATH
export NODES

echo "Using DB: $DB_PATH"
echo "Nodes: $NODES"
bash "$(dirname "$0")/job_execute.sh" "$DB_PATH" "$NODES"
