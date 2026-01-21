#!/bin/bash
set -euo pipefail

GX_PATH=/global/homes/j/jdominsk/GX/gx_next6
source ${GX_PATH}/module.sh
module load cray-python/3.11.7
WORK_DIR="${PWD}"

# To save the main log while streaming it: ./job_interactive_large.sh 2>&1 | tee -a job_main.log

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
WORKERS="${WORKERS:-$NODES}"

export GX_PATH
export NODES
export WORKERS

echo "Using DB: $DB_PATH"
echo "Nodes: $NODES"
bash "$(dirname "$0")/job_execute_large.sh" "$DB_PATH" "$NODES" --workers "$WORKERS"
