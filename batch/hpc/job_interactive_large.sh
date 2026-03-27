#!/bin/bash
set -euo pipefail

GX_PATH="${DTWIN_GX_PATH:-$HOME/GX/gx_next6}"
source ${GX_PATH}/module.sh
module load cray-python/3.11.7
WORK_DIR="${PWD}"

# To save the main log while streaming it: ./job_interactive_large.sh 2>&1 | tee -a job_main.log

cd "$WORK_DIR"
echo "Running from: $WORK_DIR"

DB_PATH=""
NODES=""
PASS_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --analyze-only|--analyze-all-nc|--force-running-torun)
      PASS_ARGS+=("$1")
      shift
      ;;
    --workers)
      PASS_ARGS+=("$1" "$2")
      shift 2
      ;;
    --nodes)
      NODES="$2"
      shift 2
      ;;
    -*)
      PASS_ARGS+=("$1")
      shift
      ;;
    *)
      if [[ -z "$DB_PATH" ]]; then
        DB_PATH="$1"
      elif [[ -z "$NODES" ]]; then
        NODES="$1"
      else
        PASS_ARGS+=("$1")
      fi
      shift
      ;;
  esac
done

if [[ -z "$DB_PATH" ]]; then
  newest_db="$(ls -t "$PWD"/batch_database_*.db 2>/dev/null | head -n 1 || true)"
  if [[ -z "$newest_db" ]]; then
    echo "No batch_database_*.db found in $PWD"
    exit 1
  fi
  DB_PATH="$newest_db"
fi
if [[ -z "$NODES" ]]; then
  NODES="${SLURM_JOB_NUM_NODES:-4}"
fi
WORKERS="${WORKERS:-$NODES}"

export GX_PATH
export NODES
export WORKERS

echo "Using DB: $DB_PATH"
echo "Nodes: $NODES"
bash "$(dirname "$0")/job_execute_large.sh" "$DB_PATH" "$NODES" --workers "$WORKERS" "${PASS_ARGS[@]}"
