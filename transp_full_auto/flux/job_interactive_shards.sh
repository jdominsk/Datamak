#!/bin/bash
set -euo pipefail

OUTDIR="/u/jdominsk/DTwin/tmp_inputs"
SHARD_DIR="/u/jdominsk/DTwin/remote_gk_inputs/shards"
LOG_DIR="/u/jdominsk/DTwin/remote_gk_inputs/logs_shards"
mkdir -p "$LOG_DIR"

TOTAL_CPUS=${SLURM_CPUS_ON_NODE:-8}
WORKERS=8
if (( WORKERS > TOTAL_CPUS )); then
  WORKERS="$TOTAL_CPUS"
fi

echo "[job] SLURM_JOB_ID=${SLURM_JOB_ID:-none} CPUS=$TOTAL_CPUS WORKERS=$WORKERS"
echo "[job] LOG_DIR=$LOG_DIR"
echo "[job] Starting srun at $(date)"

srun --ntasks="$WORKERS" --cpus-per-task=1 --exclusive bash -lc '
  i=$SLURM_PROCID
  source /u/jdominsk/pyrokinetics/.venv/bin/activate
  export PYTHONUNBUFFERED=1
  DB="'"$SHARD_DIR"'/remote_gk_inputs_shard_${i}.db"
  LOG_OUT="'"$LOG_DIR"'/worker_${i}.out"
  LOG_ERR="'"$LOG_DIR"'/worker_${i}.err"
  exec >"$LOG_OUT" 2>"$LOG_ERR"
  echo "[worker $i] db=$DB"
  python3 -u /u/jdominsk/DTwin/remote_gk_inputs/run_remote_gk_inputs_flux.py \
    --db "$DB" \
    --output-dir "'"$OUTDIR"'" \
    --max-mem-gb 4
'

echo "[job] srun finished at $(date)"