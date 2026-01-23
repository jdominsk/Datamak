#!/bin/bash
set -euo pipefail

## sbatch --mem=8G --partition=all --time=04:00:00   flux/run_gk_inputs_slurm.sh /u/jdominsk/DTwin/transp_full_auto/flux_equil_inputs_20260120_164628.db 4000 100

#SBATCH --job-name=gk_inputs
#SBATCH --output=logs/gk_inputs_%j.out
#SBATCH --error=logs/gk_inputs_%j.err
#SBATCH --time=04:00:00
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --chdir=/u/jdominsk/DTwin/transp_full_auto/flux

module load sqlite

mkdir -p logs
export PYTHONUNBUFFERED=1
./run_gk_inputs_local.sh "$@"
