#!/bin/bash
#SBATCH --job-name=gk_inputs
#SBATCH --output=logs/gk_inputs_%j.out
#SBATCH --error=logs/gk_inputs_%j.err
#SBATCH --time=04:00:00
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G

set -o pipefail

## sbatch --mem=8G --partition=all --time=04:00:00 flux/run_gk_inputs_slurm.sh /path/to/flux_equil_inputs_TIMESTAMP.db 4000 100

set +eu
if [[ -f /etc/profile ]]; then
  # shellcheck disable=SC1091
  source /etc/profile
fi
if [[ -f ~/.bash_profile ]]; then
  # shellcheck disable=SC1090
  source ~/.bash_profile
fi
if [[ -f ~/.bashrc ]]; then
  # shellcheck disable=SC1090
  source ~/.bashrc
fi
if command -v module >/dev/null 2>&1; then
  module load sqlite >/dev/null 2>&1 || true
fi
set -euo pipefail

ROOT_DIR="${SLURM_SUBMIT_DIR:-}"
if [[ -z "${ROOT_DIR}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi
SCRIPT_DIR="${ROOT_DIR}/flux"
RUNTIME_ENV="${ROOT_DIR}/datamak_runtime.env"
if [[ -f "${RUNTIME_ENV}" ]]; then
  # shellcheck disable=SC1090
  source "${RUNTIME_ENV}"
fi

cd "${ROOT_DIR}"
mkdir -p logs
export PYTHONUNBUFFERED=1
echo "[wrapper] ROOT_DIR=${ROOT_DIR}"
echo "[wrapper] invoking run_gk_inputs_local.sh with db=$1"
./run_gk_inputs_local.sh "$@"
