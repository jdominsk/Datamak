#!/bin/bash
#SBATCH --job-name=transp_fullauto
#SBATCH --output=logs/transp_fullauto_%j.out
#SBATCH --error=logs/transp_fullauto_%j.err
#SBATCH --time=04:00:00
#SBATCH --partition=all
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G

set -o pipefail

## sbatch --partition=all --time=04:00:00 --mem=8G flux/run_mainsteps2_slurm.sh /path/to/flux_equil_inputs_TIMESTAMP.db

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
echo "[wrapper] invoking MainSteps_2_launch_on_flux.sh with db=$1"
./MainSteps_2_launch_on_flux.sh "$@"
