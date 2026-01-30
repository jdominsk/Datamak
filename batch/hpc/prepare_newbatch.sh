#!/bin/bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
newbatch_root="$(cd "${script_dir}/.." && pwd)"
newbatch_dir="${newbatch_root}/new"
if [[ -n "${BASE_DIR:-}" ]]; then
  parent_dir="$BASE_DIR"
else
  parent_dir="$(cd "${newbatch_root}/.." && pwd)"
fi
hpc_dir="${script_dir}"

job_submit="${hpc_dir}/job_submit.sh"
job_execute="${hpc_dir}/job_execute.sh"
gx_analyze="${hpc_dir}/gx_analyze.py"
job_interactive="${hpc_dir}/job_interactive.sh"
linear_conv="${hpc_dir}/linear_convergence.py"
ky_growth="${hpc_dir}/ky_growth_rates.py"

if [[ ! -f "$job_submit" || ! -f "$job_execute" ]]; then
  echo "Missing job scripts in ${hpc_dir}."
  exit 1
fi

shopt -s nullglob
db_files=("${newbatch_dir}"/batch_database_*.db)
shopt -u nullglob

if [[ ${#db_files[@]} -eq 0 ]]; then
  echo "No batch_database_*.db files found in ${newbatch_dir}"
  exit 0
fi

max_run=0
for dir in "${parent_dir}"/run[0-9][0-9][0-9][0-9] "${parent_dir}"/batch[0-9][0-9][0-9][0-9]; do
  [[ -d "$dir" ]] || continue
  base="$(basename "$dir")"
  num="${base#run}"
  if [[ "$base" == batch* ]]; then
    num="${base#batch}"
  fi
  if [[ "$num" =~ ^[0-9]{4}$ ]]; then
    if ((10#$num > max_run)); then
      max_run=$((10#$num))
    fi
  fi
done

next_run=$((max_run + 1))
manifest="${newbatch_root}/new_runs.txt"
rm -f "$manifest"
for db_path in "${db_files[@]}"; do
  run_dir="${parent_dir}/batch$(printf "%04d" "$next_run")"
  mkdir -p "$run_dir"
  cp -p "$db_path" "${run_dir}/"
  cp -p "$job_submit" "$job_execute" "${run_dir}/"
  if [[ -f "$job_interactive" ]]; then
    cp -p "$job_interactive" "${run_dir}/"
  fi
  if [[ -f "$gx_analyze" ]]; then
    cp -p "$gx_analyze" "${run_dir}/"
  fi
  if [[ -f "$linear_conv" ]]; then
    cp -p "$linear_conv" "${run_dir}/"
  fi
  if [[ -f "$ky_growth" ]]; then
    cp -p "$ky_growth" "${run_dir}/"
  fi
  rm -f "$db_path"
  echo "Created ${run_dir} from $(basename "$db_path")"
  echo "$run_dir" >> "$manifest"
  next_run=$((next_run + 1))
done
