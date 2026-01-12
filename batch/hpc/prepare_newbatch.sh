#!/bin/bash
set -euo pipefail

newbatch_dir="$(pwd)/new"
parent_dir="$(cd "${newbatch_dir}/../.." && pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

job_submit="${script_dir}/job_submit.sh"
job_execute="${script_dir}/job_execute.sh"

if [[ ! -f "$job_submit" || ! -f "$job_execute" ]]; then
  echo "Missing job scripts in ${script_dir}."
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
for dir in "${parent_dir}"/run[0-9][0-9][0-9][0-9]; do
  [[ -d "$dir" ]] || continue
  base="$(basename "$dir")"
  num="${base#run}"
  if [[ "$num" =~ ^[0-9]{4}$ ]]; then
    if ((10#$num > max_run)); then
      max_run=$((10#$num))
    fi
  fi
done

next_run=$((max_run + 1))
manifest="${newbatch_dir}/../new_runs.txt"
rm -f "$manifest"
for db_path in "${db_files[@]}"; do
  run_dir="${parent_dir}/run$(printf "%04d" "$next_run")"
  mkdir -p "$run_dir"
  cp -p "$db_path" "${run_dir}/"
  cp -p "$job_submit" "$job_execute" "${run_dir}/"
  rm -f "$db_path"
  echo "Created ${run_dir} from $(basename "$db_path")"
  echo "$run_dir" >> "$manifest"
  next_run=$((next_run + 1))
done
