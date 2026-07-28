#!/usr/bin/env python3
"""Stop pool claims and request clean application stops before PBS walltime."""

from __future__ import annotations

import argparse
import re
import subprocess
import time
from pathlib import Path


def hms_seconds(value: str) -> int:
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def remaining_seconds(job_id: str) -> int:
    text = subprocess.check_output(
        ["qstat", "-f", job_id], stderr=subprocess.STDOUT, text=True
    )
    requested = re.search(r"Resource_List\.walltime\s*=\s*(\d+:\d+:\d+)", text)
    used = re.search(r"resources_used\.walltime\s*=\s*(\d+:\d+:\d+)", text)
    if requested is None:
        raise RuntimeError("qstat did not report requested walltime")
    return hms_seconds(requested.group(1)) - hms_seconds(
        used.group(1) if used else "00:00:00"
    )


def request_clean_stops(active_dir: Path) -> int:
    active_dir.mkdir(parents=True, exist_ok=True)
    (active_dir / "STOP_CLAIMING").touch()
    count = 0
    for active_file in active_dir.iterdir():
        if not active_file.is_file() or active_file.name == "STOP_CLAIMING":
            continue
        try:
            stop_path = active_file.read_text(encoding="utf-8").strip()
            if not stop_path:
                continue
            stop_file = Path(stop_path)
            (stop_file.parent / ".walltime_stop_requested").touch()
            stop_file.touch()
            count += 1
        except OSError as exc:
            print(f"walltime_monitor_warning={active_file}: {exc}", flush=True)
    return count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-dir", required=True, type=Path)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--margin-seconds", type=int, default=300)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()

    while True:
        try:
            remaining = remaining_seconds(args.job_id)
            print(f"remaining_seconds={remaining}", flush=True)
            if remaining <= args.margin_seconds:
                count = request_clean_stops(args.active_dir)
                print(f"requested_clean_stops={count}", flush=True)
                return 0
        except Exception as exc:
            print(f"walltime_monitor_warning={exc}", flush=True)
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
