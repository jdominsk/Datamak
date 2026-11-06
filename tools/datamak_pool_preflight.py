#!/usr/bin/env python3
"""Validate the exact script bundle used by a pool without launching work."""

from __future__ import annotations

import argparse
import py_compile
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shell", action="append", default=[], type=Path)
    parser.add_argument("--python", action="append", default=[], type=Path)
    parser.add_argument("--executable", action="append", default=[], type=Path)
    parser.add_argument("--required-file", action="append", default=[], type=Path)
    args = parser.parse_args()

    for path in args.shell:
        subprocess.run(["bash", "-n", str(path)], check=True)
    for path in args.python:
        py_compile.compile(str(path), doraise=True)
    for path in args.executable:
        if not path.is_file() or not path.stat().st_mode & 0o111:
            raise SystemExit(f"Missing executable: {path}")
    for path in args.required_file:
        if not path.is_file() or path.stat().st_size == 0:
            raise SystemExit(f"Missing or empty required file: {path}")
    print("Pool preflight passed. No application process was launched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
