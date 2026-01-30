#!/usr/bin/env python3
import os
import re
import sqlite3
import sys
from typing import List, Tuple


def update_restart_input(content: str) -> str:
    lines = content.splitlines()
    restart_updated = False
    restart_re = re.compile(r"^(\s*restart\s*=\s*)(.+)$", re.IGNORECASE)
    for i, line in enumerate(lines):
        m = restart_re.match(line)
        if m:
            lines[i] = f"{m.group(1)}true"
            restart_updated = True
            continue
    if not restart_updated:
        lines.append("restart = true")
    return "\n".join(lines) + "\n"


def _find_invalid_lines(content: str) -> List[Tuple[int, str]]:
    bad_lines: List[Tuple[int, str]] = []
    header_re = re.compile(r"^\s*\[.+\]\s*$")
    for idx, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            continue
        if header_re.match(line):
            continue
        if "=" in line:
            continue
        bad_lines.append((idx, line.strip()))
    return bad_lines


def _update_param_in_section(
    content: str, section: str, key: str, value: str
) -> str:
    lines = content.splitlines()
    section_re = re.compile(rf"^\s*\[{re.escape(section)}\]\s*$", re.IGNORECASE)
    header_re = re.compile(r"^\s*\[.+\]\s*$")
    key_re = re.compile(rf"^(\s*{re.escape(key)}\s*=\s*)(.+)$", re.IGNORECASE)

    section_idx = None
    for idx, line in enumerate(lines):
        if section_re.match(line):
            section_idx = idx
            break

    if section_idx is None:
        return f"{key} = {value}\n" + content

    insert_idx = section_idx + 1
    for idx in range(section_idx + 1, len(lines)):
        line = lines[idx]
        if header_re.match(line):
            break
        if key_re.match(line):
            lines[idx] = key_re.sub(rf"\1{value}", line, count=1)
            return "\n".join(lines) + "\n"
        insert_idx = idx + 1

    lines.insert(insert_idx, f"{key} = {value}")
    if key.lower() == "nstep":
        if insert_idx + 1 < len(lines) and lines[insert_idx + 1].strip():
            lines.insert(insert_idx + 1, "")
    return "\n".join(lines) + "\n"


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: claim_next_run.py DB_PATH OUTPUT_DIR")
    db_path = sys.argv[1]
    output_dir = sys.argv[2]
    nwrite_override = os.environ.get("GX_NWRITE_OVERRIDE", "").strip()
    nstep_override = os.environ.get("GX_NSTEP_OVERRIDE", "").strip()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("BEGIN IMMEDIATE")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
        if "nb_restart" not in columns:
            conn.execute(
                "ALTER TABLE gk_run ADD COLUMN nb_restart INTEGER NOT NULL DEFAULT 0"
            )
        row = conn.execute(
            "SELECT id, gk_input_id, gk_batch_id, input_content, "
            "t_max_initial, t_max, nb_restart "
            "FROM gk_run WHERE status = 'TORUN' "
            "ORDER BY nb_restart ASC, id ASC LIMIT 1"
        ).fetchone()
        if row is None:
            restart_rows = conn.execute(
                "SELECT id, input_content "
                "FROM gk_run WHERE status = 'RESTART' ORDER BY id"
            ).fetchall()
            if restart_rows:
                for run_id, content in restart_rows:
                    updated_content = update_restart_input(content)
                    conn.execute(
                        "UPDATE gk_run "
                        "SET input_content = ?, status = 'TORUN', synced = 0, "
                        "nb_restart = nb_restart + 1 "
                        "WHERE id = ?",
                        (updated_content, int(run_id)),
                    )
                row = conn.execute(
                    "SELECT id, gk_input_id, gk_batch_id, input_content, "
                    "t_max_initial, t_max, nb_restart "
                    "FROM gk_run WHERE status = 'TORUN' "
                    "ORDER BY nb_restart ASC, id ASC LIMIT 1"
                ).fetchone()
        if row is None:
            print("NO_TORUN")
            conn.commit()
            return 2

        run_id = int(row["id"])
        gk_input_id = int(row["gk_input_id"]) if row["gk_input_id"] is not None else 0
        gk_batch_id = int(row["gk_batch_id"]) if row["gk_batch_id"] is not None else 0
        content = row["input_content"]
        tmax_initial = float(row["t_max_initial"] or 0)
        tmax_current = float(row["t_max"] or 0)
        nb_restart = int(row["nb_restart"] or 0)

        tmax_re = re.compile(r"(\bt_max\s*=\s*)([-+0-9.eE]+)", re.IGNORECASE)
        match = tmax_re.search(content)
        if match is None:
            raise ValueError("t_max not found in input_content for initial capture.")
        tmax_in_content = float(match.group(2))
        if tmax_initial == 0:
            tmax_initial = tmax_in_content
        if tmax_current == 0:
            tmax_current = tmax_in_content
        tmax_current = tmax_initial * (1 + nb_restart)
        tmax_str = f"{tmax_current:.1f}"

        def repl(m):
            return f"{m.group(1)}{tmax_str}"

        content, count = tmax_re.subn(repl, content, count=1)
        if count == 0:
            raise ValueError("t_max not found in input_content for update.")

        if nwrite_override:
            content = _update_param_in_section(
                content, "Time", "nwrite", nwrite_override
            )
        if nstep_override:
            content = _update_param_in_section(
                content, "Time", "nstep", nstep_override
            )

        filename = f"input_batchid{gk_batch_id}_gkinputid{gk_input_id}_runid{run_id}.in"
        filepath = os.path.join(output_dir, filename)

        invalid_lines = _find_invalid_lines(content)
        if invalid_lines:
            lines_preview = ", ".join(
                f"{idx}:{text}" for idx, text in invalid_lines[:5]
            )
            print(
                f"Invalid GX input for run_id={run_id}; would write {filepath}"
            )
            raise ValueError(
                f"Invalid GX input: bare tokens without '=' found. "
                f"Examples: {lines_preview}. "
                f"Input file: {filepath}"
            )
        with open(filepath, "w", encoding="utf-8") as handle:
            handle.write(content)
        conn.execute(
            "UPDATE gk_run "
            "SET status = 'RUNNING', input_name = ?, job_id = ?, synced = 0, "
            "input_content = ?, t_max_initial = ?, t_max = ?, nb_restart = ? "
            "WHERE id = ?",
            (
                filename,
                os.environ.get("SLURM_JOB_ID", ""),
                content,
                tmax_initial,
                tmax_current,
                nb_restart,
                run_id,
            ),
        )
        conn.commit()
        print(f"{run_id}\t{filepath}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
