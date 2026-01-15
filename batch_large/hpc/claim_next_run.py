#!/usr/bin/env python3
import os
import re
import sqlite3
import sys


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


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: claim_next_run.py DB_PATH OUTPUT_DIR")
    db_path = sys.argv[1]
    output_dir = sys.argv[2]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT id, gk_input_id, gk_batch_id, input_content, t_max_initial, t_max "
            "FROM gk_run WHERE status = 'TORUN' ORDER BY id LIMIT 1"
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
                        "UPDATE gk_run SET input_content = ?, status = 'TORUN', synced = 0 WHERE id = ?",
                        (updated_content, int(run_id)),
                    )
                row = conn.execute(
                    "SELECT id, gk_input_id, gk_batch_id, input_content, t_max_initial, t_max "
                    "FROM gk_run WHERE status = 'TORUN' ORDER BY id LIMIT 1"
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

        tmax_re = re.compile(r"(\bt_max\s*=\s*)([-+0-9.eE]+)", re.IGNORECASE)
        match = tmax_re.search(content)
        if match is None:
            raise ValueError("t_max not found in input_content for initial capture.")
        tmax_in_content = float(match.group(2))
        if tmax_initial == 0:
            tmax_initial = tmax_in_content
        if tmax_current == 0:
            tmax_current = tmax_in_content
        else:
            tmax_current = max(tmax_current, tmax_in_content)

        restart_true = bool(
            re.search(r"^\s*restart\s*=\s*true\b", content, re.IGNORECASE | re.MULTILINE)
        )
        if restart_true:
            tmax_current += tmax_initial
        tmax_str = f"{tmax_current:.1f}"

        def repl(m):
            return f"{m.group(1)}{tmax_str}"

        content, count = tmax_re.subn(repl, content, count=1)
        if count == 0:
            raise ValueError("t_max not found in input_content for update.")

        filename = f"input_batchid{gk_batch_id}_gkinputid{gk_input_id}_runid{run_id}.in"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as handle:
            handle.write(content)
        conn.execute(
            "UPDATE gk_run "
            "SET status = 'RUNNING', input_name = ?, job_id = ?, synced = 0, "
            "input_content = ?, t_max_initial = ?, t_max = ? "
            "WHERE id = ?",
            (
                filename,
                os.environ.get("SLURM_JOB_ID", ""),
                content,
                tmax_initial,
                tmax_current,
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
