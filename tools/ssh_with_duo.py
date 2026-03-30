#!/usr/bin/env python3
import argparse
import os
import pty
import re
import select
import subprocess
import sys
from typing import List


DUO_PROMPT_RE = re.compile(r"Passcode or option\s*\([^)]*\)\s*:\s*$", re.IGNORECASE)
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a command in a PTY and optionally auto-send a saved Duo option "
            "when the Flux login prompt appears."
        )
    )
    parser.add_argument(
        "--duo-option",
        default="",
        help="Duo option to auto-send when prompted, usually 1, 2, or 3.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run after --, for example: -- ssh -t user@host true",
    )
    return parser.parse_args()


def _clean_command(parts: List[str]) -> List[str]:
    if parts and parts[0] == "--":
        return parts[1:]
    return parts


def _normalize_terminal_text(text: str) -> str:
    cleaned = ANSI_ESCAPE_RE.sub("", text)
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    return cleaned


def _is_duo_prompt_visible(rolling: str) -> bool:
    normalized = _normalize_terminal_text(rolling)
    last_line = normalized.split("\n")[-1]
    return bool(DUO_PROMPT_RE.search(last_line))


def main() -> int:
    args = parse_args()
    command = _clean_command(args.command)
    if not command:
        raise SystemExit("Missing command. Usage: ssh_with_duo.py --duo-option 1 -- ssh ...")

    duo_option = str(args.duo_option or "").strip()
    if duo_option and duo_option not in {"1", "2", "3"}:
        raise SystemExit("--duo-option must be one of 1, 2, 3, or blank.")

    master_fd, slave_fd = pty.openpty()
    process = subprocess.Popen(
        command,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )
    os.close(slave_fd)

    stdin_fd = sys.stdin.fileno() if sys.stdin.isatty() else None
    rolling = ""
    auto_sent = False

    try:
        while True:
            watched = [master_fd]
            if stdin_fd is not None:
                watched.append(stdin_fd)
            ready, _, _ = select.select(watched, [], [], 0.1)

            if master_fd in ready:
                try:
                    data = os.read(master_fd, 4096)
                except OSError:
                    data = b""
                if not data:
                    break
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
                rolling = (rolling + data.decode(errors="ignore"))[-8000:]
                if duo_option and not auto_sent and _is_duo_prompt_visible(rolling):
                    os.write(master_fd, f"{duo_option}\n".encode("utf-8"))
                    auto_sent = True

            if stdin_fd is not None and stdin_fd in ready:
                try:
                    user_data = os.read(stdin_fd, 1024)
                except OSError:
                    user_data = b""
                if not user_data:
                    stdin_fd = None
                else:
                    os.write(master_fd, user_data)

            if process.poll() is not None and master_fd not in ready:
                # Give the PTY one more chance to drain before exiting.
                try:
                    data = os.read(master_fd, 4096)
                except OSError:
                    data = b""
                if data:
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                break
    except KeyboardInterrupt:
        process.terminate()
    finally:
        try:
            os.close(master_fd)
        except OSError:
            pass

    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
