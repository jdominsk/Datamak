from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_datamak_pool_marker(
    pool_root: str | Path,
    *,
    pool_db: str = "pool.db",
    status_command: str = "python3 pool_status.py",
    interactive_launcher: str = "./run_pool_interactive.sh",
    lite_sidecar: str = "datamak_lite.json",
    worker_model: str = "sqlite_worker_pool",
    recovery_command: str | None = None,
    notes: str = "",
) -> dict[str, Any]:
    recovery_command = recovery_command or f"RECOVER_RUNNING=1 {interactive_launcher}"
    return {
        "schema_version": 1,
        "workflow_engine": "datamak",
        "pool_type": worker_model,
        "pool_root": str(Path(pool_root).expanduser().resolve()),
        "pool_db": pool_db,
        "status_command": status_command,
        "interactive_launcher": interactive_launcher,
        "recovery": {
            "recover_running": recovery_command,
        },
        "lite_sidecar": lite_sidecar,
        "notes": notes,
    }


def build_pool_readme(marker: dict[str, Any]) -> str:
    recovery = marker.get("recovery", {})
    recover_running = recovery.get("recover_running", "")
    notes = str(marker.get("notes") or "").strip()
    notes_block = f"\n## Notes\n{notes}\n" if notes else ""
    return (
        "# Datamak-Style Simulation Pool\n\n"
        "This directory is a Datamak-style simulation or analysis pool.\n"
        "Agents should use the `datamak` skill before modifying this pool.\n\n"
        "## How To Run\n\n"
        "Use the launcher in this directory:\n\n"
        "```bash\n"
        f"{marker['interactive_launcher']}\n"
        "```\n\n"
        "## Status\n\n"
        "```bash\n"
        f"{marker['status_command']}\n"
        "```\n\n"
        "## Recovery\n\n"
        "If an allocation died while cases were marked running:\n\n"
        "```bash\n"
        f"{recover_running}\n"
        "```\n\n"
        "## Metadata\n\n"
        f"- Pool database: `{marker['pool_db']}`\n"
        "- Machine-readable pool marker: `datamak_pool.json`\n"
        f"- Lite sidecar: `{marker['lite_sidecar']}`\n"
        f"{notes_block}"
    )


def write_datamak_pool_marker(
    pool_root: str | Path,
    marker: dict[str, Any],
    *,
    write_readme: bool = True,
    overwrite: bool = False,
) -> dict[str, Path]:
    root = Path(pool_root).expanduser()
    root.mkdir(parents=True, exist_ok=True)

    marker_path = root / "datamak_pool.json"
    readme_path = root / "README.md"

    _write_text_if_allowed(marker_path, json.dumps(marker, indent=2, sort_keys=False) + "\n", overwrite=overwrite)
    written = {"marker": marker_path}
    if write_readme:
        _write_text_if_allowed(readme_path, build_pool_readme(marker), overwrite=overwrite)
        written["readme"] = readme_path
    return written


def _write_text_if_allowed(path: Path, text: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        return
    path.write_text(text)
