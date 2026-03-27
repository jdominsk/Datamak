#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dtwin_config import (  # noqa: E402
    format_shell_exports,
    resolve_flux_profile,
    resolve_perlmutter_profile,
    resolve_sources_profile,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve Datamak runtime settings into shell exports or JSON."
    )
    parser.add_argument(
        "--profile",
        required=True,
        choices=["perlmutter", "flux", "sources"],
        help="Profile to resolve.",
    )
    parser.add_argument(
        "--format",
        default="shell",
        choices=["shell", "json"],
        help="Output format.",
    )
    args = parser.parse_args()

    if args.profile == "perlmutter":
        profile = resolve_perlmutter_profile()
        payload = {
            "DTWIN_PERLMUTTER_USER": profile["user"],
            "DTWIN_PERLMUTTER_HOST": profile["host"],
            "DTWIN_PERLMUTTER_REMOTE": profile["remote"],
            "DTWIN_PERLMUTTER_BASE_DIR": profile["base_dir"],
            "DTWIN_PERLMUTTER_BATCH_DIR": profile["batch_dir"],
            "DTWIN_GX_PATH": profile["gx_path"],
            "DTWIN_SSH_IDENTITY": profile["identity"],
            "DTWIN_SSH_CONTROL_PATH": profile["control_path"],
            "DTWIN_SSH_CONTROL_PERSIST": profile["control_persist"],
            "DTWIN_SSH_CONNECT_TIMEOUT": profile["connect_timeout"],
            "DTWIN_MONITOR_TIMEOUT": profile["monitor_timeout"],
        }
    elif args.profile == "flux":
        profile = resolve_flux_profile()
        payload = {
            "DTWIN_FLUX_USER": profile["user"],
            "DTWIN_FLUX_HOST": profile["host"],
            "DTWIN_FLUX_REMOTE": profile["remote"],
            "DTWIN_FLUX_BASE_DIR": profile["base_dir"],
            "DTWIN_FLUX_PYTHON": profile["python_bin"],
            "DTWIN_FLUX_DUO_OPTION": profile["duo_option"],
        }
    else:
        profile = resolve_sources_profile()
        payload = {
            "DTWIN_MATE_ROOT": profile["mate_root"],
            "DTWIN_TRANSP_COPY_ROOT_09": profile["transp_copy_root_09"],
            "DTWIN_TRANSP_COPY_ROOT_10": profile["transp_copy_root_10"],
        }

    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(format_shell_exports(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
