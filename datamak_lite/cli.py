from __future__ import annotations

import argparse
import sys
from pathlib import Path

from datamak_lite.adapters.campaign_registry import import_campaign_registry
from datamak_lite.adapters.datamak_pool_marker import build_datamak_pool_marker, write_datamak_pool_marker
from datamak_lite.adapters.figure_audit import discover_figure_audit_files, import_figure_audits
from datamak_lite.adapters.folder_inventory import import_folder_inventory, read_inventory_file
from datamak_lite.adapters.pool_packet import build_pool_packet, write_pool_packet
from datamak_lite.core.campaign_profile import format_refresh_summary, refresh_campaign
from datamak_lite.core.campaign_status import (
    build_campaign_status_from_path,
    format_campaign_status,
    resolve_database_path,
)
from datamak_lite.core.db import init_db
from datamak_lite.core.packet import import_packet
from datamak_lite.core.report import render_entity_report
from datamak_lite.core.repository import LiteRepository
from datamak_lite.core.sync import PacketSyncError, PacketValidationError, sync_and_import_packet
from datamak_lite.core.validate import format_issues, has_errors, issues_as_json, validate_packet
from datamak_lite.examples.demo_seed import FIGURE_UID, seed as seed_demo
from datamak_lite.gui.app import DEFAULT_HOST, DEFAULT_PORT, serve


def cmd_init(args: argparse.Namespace) -> None:
    path = init_db(args.db)
    print(f"Initialized Datamak Lite database: {path}")


def cmd_seed_demo(args: argparse.Namespace) -> None:
    init_db(args.db)
    with LiteRepository(args.db) as repo:
        figure_uid = seed_demo(repo)
    print(f"Seeded example graph. Report target: {figure_uid}")


def cmd_import_packet(args: argparse.Namespace) -> None:
    _validate_packet_or_exit(args.packet)
    init_db(args.db)
    with LiteRepository(args.db) as repo:
        root_uid = import_packet(repo, args.packet)
    print(f"Imported packet: {args.packet}")
    print(f"Root entity: {root_uid}")


def cmd_validate_packet(args: argparse.Namespace) -> None:
    issues = validate_packet(args.packet)
    if args.json:
        print(issues_as_json(issues))
    else:
        print(format_issues(issues), end="")
    if has_errors(issues):
        raise SystemExit(1)


def cmd_sync_packet(args: argparse.Namespace) -> None:
    init_db(args.db)
    with LiteRepository(args.db) as repo:
        try:
            root_uid, local_packet = sync_and_import_packet(
                repo,
                args.packet,
                cache_dir=args.cache_dir,
                dry_run=args.dry_run,
                timeout_seconds=args.timeout,
            )
        except PacketSyncError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            raise SystemExit(2) from None
        except PacketValidationError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            raise SystemExit(1) from None
        if args.dry_run:
            print(f"Would cache packet at: {local_packet}")
            return
        print(f"Synced packet cache: {local_packet}")
        print(f"Root entity: {root_uid}")
        if args.report:
            print()
            print(render_entity_report(repo, str(root_uid)), end="")


def cmd_create_pool_packet(args: argparse.Namespace) -> None:
    packet = build_pool_packet(
        args.pool_root,
        uid=args.uid,
        name=args.name,
        campaign_uid=args.campaign_uid,
        campaign_name=args.campaign_name,
        uses_dataset_uid=args.uses_dataset_uid,
        dataset_name=args.dataset_name,
        dataset_path=args.dataset_path,
        dataset_type=args.dataset_type,
        relation_type=args.relation_type,
        status=args.status,
        scientific_status=args.scientific_status,
        note=args.note,
        author=args.author,
    )
    if args.dry_run:
        import json

        print(json.dumps(packet, indent=2) + "\n", end="")
        return

    packet_path = write_pool_packet(packet, args.pool_root, args.output)
    print(f"Wrote packet: {packet_path}")
    if args.import_db:
        _validate_packet_or_exit(packet_path)
        init_db(args.import_db)
        with LiteRepository(args.import_db) as repo:
            root_uid = import_packet(repo, packet_path)
            print(f"Imported packet into: {args.import_db}")
            print(f"Root entity: {root_uid}")
            if args.report:
                print()
                print(render_entity_report(repo, root_uid), end="")


def cmd_create_pool_marker(args: argparse.Namespace) -> None:
    marker = build_datamak_pool_marker(
        args.pool_root,
        pool_db=args.pool_db,
        status_command=args.status_command,
        interactive_launcher=args.interactive_launcher,
        lite_sidecar=args.lite_sidecar,
        worker_model=args.worker_model,
        recovery_command=args.recovery_command,
        notes=args.note or "",
    )
    written = write_datamak_pool_marker(
        args.pool_root,
        marker,
        write_readme=not args.no_readme,
        overwrite=args.overwrite,
    )
    print(f"Wrote Datamak pool marker: {written['marker']}")
    if "readme" in written:
        print(f"Wrote pool README: {written['readme']}")
    if not args.overwrite:
        print("Existing marker or README files were preserved.")


def cmd_import_campaign_registry(args: argparse.Namespace) -> None:
    init_db(args.db)
    with LiteRepository(args.db) as repo:
        counts = import_campaign_registry(
            repo,
            args.registry_db,
            campaign_uid=args.campaign_uid,
            campaign_name=args.campaign_name,
        )
        print(f"Imported campaign registry: {args.registry_db}")
        print(
            "Counts: "
            f"campaigns={counts['campaigns']}, "
            f"histories={counts['histories']}, "
            f"jobs={counts['jobs']}, "
            f"relations={counts['relations']}"
        )
        if args.report:
            print()
            print(render_entity_report(repo, args.campaign_uid), end="")


def cmd_import_folder_inventory(args: argparse.Namespace) -> None:
    init_db(args.db)
    paths = read_inventory_file(args.inventory_file)
    with LiteRepository(args.db) as repo:
        counts = import_folder_inventory(
            repo,
            paths,
            campaign_uid=args.campaign_uid,
            discovery_source=args.discovery_source,
        )
        print(f"Imported folder inventory: {args.inventory_file}")
        print(f"Counts: added={counts['added']}, skipped_existing_path={counts['skipped_existing_path']}")
        if args.report:
            print()
            print(render_entity_report(repo, args.campaign_uid), end="")


def cmd_import_figure_audits(args: argparse.Namespace) -> None:
    init_db(args.db)
    audit_files = discover_figure_audit_files(args.roots)
    with LiteRepository(args.db) as repo:
        counts = import_figure_audits(
            repo,
            audit_files,
            campaign_uid=args.campaign_uid,
            campaign_name=args.campaign_name,
            create_source_entities=not args.no_source_entities,
        )
        print("Imported figure audits:")
        for root in args.roots:
            print(f"  root: {root}")
        print(
            "Counts: "
            f"audits={counts['audits']}, "
            f"figures={counts['figures']}, "
            f"outputs={counts['outputs']}, "
            f"inputs={counts['inputs']}, "
            f"scripts={counts['scripts']}, "
            f"source_entities={counts['source_entities']}, "
            f"relations={counts['relations']}, "
            f"warnings={counts['warnings']}, "
            f"skipped={counts['skipped']}"
        )
        if args.report:
            print()
            print(render_entity_report(repo, args.campaign_uid), end="")


def cmd_refresh_campaign(args: argparse.Namespace) -> None:
    try:
        summary = refresh_campaign(args.profile, dry_run=args.dry_run)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print(format_refresh_summary(summary, dry_run=args.dry_run), end="")
    if summary.errors:
        raise SystemExit(1)


def cmd_campaign_status(args: argparse.Namespace) -> None:
    try:
        status = build_campaign_status_from_path(args.db_or_profile)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    print(format_campaign_status(status), end="")


def cmd_list(args: argparse.Namespace) -> None:
    with LiteRepository(resolve_database_path(args.db)) as repo:
        for entity in repo.list_entities():
            print(
                f"{entity['uid']:48s} {entity['type']:14s} "
                f"{entity['status']:18s} {entity['scientific_status']:14s} {entity['name']}"
            )


def cmd_report(args: argparse.Namespace) -> None:
    with LiteRepository(resolve_database_path(args.db)) as repo:
        text = render_entity_report(repo, args.entity)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        print(f"Wrote report: {output}")
    else:
        print(text, end="")


def cmd_serve(args: argparse.Namespace) -> None:
    serve(args.db, host=args.host, port=args.port)


def _validate_packet_or_exit(packet: Path) -> None:
    issues = validate_packet(packet)
    if issues:
        stream = sys.stderr if has_errors(issues) else sys.stdout
        print(format_issues(issues), end="", file=stream)
    if has_errors(issues):
        raise SystemExit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Datamak Lite campaign registry prototype")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Create or migrate a Datamak Lite SQLite database.")
    p_init.add_argument("db", type=Path)
    p_init.set_defaults(func=cmd_init)

    p_seed = sub.add_parser("seed-demo", help="Insert a generic demo dependency graph.")
    p_seed.add_argument("db", type=Path)
    p_seed.set_defaults(func=cmd_seed_demo)

    p_import = sub.add_parser("import-packet", help="Import a datamak_lite.json sidecar packet.")
    p_import.add_argument("db", type=Path)
    p_import.add_argument("packet", type=Path)
    p_import.set_defaults(func=cmd_import_packet)

    p_validate = sub.add_parser("validate-packet", help="Validate a datamak_lite.json sidecar packet.")
    p_validate.add_argument("packet", type=Path)
    p_validate.add_argument("--json", action="store_true", help="Print validation issues as JSON.")
    p_validate.set_defaults(func=cmd_validate_packet)

    p_sync = sub.add_parser("sync-packet", help="Copy a local or remote datamak_lite.json packet into the registry.")
    p_sync.add_argument("db", type=Path)
    p_sync.add_argument(
        "packet",
        help="Local packet path or scp-style remote path, for example user@host:/path/datamak_lite.json.",
    )
    p_sync.add_argument("--cache-dir", type=Path, help="Local packet cache directory. Defaults to DB_DIR/packets.")
    p_sync.add_argument("--report", action="store_true", help="Print the imported root entity report.")
    p_sync.add_argument("--dry-run", action="store_true", help="Show the local cache path without copying or importing.")
    p_sync.add_argument("--timeout", type=int, default=60, help="SCP timeout in seconds for remote packets.")
    p_sync.set_defaults(func=cmd_sync_packet)

    p_pool = sub.add_parser(
        "create-pool-packet",
        help="Create datamak_lite.json for a generic pool.",
    )
    p_pool.add_argument("pool_root", type=Path)
    p_pool.add_argument("--uid", help="Stable uid for the pool entity. Defaults to a slug from the folder name.")
    p_pool.add_argument("--name", help="Human-readable pool name. Defaults to the folder name.")
    p_pool.add_argument("--campaign-uid", help="Campaign uid to relate this pool to.")
    p_pool.add_argument("--campaign-name", help="Campaign name to include if --campaign-uid is set.")
    p_pool.add_argument("--uses-dataset-uid", help="Input dataset uid used by this pool.")
    p_pool.add_argument("--dataset-name", help="Human-readable name for the input dataset entity.")
    p_pool.add_argument("--dataset-path", help="Path to the input dataset used by this pool.")
    p_pool.add_argument("--dataset-type", default="dataset", help="Entity type for the input dataset.")
    p_pool.add_argument("--relation-type", default="uses_input", help="Relation type from pool to input dataset.")
    p_pool.add_argument("--status", default="prepared", help="Operational status for the pool entity.")
    p_pool.add_argument("--scientific-status", default="candidate", help="Scientific status for the pool entity.")
    p_pool.add_argument("--note", help="Markdown note attached to the pool.")
    p_pool.add_argument("--author", default="", help="Optional note author.")
    p_pool.add_argument("--output", type=Path, help="Packet output path. Defaults to POOL_ROOT/datamak_lite.json.")
    p_pool.add_argument("--dry-run", action="store_true", help="Print packet JSON without writing it.")
    p_pool.add_argument("--import-db", type=Path, help="Import the packet into this SQLite DB after writing it.")
    p_pool.add_argument("--report", action="store_true", help="Print the imported root entity report.")
    p_pool.set_defaults(func=cmd_create_pool_packet)

    p_marker = sub.add_parser(
        "create-pool-marker",
        help="Create README.md and datamak_pool.json for a Datamak-style pool.",
    )
    p_marker.add_argument("pool_root", type=Path)
    p_marker.add_argument("--pool-db", default="pool.db", help="Pool SQLite DB path relative to POOL_ROOT.")
    p_marker.add_argument(
        "--status-command",
        default="python3 pool_status.py",
        help="Command used to inspect pool status.",
    )
    p_marker.add_argument(
        "--interactive-launcher",
        default="./run_pool_interactive.sh",
        help="Command used to launch the pool interactively.",
    )
    p_marker.add_argument("--lite-sidecar", default="datamak_lite.json", help="Lite sidecar path.")
    p_marker.add_argument("--worker-model", default="sqlite_worker_pool", help="Datamak worker model name.")
    p_marker.add_argument("--recovery-command", help="Recovery command for interrupted RUNNING rows.")
    p_marker.add_argument("--note", help="Optional note to include in README.md and datamak_pool.json.")
    p_marker.add_argument("--no-readme", action="store_true", help="Only write datamak_pool.json.")
    p_marker.add_argument("--overwrite", action="store_true", help="Overwrite existing marker and README files.")
    p_marker.set_defaults(func=cmd_create_pool_marker)

    p_registry = sub.add_parser(
        "import-campaign-registry",
        help="Import a Datamak-style campaign registry into Datamak Lite.",
    )
    p_registry.add_argument("db", type=Path)
    p_registry.add_argument("registry_db", type=Path)
    p_registry.add_argument("--campaign-uid", default="campaign_default")
    p_registry.add_argument(
        "--campaign-name",
        default="Default campaign",
    )
    p_registry.add_argument("--report", action="store_true")
    p_registry.set_defaults(func=cmd_import_campaign_registry)

    p_inventory = sub.add_parser(
        "import-folder-inventory",
        help="Import a newline-delimited folder inventory as candidate Lite entities.",
    )
    p_inventory.add_argument("db", type=Path)
    p_inventory.add_argument("inventory_file", type=Path)
    p_inventory.add_argument("--campaign-uid", default="campaign_default")
    p_inventory.add_argument("--discovery-source", default="folder_inventory")
    p_inventory.add_argument("--report", action="store_true")
    p_inventory.set_defaults(func=cmd_import_folder_inventory)

    p_figures = sub.add_parser(
        "import-figure-audits",
        help="Import legacy plotting audit JSON files as Lite figure entities.",
    )
    p_figures.add_argument("db", type=Path)
    p_figures.add_argument(
        "roots",
        nargs="+",
        type=Path,
        help="Figure audit JSON files or directories to scan recursively.",
    )
    p_figures.add_argument("--campaign-uid", default="campaign_default")
    p_figures.add_argument(
        "--campaign-name",
        default="Default campaign",
    )
    p_figures.add_argument(
        "--no-source-entities",
        action="store_true",
        help="Do not auto-create analysis/source entities for unmatched input paths.",
    )
    p_figures.add_argument("--report", action="store_true")
    p_figures.set_defaults(func=cmd_import_figure_audits)

    p_refresh = sub.add_parser(
        "refresh-campaign",
        help="Refresh a campaign SQLite DB from a campaign profile JSON.",
    )
    p_refresh.add_argument("profile", type=Path)
    p_refresh.add_argument("--dry-run", action="store_true", help="Count configured sources without importing.")
    p_refresh.set_defaults(func=cmd_refresh_campaign)

    p_status = sub.add_parser(
        "campaign-status",
        help="Print a campaign-wide status summary from a SQLite DB or campaign profile.",
    )
    p_status.add_argument("db_or_profile", type=Path)
    p_status.set_defaults(func=cmd_campaign_status)

    p_list = sub.add_parser("list", help="List registered entities.")
    p_list.add_argument("db", type=Path, help="SQLite DB or campaign profile JSON.")
    p_list.set_defaults(func=cmd_list)

    p_report = sub.add_parser("report", help="Render a Markdown report for one entity.")
    p_report.add_argument("db", type=Path, help="SQLite DB or campaign profile JSON.")
    p_report.add_argument("--entity", default=FIGURE_UID, help="Entity uid to report.")
    p_report.add_argument("--output", type=Path, help="Write report to this file instead of stdout.")
    p_report.set_defaults(func=cmd_report)

    p_serve = sub.add_parser("serve", help="Start the read-only Datamak Lite web UI.")
    p_serve.add_argument("db", type=Path, help="SQLite DB or campaign profile JSON.")
    p_serve.add_argument("--host", default=DEFAULT_HOST)
    p_serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_serve.set_defaults(func=cmd_serve)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
