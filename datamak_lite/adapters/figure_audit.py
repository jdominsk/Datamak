from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from datamak_lite.core.repository import LiteRepository


DEFAULT_CAMPAIGN_UID = "campaign_default"
DEFAULT_CAMPAIGN_NAME = "Default campaign"


def discover_figure_audit_files(roots: Iterable[str | Path]) -> list[Path]:
    """Discover legacy figure audit JSON files under the provided roots."""

    files: list[Path] = []
    for root in roots:
        path = Path(root)
        if path.is_file() and path.suffix.lower() == ".json":
            files.append(path)
        elif path.is_dir():
            files.extend(path.rglob("*.json"))
    return sorted(_is_candidate_audit(path) for path in files if _is_candidate_audit(path))


def import_figure_audits(
    repo: LiteRepository,
    audit_files: Iterable[str | Path],
    *,
    campaign_uid: str = DEFAULT_CAMPAIGN_UID,
    campaign_name: str = DEFAULT_CAMPAIGN_NAME,
    create_source_entities: bool = True,
) -> dict[str, int]:
    """Import legacy figure audit JSON files as Lite figure entities.

    The existing campaign already has many audit JSON files written by plotting
    scripts.  They are not Datamak Lite packets yet, so this importer maps them
    conservatively:

    - one figure entity per audit JSON;
    - audit JSON, output PNG/PDF, script, and input paths as artifacts;
    - optional source-analysis entities for input data paths;
    - warnings when audit text mentions absolute-value D(ky) spectra.
    """

    repo.upsert_entity(
        uid=campaign_uid,
        type="campaign",
        name=campaign_name,
        status="active",
        scientific_status="candidate",
        description="Campaign root used for imported figure audit metadata.",
    )

    counts = {
        "audits": 0,
        "figures": 0,
        "outputs": 0,
        "inputs": 0,
        "scripts": 0,
        "source_entities": 0,
        "relations": 0,
        "warnings": 0,
        "skipped": 0,
    }
    for raw_path in audit_files:
        audit_path = Path(raw_path)
        try:
            data = json.loads(audit_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            counts["skipped"] += 1
            continue
        if not isinstance(data, dict):
            counts["skipped"] += 1
            continue

        imported = _import_one_audit(
            repo,
            audit_path,
            data,
            campaign_uid=campaign_uid,
            create_source_entities=create_source_entities,
        )
        for key, value in imported.items():
            counts[key] += value
    return counts


def _import_one_audit(
    repo: LiteRepository,
    audit_path: Path,
    data: dict[str, Any],
    *,
    campaign_uid: str,
    create_source_entities: bool,
) -> dict[str, int]:
    counts = {
        "audits": 1,
        "figures": 1,
        "outputs": 0,
        "inputs": 0,
        "scripts": 0,
        "source_entities": 0,
        "relations": 0,
        "warnings": 0,
        "skipped": 0,
    }
    output_paths = _dedupe(_output_paths(data))
    script_paths = _dedupe(_script_paths(data))
    input_paths = _dedupe(_input_paths(data, output_paths=output_paths, script_paths=script_paths))

    primary_output = output_paths[0] if output_paths else str(audit_path)
    uid = _figure_uid(audit_path, primary_output)
    name = _human_name(Path(primary_output).stem if output_paths else audit_path.stem)
    status = "available" if _path_exists(primary_output, audit_path.parent) else "unknown"
    metadata = {
        "source": "legacy_figure_audit",
        "audit_json": str(audit_path),
        "keys": sorted(data.keys()),
        "n_outputs": len(output_paths),
        "n_inputs": len(input_paths),
    }
    repo.upsert_entity(
        uid=uid,
        type="figure",
        name=name,
        path=primary_output,
        status=status,
        scientific_status="candidate",
        description="Figure imported from a legacy audit JSON sidecar.",
        metadata=metadata,
    )
    repo.add_relation(source_uid=uid, relation_type="member_of", target_uid=campaign_uid)
    counts["relations"] += 1
    repo.add_artifact(
        entity_uid=uid,
        kind="audit_json",
        path=str(audit_path),
        format="json",
        description="Legacy figure audit JSON.",
    )

    for path in output_paths:
        repo.add_artifact(
            entity_uid=uid,
            kind="figure_output",
            path=path,
            format=_guess_format(path),
            description="Figure output referenced by audit JSON.",
        )
        counts["outputs"] += 1

    for path in script_paths:
        repo.add_artifact(
            entity_uid=uid,
            kind="plot_script",
            path=path,
            format="python" if str(path).endswith(".py") else _guess_format(path),
            description="Script referenced by figure audit JSON.",
        )
        counts["scripts"] += 1

    for path in input_paths:
        matched_uid = _find_entity_for_path(repo, path)
        if matched_uid:
            repo.add_relation(
                source_uid=uid,
                relation_type="plots",
                target_uid=matched_uid,
                note="Matched from a source path in the figure audit JSON.",
            )
            counts["relations"] += 1
        elif create_source_entities:
            source_uid = _source_entity_uid(path)
            source_path = _source_entity_path(path)
            repo.upsert_entity(
                uid=source_uid,
                type=_source_entity_type(path),
                name=_human_name(Path(source_path).name or Path(path).stem),
                path=source_path,
                status="unknown",
                scientific_status="candidate",
                description="Auto-created source object from a figure audit input path.",
                metadata={"source": "figure_audit_input_path"},
            )
            repo.add_relation(source_uid=source_uid, relation_type="member_of", target_uid=campaign_uid)
            repo.add_relation(
                source_uid=uid,
                relation_type="plots",
                target_uid=source_uid,
                note="Auto-created from a source path in the figure audit JSON.",
            )
            counts["relations"] += 2
            counts["source_entities"] += 1
            matched_uid = source_uid

        repo.add_artifact(
            entity_uid=matched_uid or uid,
            kind="source_data" if matched_uid else "unmatched_source_data",
            path=path,
            format=_guess_format(path),
            description="Input data path referenced by figure audit JSON.",
        )
        counts["inputs"] += 1

    if _mentions_abs_diffusion(data):
        repo.add_note(
            entity_uid=uid,
            note_type="warning",
            markdown_text=(
                "Audit text mentions an absolute-value diffusion spectrum. "
                "Verify this figure uses 2 Re[D(ky)], not |D(ky)|, unless it is explicitly a test."
            ),
        )
        counts["warnings"] += 1

    return counts


def _is_candidate_audit(path: Path) -> Path | None:
    name = path.name.lower()
    if name == "datamak_lite.json":
        return None
    if name.endswith(("_audit.json", "_summary.json")):
        return path
    if "figure" in str(path.parent).lower():
        return path
    return None


def _output_paths(data: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key, value in _walk(data):
        lower = key.lower()
        if "output" not in lower and "figure" not in lower:
            continue
        for item in _as_strings(value):
            if _suffix(item) in {".png", ".pdf", ".svg", ".jpg", ".jpeg"}:
                paths.append(item)
    return paths


def _script_paths(data: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key, value in _walk(data):
        lower = key.lower()
        if "script" not in lower:
            continue
        for item in _as_strings(value):
            if _looks_like_path(item):
                paths.append(item)
    return paths


def _input_paths(data: dict[str, Any], *, output_paths: list[str], script_paths: list[str]) -> list[str]:
    excluded = set(output_paths) | set(script_paths)
    paths: list[str] = []
    for key, value in _walk(data):
        lower = key.lower()
        if any(token in lower for token in ("output", "script")):
            continue
        for item in _as_strings(value):
            if item in excluded or not _looks_like_path(item):
                continue
            if _suffix(item) in {".csv", ".json", ".nc", ".h5", ".hdf5", ".toml", ".npz", ".txt"}:
                paths.append(item)
    return paths


def _walk(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    if isinstance(value, dict):
        items: list[tuple[str, Any]] = []
        for key, subvalue in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            items.extend(_walk(subvalue, next_prefix))
        return items
    return [(prefix, value)]


def _as_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    return []


def _looks_like_path(value: str) -> bool:
    if value.startswith(("http://", "https://", "datafed:", "globus:")):
        return False
    if re.search(r"\s", value) and not Path(value).expanduser().exists():
        return False
    return value.startswith(("/", "~")) or "/" in value or "\\" in value


def _dedupe(paths: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for path in paths:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


def _find_entity_for_path(repo: LiteRepository, path: str) -> str | None:
    candidates = _path_variants(path)
    for candidate in candidates:
        row = repo.conn.execute("SELECT uid FROM entity WHERE path=?", (candidate,)).fetchone()
        if row:
            return str(row["uid"])
        row = repo.conn.execute(
            """
            SELECT e.uid
            FROM artifact a
            JOIN entity e ON e.id = a.entity_id
            WHERE a.path=?
            """,
            (candidate,),
        ).fetchone()
        if row:
            return str(row["uid"])
    return None


def _path_variants(path: str) -> list[str]:
    variants = [path]
    p = Path(path).expanduser()
    if not p.is_absolute():
        variants.append(str(p))
    try:
        variants.append(str(p.resolve()))
    except OSError:
        pass
    return _dedupe(variants)


def _source_entity_uid(path: str) -> str:
    return _slug(f"source_{_source_entity_path(path)}")


def _source_entity_path(path: str) -> str:
    p = Path(path)
    parts = p.parts
    if "analysis" in parts:
        index = parts.index("analysis")
        if len(parts) > index + 1:
            return str(Path(*parts[: index + 2]))
    if "figure" in parts:
        index = parts.index("figure")
        return str(Path(*parts[: index + 1]))
    if "replay" in parts:
        index = parts.index("replay")
        if len(parts) > index + 1:
            return str(Path(*parts[: index + 2]))
    return str(p.parent)


def _source_entity_type(path: str) -> str:
    lower = path.lower()
    if "/figure/" in lower or "\\figure\\" in lower:
        return "analysis"
    if "/analysis/" in lower or "\\analysis\\" in lower:
        return "analysis"
    if "/replay/" in lower or "\\replay\\" in lower:
        return "pool"
    return "analysis"


def _figure_uid(audit_path: Path, primary_output: str) -> str:
    stem = Path(primary_output).stem if primary_output else audit_path.stem
    for suffix in ("_audit", "_plot_audit", "_summary"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return _slug(f"figure_{stem}")


def _human_name(text: str) -> str:
    clean = re.sub(r"[_\-]+", " ", text).strip()
    return clean or "Figure"


def _mentions_abs_diffusion(data: dict[str, Any]) -> bool:
    text = json.dumps(data, sort_keys=True).lower()
    return any(
        token in text
        for token in (
            "abs(d_ky",
            "abs[d_ky",
            "abs(dky",
            "abs[dky",
            "|d(ky)|",
            "|d_ky|",
            "absolute-value diffusion",
            "2*abs",
            "2 * abs",
        )
    )


def _path_exists(path: str, audit_parent: Path) -> bool:
    p = Path(path).expanduser()
    if p.exists():
        return True
    if not p.is_absolute() and (audit_parent / p).exists():
        return True
    return False


def _guess_format(path: str) -> str | None:
    suffix = _suffix(path)
    return {
        ".csv": "csv",
        ".json": "json",
        ".nc": "netcdf",
        ".h5": "hdf5",
        ".hdf5": "hdf5",
        ".png": "png",
        ".pdf": "pdf",
        ".svg": "svg",
        ".py": "python",
        ".toml": "toml",
        ".npz": "npz",
        ".txt": "text",
    }.get(suffix)


def _suffix(path: str) -> str:
    return Path(path.split("?", 1)[0]).suffix.lower()


def _slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "datamak_lite_object"
