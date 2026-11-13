"""Index explicitly selected literature-review folders without editing them."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote

from datamak_lite.core.literature import register_paper, register_review


_CONTENT_DIRS = {"references", "web_pdfs", "paper_notes", "web_notes", "paper_audit", "reports", "slides"}
_TOPIC_FILES = {"topic_overview.md", "claim_bank.md", "working_positions.md", "open_questions.md"}
_DEPTHS = {"seed", "comparative", "deep"}
_DOI = re.compile(r"10\.\d{4,9}/[^\s<>\[\]{}]+", re.I)
_ARXIV = re.compile(r"(?:arxiv\s*:\s*|arxiv\.org/(?:abs|pdf)/)([\w.-]+(?:/\d+)?(?:v\d+)?)", re.I)


def _root(value: str | Path) -> Path:
    root = Path(value).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Review root is not a directory: {root}")
    return root


def _evidence(root: Path, filenames: Iterable[str], dirnames: Iterable[str]) -> list[str]:
    files = set(filenames)
    dirs = set(dirnames)
    evidence = sorted(files & _TOPIC_FILES)
    if "scope.md" in files:
        evidence.append("scope.md")
    evidence.extend(f"{name}/" for name in sorted(dirs) if name.lower() in {"references", "paper_notes"})
    for name in sorted(files):
        path = Path(name)
        if path.suffix.lower() in {".md", ".tex"} and "review" in path.stem.lower():
            evidence.append(name)
    return evidence


def discover_reviews(roots: Iterable[str | Path] | str | Path) -> list[dict[str, Any]]:
    """Discover review candidates only beneath the supplied directories.

    This does not register anything.  Report/slide/corpus folders are not
    independent review candidates.  A user may inspect the returned evidence
    before importing any candidate.
    """
    if isinstance(roots, (str, Path)):
        roots = [roots]
    found: dict[str, dict[str, Any]] = {}
    topic_roots: set[Path] = set()
    for value in roots:
        selected = _root(value)
        for dirname, dirnames, filenames in os.walk(selected, followlinks=False):
            current = Path(dirname)
            if "SKILL.md" in filenames:
                dirnames[:] = []
                continue
            if current.name.lower() in _CONTENT_DIRS:
                dirnames[:] = []
                continue
            evidence = _evidence(current, filenames, dirnames)
            strong = bool(set(filenames) & (_TOPIC_FILES | {"scope.md"})) or any(
                name.lower() in {"references", "paper_notes"} for name in dirnames
            )
            derivative = not strong and any(parent in topic_roots for parent in current.parents)
            if evidence and not derivative:
                key = str(current.resolve())
                found[key] = {"path": key, "name": current.name, "evidence": evidence}
                if strong:
                    topic_roots.add(current)
            dirnames[:] = sorted(
                name for name in dirnames
                if not name.startswith(".") and name.lower() not in _CONTENT_DIRS
                and not (current / name).is_symlink()
            )
    return sorted(found.values(), key=lambda item: item["path"])


def _value(text: str) -> str:
    value = text.strip().rstrip().strip("`").strip()
    # Metadata is often written as **Title:** value or **Title**: value.
    value = value.removeprefix("**").strip()
    return value.strip('"\'')


def _key(text: str) -> str:
    return re.sub(r"[\s_-]+", " ", text.replace("**", "").strip()).lower()


def _note_metadata(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    fields: dict[str, str] = {}
    heading = ""
    for line in text.splitlines():
        # Do not mistake evidence or scientific claims for bibliographic fields.
        if re.match(r"^\s*#{2,}\s", line):
            break
        match = re.match(r"^\s*#\s+(.+?)\s*#*\s*$", line)
        if match and not heading:
            heading = match.group(1).strip()
        match = re.match(r"^\s*(?:[-*]\s+)?([^:\n]{1,65}):\s*(.*)$", line)
        if match:
            value = _value(match.group(2))
            if value.casefold() not in {"", "...", "none", "n/a", "unknown", "<paper title>"}:
                fields[_key(match.group(1))] = value
    if heading:
        fields.setdefault("title", heading)
    return fields


def _path_value(value: str, note: Path, root: Path) -> Path | None:
    value = _value(value)
    if not value or value.lower() in {"...", "none", "n/a", "unknown"}:
        return None
    link = re.fullmatch(r"\[[^\]]*\]\((.+?)\)", value)
    if link:
        value = link.group(1).strip("<>")
    if value.startswith("file://"):
        value = value[7:]
    if "://" in value:
        return None
    path = Path(unquote(value)).expanduser()
    if path.is_absolute():
        return path.resolve()
    choices = list(dict.fromkeys(
        (base / path).resolve() for base in (note.parent, root, *root.parents)
    ))
    existing = [item for item in choices if item.is_file()]
    if len(existing) > 1:
        raise ValueError(f"Ambiguous relative PDF path {value!r} in {note}: {existing}")
    if existing:
        return existing[0]
    return next((item for item in choices if item.parent.is_dir()), choices[0])


def _papers(root: Path) -> tuple[list[dict[str, Any]], list[str], int, int]:
    gaps: list[str] = []
    pdfs: dict[str, str] = {}
    folders = [path for path in sorted(root.iterdir()) if path.is_dir()]
    for folder in folders:
        if folder.name.lower() in {"references", "web_pdfs"}:
            status = "provisional" if folder.name.lower() == "web_pdfs" else "canonical"
            for path in sorted(folder.rglob("*")):
                if path.is_file() and path.suffix.lower() == ".pdf":
                    pdfs[str(path.resolve())] = status
    by_stem: dict[str, list[str]] = {}
    for path in pdfs:
        by_stem.setdefault(Path(path).stem, []).append(path)
    used_pdfs: set[str] = set()
    records: list[dict[str, Any]] = []
    note_count = 0
    for folder in folders:
        if folder.name.lower() not in {"paper_notes", "web_notes"}:
            continue
        status = "provisional" if folder.name.lower() == "web_notes" else "canonical"
        for note in sorted(folder.rglob("*.md")):
            if not note.is_file():
                continue
            note_count += 1
            metadata = _note_metadata(note)
            title = metadata.get("title", "").strip()
            incomplete = not title or title in {"...", "<paper title>"}
            if incomplete:
                title = note.stem
                gaps.append(f"No bibliographic title in note: {note}")
            raw_path = next((metadata[key] for key in ("pdf path", "local path", "source pdf", "pdf") if metadata.get(key)), "")
            pdf = _path_value(raw_path, note, root) if raw_path else None
            if pdf is None:
                matches = by_stem.get(note.stem, [])
                if len(matches) == 1:
                    pdf = Path(matches[0])
                elif len(matches) > 1:
                    gaps.append(f"Ambiguous PDF basename for note: {note}")
            if pdf:
                used_pdfs.add(str(pdf))
                if not pdf.is_file():
                    gaps.append(f"PDF path in note is missing: {pdf} (recorded as {raw_path!r})")
            doi = metadata.get("doi", "")
            arxiv = metadata.get("arxiv", metadata.get("arxiv id", ""))
            citation = metadata.get("citation", "")
            if not doi:
                match = _DOI.search(citation)
                doi = match.group(0).rstrip(".,;)") if match else ""
            if not arxiv:
                match = _ARXIV.search(citation)
                arxiv = match.group(1).removesuffix(".pdf") if match else ""
            if not doi and not arxiv and not metadata.get("paper id"):
                gaps.append(f"No DOI/arXiv/Paper ID in note; identity relies on its local file paths: {note}")
            depth = metadata.get("review depth", "").lower()
            depth_match = re.fullmatch(r"(seed|comparative|deep)(?: pass)?(?:\s*,.*)?", depth)
            if depth_match:
                depth = depth_match.group(1)
            if depth and depth not in _DEPTHS:
                gaps.append(f"Unrecognized review depth {depth!r} in {note}; indexed as unknown")
                depth = ""
            records.append({
                "title": title,
                "authors": metadata.get("authors", ""),
                "year": metadata.get("year", ""),
                "doi": doi,
                "arxiv": arxiv,
                "uid": metadata.get("paper id") or None,
                "pdf_path": str(pdf) if pdf else None,
                "note_path": str(note.resolve()),
                "depth": depth,
                "corpus_status": status,
                "metadata_incomplete": incomplete,
            })
    for path, status in pdfs.items():
        if path not in used_pdfs:
            records.append({
                "title": Path(path).stem,
                "authors": "", "year": "", "doi": "", "arxiv": "", "uid": None,
                "pdf_path": path, "note_path": None, "depth": "",
                "corpus_status": status, "metadata_incomplete": True,
            })
            gaps.append(f"PDF has filename-only metadata and unknown review depth: {path}")
    return records, gaps, note_count, len(pdfs)


def import_review(
    root: str | Path,
    *,
    db_path: str | Path | None = None,
    name: str | None = None,
    uid: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Register a review and its explicit paper records, preserving all files.

    A dry run returns the same metadata plan without creating a database.
    Corpus PDFs without notes are candidates with unknown review depth; their
    filenames are not treated as verified bibliographic metadata.
    """
    selected = _root(root)
    records, gaps, note_count, pdf_count = _papers(selected)
    if (selected / "paper_audit").is_dir() or (selected / "source_manifest.json").is_file():
        gaps.append("Legacy paper_audit/source_manifest records are not imported automatically; only standard note/corpus folders are indexed.")
    scope_path = selected / "scope.md"
    scope = scope_path.read_text(encoding="utf-8", errors="replace").strip() if scope_path.is_file() else ""
    if not records:
        gaps.append("No paper records found in paper_notes/, web_notes/, References/, or web_pdfs/. The review folder can still be registered.")
    result: dict[str, Any] = {
        "path": str(selected), "name": name or selected.name, "dry_run": dry_run,
        "review_uid": uid, "papers": records, "gaps": gaps,
        "counts": {"notes": note_count, "pdfs": pdf_count, "paper_records": len(records),
                   "metadata_incomplete": sum(bool(item["metadata_incomplete"]) for item in records),
                   "missing_identifiers": sum(not (item["doi"] or item["arxiv"] or item["uid"]) for item in records)},
    }
    if dry_run:
        return result
    review = register_review(selected, name=name, uid=uid, scope=scope, db_path=db_path)
    result["review_uid"] = review["uid"]
    imported: set[str] = set()
    for record in records:
        paper = register_paper(review["uid"], db_path=db_path, **record)
        imported.add(paper["uid"])
        record["uid"] = paper["uid"]
    result["counts"]["papers_indexed"] = len(imported)
    return result
