"""A metadata-only literature catalogue using Datamak Lite's generic graph.

Paper identity is shared; review depth, corpus membership, and note locations
belong to each review's relationship to that paper. Nothing here edits source
reviews or copies PDF files.
"""
from __future__ import annotations

import json
import re
import sqlite3
import uuid
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from .db import connect, init_db
from .user_config import default_config_dir


def default_literature_path(db_path: str | Path | None = None) -> Path:
    return Path(db_path).expanduser() if db_path is not None else default_config_dir() / "literature.sqlite"


def normalize_doi(value: str) -> str:
    """Normalize a DOI or DOI URL without guessing from titles or citations."""
    value = unquote(str(value or "").strip())
    value = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi\s*:\s*)", "", value, flags=re.I)
    return value.strip().lower()


def normalize_arxiv(value: str) -> tuple[str, str]:
    """Return the arXiv base ID and optional version (for example ``v2``)."""
    value = unquote(str(value or "").strip())
    value = re.sub(r"^(?:https?://(?:www\.)?arxiv\.org/(?:abs|pdf)/|arxiv\s*:\s*)", "", value, flags=re.I)
    value = re.sub(r"\.pdf$", "", value, flags=re.I).strip().lower()
    match = re.fullmatch(r"(.+?)(v\d+)?", value)
    return (match.group(1), match.group(2) or "") if match else ("", "")


def _path(value: str | Path) -> str:
    text = str(value)
    if "://" in text:
        return text
    return str(Path(value).expanduser().resolve())


def _metadata(row: sqlite3.Row) -> dict[str, Any]:
    return json.loads(row["metadata_json"])


def _uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _read_connection(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def register_review(
    root: str | Path,
    name: str | None = None,
    uid: str | None = None,
    scope: str = "",
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Register a review directory, preserving its files and existing UID.

    Re-register the same path to refresh its name/scope. Supply its existing UID
    explicitly to update the location after moving a review.
    """
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Review directory does not exist: {root}")
    path = default_literature_path(db_path)
    init_db(path)
    conn = connect(path)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = list(conn.execute("SELECT * FROM entity WHERE type='literature_review'"))
            matches = [row for row in rows if row["path"] == str(root) or (uid and row["uid"] == uid)]
            if len(matches) > 1 or (matches and uid and matches[0]["uid"] != uid):
                raise ValueError("Review UID and directory refer to different catalogue entries")
            previous = matches[0] if matches else None
            review_uid = previous["uid"] if previous else uid or _uid("review")
            occupied = conn.execute("SELECT type FROM entity WHERE uid=?", (review_uid,)).fetchone()
            if occupied and occupied["type"] != "literature_review":
                raise ValueError(f"UID already belongs to another entity type: {review_uid}")
            metadata = _metadata(previous) if previous else {}
            if scope:
                metadata["scope"] = scope
            review_name = name or (previous["name"] if previous else root.name)
            conn.execute(
                """INSERT INTO entity(uid,type,name,path,status,scientific_status,metadata_json)
                VALUES (?,'literature_review',?,?,'indexed','unassessed',?)
                ON CONFLICT(uid) DO UPDATE SET name=excluded.name,path=excluded.path,
                metadata_json=excluded.metadata_json,updated_at=datetime('now')""",
                (review_uid, review_name, str(root), json.dumps(metadata, sort_keys=True)),
            )
        return next(item for item in list_reviews(path) if item["uid"] == review_uid)
    finally:
        conn.close()


def _artifact(conn: sqlite3.Connection, paper_id: int, kind: str, path: str) -> None:
    local = Path(path)
    stat = local.stat() if "://" not in path and local.exists() else None
    conn.execute(
        """INSERT INTO artifact(entity_id,kind,path,format,exists_on_disk,size_bytes,mtime)
        VALUES (?,?,?,?,?,?,datetime(?,'unixepoch'))
        ON CONFLICT(entity_id,kind,path) DO UPDATE SET
        exists_on_disk=excluded.exists_on_disk,size_bytes=excluded.size_bytes,mtime=excluded.mtime""",
        (paper_id, kind, path, local.suffix.lstrip("."), int(stat is not None),
         stat.st_size if stat else None, stat.st_mtime if stat else None),
    )


def register_paper(
    review_uid: str,
    *,
    title: str,
    authors: str = "",
    year: str = "",
    doi: str = "",
    arxiv: str = "",
    uid: str | None = None,
    pdf_path: str | Path | None = None,
    note_path: str | Path | None = None,
    depth: str = "",
    corpus_status: str | None = None,
    metadata_incomplete: bool | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Index one paper and its use in a review, without reading its content.

    Identity uses an explicit UID, DOI, arXiv base ID, or an already indexed
    exact PDF/note location. Title similarity never merges papers. Conflicting
    identifiers fail atomically; no existing identity is silently overwritten.
    ``metadata_incomplete`` marks filename-only or unresolved title labels,
    rather than the absence of optional bibliographic fields. Omitting
    ``corpus_status`` preserves existing membership and uses canonical for new
    memberships.
    """
    if not str(title).strip():
        raise ValueError("Paper title is required (a filename-derived label is allowed)")
    doi = normalize_doi(doi)
    arxiv, version = normalize_arxiv(arxiv)
    if doi and not re.fullmatch(r"10\.\d{4,9}/\S+", doi):
        raise ValueError(f"Expected a DOI or DOI URL, got: {doi}")
    if arxiv and not re.fullmatch(r"(?:\d{4}\.\d{4,5}|[a-z][a-z0-9.-]*/\d{7})", arxiv):
        raise ValueError(f"Expected an arXiv identifier or URL, got: {arxiv}")
    paths = {kind: _path(value) for kind, value in (("pdf", pdf_path), ("paper_note", note_path)) if value}
    path = default_literature_path(db_path)
    if not path.exists():
        raise ValueError("Register the review before adding its papers")
    conn = connect(path)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            review = conn.execute("SELECT * FROM entity WHERE uid=? AND type='literature_review'", (review_uid,)).fetchone()
            if review is None:
                raise ValueError(f"Unknown literature review: {review_uid}")
            papers = list(conn.execute("SELECT * FROM entity WHERE type='paper'"))
            matches: dict[int, sqlite3.Row] = {}
            for row in papers:
                metadata = _metadata(row)
                if ((uid and row["uid"] == uid) or (doi and metadata.get("doi") == doi)
                        or (arxiv and metadata.get("arxiv") == arxiv)):
                    matches[row["id"]] = row
            for kind, location in paths.items():
                for row in conn.execute(
                    """SELECT e.* FROM entity e JOIN artifact a ON a.entity_id=e.id
                    WHERE e.type='paper' AND a.kind=? AND a.path=?""", (kind, location)
                ):
                    matches[row["id"]] = row
            if len(matches) > 1:
                raise ValueError("Identifiers or file locations match multiple papers; resolve the identity before registering")
            previous = next(iter(matches.values()), None)
            if previous is not None and uid and previous["uid"] != uid:
                raise ValueError("Explicit paper UID conflicts with an existing identifier or location")
            metadata = _metadata(previous) if previous is not None else {}
            for key, value in (("doi", doi), ("arxiv", arxiv)):
                if value and metadata.get(key) and metadata[key] != value:
                    raise ValueError(f"Conflicting {key} for an existing paper; index versions separately")
            paper_uid = previous["uid"] if previous is not None else uid or _uid("paper")
            occupied = conn.execute("SELECT type FROM entity WHERE uid=?", (paper_uid,)).fetchone()
            if occupied and occupied["type"] != "paper":
                raise ValueError(f"UID already belongs to another entity type: {paper_uid}")
            for key, value in (("authors", authors), ("year", str(year) if year else ""), ("doi", doi), ("arxiv", arxiv)):
                if value:
                    metadata[key] = value
            if version:
                metadata["arxiv_versions"] = sorted(set(metadata.get("arxiv_versions", [])) | {version})
            # A later filename-only import should not replace a richer title.
            paper_title = str(title).strip()
            if previous is not None and metadata_incomplete is True and not metadata.get("metadata_incomplete", True):
                paper_title = previous["name"]
            if metadata_incomplete is not None:
                metadata["metadata_incomplete"] = bool(metadata_incomplete and metadata.get("metadata_incomplete", True))
            elif doi or arxiv or authors or year:
                metadata["metadata_incomplete"] = False
            conn.execute(
                """INSERT INTO entity(uid,type,name,status,scientific_status,metadata_json)
                VALUES (?,'paper',?,'indexed','unassessed',?)
                ON CONFLICT(uid) DO UPDATE SET name=excluded.name,
                metadata_json=excluded.metadata_json,updated_at=datetime('now')""",
                (paper_uid, paper_title, json.dumps(metadata, sort_keys=True)),
            )
            paper_id = conn.execute("SELECT id FROM entity WHERE uid=?", (paper_uid,)).fetchone()["id"]
            relation = conn.execute(
                "SELECT * FROM relation WHERE source_entity_id=? AND target_entity_id=? AND relation_type='reviews_paper'",
                (review["id"], paper_id),
            ).fetchone()
            membership = _metadata(relation) if relation is not None else {}
            if depth:
                membership["depth"] = depth
            if corpus_status:
                membership["corpus_status"] = corpus_status
            elif not membership.get("corpus_status"):
                membership["corpus_status"] = "canonical"
            for kind, location in paths.items():
                _artifact(conn, paper_id, kind, location)
                key = "pdf_paths" if kind == "pdf" else "note_paths"
                membership[key] = sorted(set(membership.get(key, [])) | {location})
            conn.execute(
                """INSERT INTO relation(source_entity_id,relation_type,target_entity_id,metadata_json)
                VALUES (?,'reviews_paper',?,?)
                ON CONFLICT(source_entity_id,relation_type,target_entity_id)
                DO UPDATE SET metadata_json=excluded.metadata_json""",
                (review["id"], paper_id, json.dumps(membership, sort_keys=True)),
            )
        return next(item for item in find_papers(db_path=path) if item["uid"] == paper_uid)
    finally:
        conn.close()


def list_reviews(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    conn = _read_connection(default_literature_path(db_path))
    if conn is None:
        return []
    try:
        return [
            {"uid": row["uid"], "name": row["name"], "path": row["path"],
             "scope": _metadata(row).get("scope", ""), "paper_count": row["paper_count"]}
            for row in conn.execute(
                """SELECT e.*, (SELECT COUNT(*) FROM relation r
                WHERE r.source_entity_id=e.id AND r.relation_type='reviews_paper') AS paper_count
                FROM entity e WHERE e.type='literature_review' ORDER BY e.name,e.uid"""
            )
        ]
    finally:
        conn.close()


def find_papers(query: str = "", db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Search indexed bibliographic metadata and review names, never PDF text."""
    conn = _read_connection(default_literature_path(db_path))
    if conn is None:
        return []
    try:
        results = []
        needles = {query.strip().casefold(), normalize_doi(query).casefold()}
        if re.match(r"^(?:arxiv:|https?://(?:www\.)?arxiv\.org/)", query.strip(), re.I):
            needles.add(normalize_arxiv(query)[0])
        for row in conn.execute("SELECT * FROM entity WHERE type='paper' ORDER BY name,uid"):
            metadata = _metadata(row)
            paper = {"uid": row["uid"], "title": row["name"],
                     **{key: metadata.get(key, "") for key in ("authors", "year", "doi", "arxiv")},
                     "arxiv_versions": metadata.get("arxiv_versions", []),
                     "metadata_incomplete": metadata.get("metadata_incomplete", False)}
            paper["artifacts"] = [dict(artifact) for artifact in conn.execute(
                "SELECT kind,path,format,exists_on_disk,size_bytes FROM artifact WHERE entity_id=? ORDER BY kind,path", (row["id"],)
            )]
            paper["reviews"] = []
            for membership in conn.execute(
                """SELECT r.metadata_json,e.uid,e.name,e.path FROM relation r
                JOIN entity e ON e.id=r.source_entity_id
                WHERE r.target_entity_id=? AND r.relation_type='reviews_paper' AND e.type='literature_review'
                ORDER BY e.name,e.uid""", (row["id"],)
            ):
                fields = _metadata(membership)
                paper["reviews"].append({
                    "uid": membership["uid"], "name": membership["name"], "path": membership["path"],
                    "depth": fields.get("depth", ""), "corpus_status": fields.get("corpus_status", ""),
                    "note_paths": fields.get("note_paths", []), "pdf_paths": fields.get("pdf_paths", []),
                })
            searchable = json.dumps(paper, ensure_ascii=False).casefold()
            if not query.strip() or any(needle and needle in searchable for needle in needles):
                results.append(paper)
        return results
    finally:
        conn.close()


def find_duplicates(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Suggest title-based duplicate candidates; never merge or remove papers."""
    papers = find_papers(db_path=db_path)
    pairs = []
    for index, paper in enumerate(papers):
        title = " ".join(re.findall(r"\w+", paper["title"].casefold()))
        if len(title) < 12:
            continue
        for other in papers[index + 1:]:
            other_title = " ".join(re.findall(r"\w+", other["title"].casefold()))
            score = SequenceMatcher(None, title, other_title).ratio()
            if score >= 0.92:
                pairs.append({"paper_uid": paper["uid"], "other_uid": other["uid"],
                              "title": paper["title"], "other_title": other["title"],
                              "reason": "similar_title", "score": round(score, 3)})
    return pairs
