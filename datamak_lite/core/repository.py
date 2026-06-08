from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .db import connect, init_db


def _json(data: dict[str, Any] | None) -> str:
    return json.dumps(data or {}, sort_keys=True)


class LiteRepository:
    """Small repository wrapper around the code-agnostic Datamak Lite schema."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            init_db(self.db_path)
        self.conn = connect(self.db_path)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "LiteRepository":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def upsert_entity(
        self,
        *,
        uid: str,
        type: str,
        name: str,
        path: str | None = None,
        status: str = "unknown",
        scientific_status: str = "candidate",
        description: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        self.conn.execute(
            """
            INSERT INTO entity (
                uid, type, name, path, status, scientific_status,
                description, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                type=excluded.type,
                name=excluded.name,
                path=excluded.path,
                status=excluded.status,
                scientific_status=excluded.scientific_status,
                description=excluded.description,
                metadata_json=excluded.metadata_json,
                updated_at=datetime('now')
            """,
            (uid, type, name, path, status, scientific_status, description, _json(metadata)),
        )
        self.conn.commit()
        return int(self.get_entity(uid)["id"])

    def get_entity(self, uid_or_id: str | int) -> sqlite3.Row:
        if isinstance(uid_or_id, int):
            row = self.conn.execute("SELECT * FROM entity WHERE id=?", (uid_or_id,)).fetchone()
        else:
            row = self.conn.execute("SELECT * FROM entity WHERE uid=?", (uid_or_id,)).fetchone()
        if row is None:
            raise KeyError(f"Unknown entity: {uid_or_id}")
        return row

    def list_entities(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM entity ORDER BY type, name"))

    def add_relation(
        self,
        *,
        source_uid: str,
        relation_type: str,
        target_uid: str,
        note: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        source_id = int(self.get_entity(source_uid)["id"])
        target_id = int(self.get_entity(target_uid)["id"])
        self.conn.execute(
            """
            INSERT INTO relation (
                source_entity_id, relation_type, target_entity_id, note, metadata_json
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_entity_id, relation_type, target_entity_id)
            DO UPDATE SET note=excluded.note, metadata_json=excluded.metadata_json
            """,
            (source_id, relation_type, target_id, note, _json(metadata)),
        )
        self.conn.commit()
        row = self.conn.execute(
            """
            SELECT id FROM relation
            WHERE source_entity_id=? AND relation_type=? AND target_entity_id=?
            """,
            (source_id, relation_type, target_id),
        ).fetchone()
        return int(row["id"])

    def outgoing_relations(self, entity_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT r.*, e.uid AS target_uid, e.name AS target_name, e.type AS target_type
                FROM relation r
                JOIN entity e ON e.id = r.target_entity_id
                WHERE r.source_entity_id=?
                ORDER BY r.relation_type, e.name
                """,
                (entity_id,),
            )
        )

    def incoming_relations(self, entity_id: int) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT r.*, e.uid AS source_uid, e.name AS source_name, e.type AS source_type
                FROM relation r
                JOIN entity e ON e.id = r.source_entity_id
                WHERE r.target_entity_id=?
                ORDER BY r.relation_type, e.name
                """,
                (entity_id,),
            )
        )

    def add_artifact(
        self,
        *,
        entity_uid: str,
        kind: str,
        path: str,
        format: str | None = None,
        description: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> int:
        entity_id = int(self.get_entity(entity_uid)["id"])
        file_path = Path(path).expanduser()
        exists = file_path.exists()
        stat = file_path.stat() if exists else None
        self.conn.execute(
            """
            INSERT INTO artifact (
                entity_id, kind, path, format, description, exists_on_disk,
                size_bytes, mtime, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'), ?)
            ON CONFLICT(entity_id, kind, path) DO UPDATE SET
                format=excluded.format,
                description=excluded.description,
                exists_on_disk=excluded.exists_on_disk,
                size_bytes=excluded.size_bytes,
                mtime=excluded.mtime,
                metadata_json=excluded.metadata_json
            """,
            (
                entity_id,
                kind,
                path,
                format,
                description,
                int(exists),
                stat.st_size if stat else None,
                stat.st_mtime if stat else None,
                _json(metadata),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id FROM artifact WHERE entity_id=? AND kind=? AND path=?",
            (entity_id, kind, path),
        ).fetchone()
        return int(row["id"])

    def artifacts_for_entity(self, entity_id: int) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM artifact WHERE entity_id=? ORDER BY kind, path", (entity_id,)))

    def add_metric(
        self,
        *,
        entity_uid: str,
        name: str,
        value: float | None,
        unit: str | None = None,
        context: dict[str, Any] | None = None,
        source_artifact_id: int | None = None,
    ) -> int:
        entity_id = int(self.get_entity(entity_uid)["id"])
        context_json = _json(context)
        self.conn.execute(
            """
            INSERT INTO metric (entity_id, name, value, unit, context_json, source_artifact_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id, name, context_json) DO UPDATE SET
                value=excluded.value,
                unit=excluded.unit,
                source_artifact_id=excluded.source_artifact_id
            """,
            (entity_id, name, value, unit, context_json, source_artifact_id),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id FROM metric WHERE entity_id=? AND name=? AND context_json=?",
            (entity_id, name, context_json),
        ).fetchone()
        return int(row["id"])

    def metrics_for_entity(self, entity_id: int) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM metric WHERE entity_id=? ORDER BY name", (entity_id,)))

    def add_note(
        self,
        *,
        markdown_text: str,
        entity_uid: str | None = None,
        relation_id: int | None = None,
        author: str = "",
        note_type: str = "comment",
    ) -> int:
        entity_id = int(self.get_entity(entity_uid)["id"]) if entity_uid else None
        existing = self.conn.execute(
            """
            SELECT id FROM note
            WHERE (entity_id IS ? OR entity_id = ?)
              AND (relation_id IS ? OR relation_id = ?)
              AND author = ?
              AND note_type = ?
              AND markdown_text = ?
            """,
            (entity_id, entity_id, relation_id, relation_id, author, note_type, markdown_text),
        ).fetchone()
        if existing is not None:
            return int(existing["id"])
        cur = self.conn.execute(
            """
            INSERT INTO note (entity_id, relation_id, author, note_type, markdown_text)
            VALUES (?, ?, ?, ?, ?)
            """,
            (entity_id, relation_id, author, note_type, markdown_text),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def notes_for_entity(self, entity_id: int) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM note WHERE entity_id=? ORDER BY created_at, id", (entity_id,)))
