from __future__ import annotations

import sqlite3
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA = REPO_ROOT / "schema" / "datamak_lite_schema.sql"


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a Datamak Lite SQLite database with useful defaults."""
    conn = sqlite3.connect(Path(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db(db_path: str | Path, schema_path: str | Path = DEFAULT_SCHEMA) -> Path:
    """Create or migrate a Datamak Lite database in place."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = Path(schema_path).read_text()
    conn = connect(db_path)
    try:
        conn.executescript(schema)
        conn.commit()
    finally:
        conn.close()
    return db_path
