PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS entity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    scientific_status TEXT NOT NULL DEFAULT 'candidate',
    description TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS relation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_entity_id INTEGER NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,
    target_entity_id INTEGER NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
    note TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_entity_id, relation_type, target_entity_id)
);

CREATE TABLE IF NOT EXISTS artifact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    format TEXT,
    description TEXT NOT NULL DEFAULT '',
    exists_on_disk INTEGER,
    size_bytes INTEGER,
    mtime TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(entity_id, kind, path)
);

CREATE TABLE IF NOT EXISTS metric (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    value REAL,
    unit TEXT,
    context_json TEXT NOT NULL DEFAULT '{}',
    source_artifact_id INTEGER REFERENCES artifact(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(entity_id, name, context_json)
);

CREATE TABLE IF NOT EXISTS note (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER REFERENCES entity(id) ON DELETE CASCADE,
    relation_id INTEGER REFERENCES relation(id) ON DELETE CASCADE,
    author TEXT NOT NULL DEFAULT '',
    note_type TEXT NOT NULL DEFAULT 'comment',
    markdown_text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (entity_id IS NOT NULL OR relation_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER REFERENCES entity(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    status TEXT,
    message TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_entity_type ON entity(type);
CREATE INDEX IF NOT EXISTS idx_entity_status ON entity(status);
CREATE INDEX IF NOT EXISTS idx_relation_source ON relation(source_entity_id);
CREATE INDEX IF NOT EXISTS idx_relation_target ON relation(target_entity_id);
CREATE INDEX IF NOT EXISTS idx_artifact_entity ON artifact(entity_id);
CREATE INDEX IF NOT EXISTS idx_metric_entity_name ON metric(entity_id, name);
CREATE INDEX IF NOT EXISTS idx_note_entity ON note(entity_id);
CREATE INDEX IF NOT EXISTS idx_event_entity ON event(entity_id);
