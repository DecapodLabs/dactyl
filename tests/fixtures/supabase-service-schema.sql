-- Applied only by a trusted external harness to a fresh, disposable PostgreSQL
-- database. The Dactyl client never provisions this schema or receives a DB key.
-- No IF NOT EXISTS: reusing a database is an error, not an implicit cleanup.
-- Service-owned authorization must give two independent test principals access
-- to this fixture and deny the separately supplied authentication/scope cases.
CREATE TABLE dactyl_service_conformance (
    id BIGINT PRIMARY KEY,
    flag BOOLEAN NOT NULL,
    signed_value BIGINT NOT NULL,
    real_value DOUBLE PRECISION NOT NULL,
    text_value TEXT NOT NULL,
    blob_value BYTEA NOT NULL,
    nullable_text TEXT,
    version BIGINT NOT NULL
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    hash TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    description TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    owner TEXT DEFAULT '',
    due TEXT,
    ref TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    closed_at TEXT,
    dir_path TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT 'repo',
    parent_task_id TEXT,
    priority TEXT DEFAULT 'medium',
    depends_on TEXT DEFAULT '',
    blocks TEXT DEFAULT '',
    category TEXT DEFAULT '',
    component TEXT DEFAULT '',
    assigned_to TEXT DEFAULT '',
    assigned_at TEXT,
    one_shot BIGINT DEFAULT 0,
    lease_expires_at TEXT,
    lease_generation BIGINT DEFAULT 0,
    lease_lifecycle TEXT DEFAULT '',
    intent_anchor TEXT DEFAULT '',
    revision BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    seq BIGINT NOT NULL,
    stream TEXT NOT NULL,
    subject_kind TEXT,
    subject_id TEXT,
    event_type TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'decapod',
    UNIQUE (stream, seq)
);
