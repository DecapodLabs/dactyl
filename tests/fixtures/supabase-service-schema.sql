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
