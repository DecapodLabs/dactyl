//! Minimal CREATE TABLE bootstrapping for the 9 Decapod stores.
//!
//! These statements mirror the schema in `.decapod/data/*.db` so the SQLite
//! adapter can be used standalone. The schema is intentionally tiny — dactyl
//! does not own the full Decapod store layout; it only needs the table names
//! to be discoverable from the universal query contract.
//!
//! Columns are kept minimal so the universal query in the conformance suite
//! (`select id, title, status from <store>`) succeeds. Production schemas are
//! managed by Decapod itself.

/// DDL statements run by `SqliteAdapter::open` for each known store.
#[allow(dead_code)]
pub const STORE_TABLES: &[(&str, &str)] = &[
    (
        "todos",
        "create table if not exists todos (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "knowledge",
        "create table if not exists knowledge (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "governance",
        "create table if not exists governance (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "memory",
        "create table if not exists memory (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "automation",
        "create table if not exists automation (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "broker_dedupe",
        "create table if not exists broker_dedupe (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "lcm",
        "create table if not exists lcm (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "federation",
        "create table if not exists federation (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
    (
        "events",
        "create table if not exists events (
            id integer primary key,
            title text not null,
            status text not null,
            assignee text
        )",
    ),
];

/// All known store names, in declaration order.
#[allow(dead_code)]
pub const STORE_NAMES: &[&str] = &[
    "todos",
    "knowledge",
    "governance",
    "memory",
    "automation",
    "broker_dedupe",
    "lcm",
    "federation",
    "events",
];

/// Run the bootstrap DDL against the given connection.
#[allow(dead_code)]
pub fn bootstrap(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    for (_name, ddl) in STORE_TABLES {
        conn.execute(ddl, [])?;
    }
    Ok(())
}

/// Convenience helper for tests/standalone usage: bootstrap a single table
/// by name.
#[allow(dead_code)]
pub fn bootstrap_table(conn: &rusqlite::Connection, name: &str) -> rusqlite::Result<()> {
    if let Some((_, ddl)) = STORE_TABLES.iter().find(|(n, _)| *n == name) {
        conn.execute(ddl, [])?;
    }
    Ok(())
}
