//! Backend-neutral local schema inspection.
//!
//! This is the portable catalog surface. Callers must not query
//! `sqlite_master` or `PRAGMA table_info`; those names are SQLite-specific
//! and are not part of the Dactyl contract.

use serde::{Deserialize, Serialize};

/// Snapshot of the local store's caller-visible schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreSchema {
    pub format_version: u32,
    pub tables: Vec<TableSchema>,
    pub indexes: Vec<IndexSchema>,
}

impl StoreSchema {
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        let needle = name.to_ascii_lowercase();
        self.tables
            .iter()
            .find(|table| table.name == needle || table.name == name)
    }

    pub fn row_count(&self) -> u64 {
        self.tables.iter().map(|table| table.row_count).sum()
    }
}

/// One table in the local snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub unique_constraints: Vec<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeySchema>,
    pub row_count: u64,
}

/// One column, including nullability and recorded default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub primary_key: bool,
    pub unique: bool,
    pub not_null: bool,
    pub default: Option<serde_json::Value>,
}

/// A structural index. This is not a query planner entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// A recorded foreign key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeySchema {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
}

/// Delete action stored in the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
}
