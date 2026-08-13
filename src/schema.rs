//! Backend-neutral local schema inspection.
//!
//! This is the portable catalog surface. Callers do not need to depend on
//! SQLite catalog queries or Neon metadata response shapes.

use serde::{Deserialize, Serialize};

/// Projection of the caller-visible schema for a physical store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreSchema {
    /// Version of this backend-neutral schema description, not the file format.
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

/// One table in the physical store.
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

/// A recorded foreign key and its delete action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeySchema {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
}

/// Delete action reported by the physical store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
