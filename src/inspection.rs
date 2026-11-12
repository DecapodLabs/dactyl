//! Additive, caller-owned diagnostics in one bounded read snapshot.
//!
//! No semantic schema, migration or retention policy is defined here. Remote
//! adapters must explicitly implement the capability; there is no local fallback.

use std::time::Duration;

use crate::{DactylError, Parameter, Row, Rows};

/// Per-snapshot limits, shared by all queries and visits. These bound returned
/// data and SQLite VM work, not the process's entire memory or callback work.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotLimits {
    pub duration: Duration,
    pub vm_steps: u64,
    pub queries: u64,
    pub rows: u64,
    /// Conservative decoded result charge, including column names and blobs.
    pub result_bytes: u64,
    pub cell_bytes: u64,
    pub columns: u32,
    pub sql_bytes: usize,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(5),
            vm_steps: 100_000_000,
            queries: 4096,
            rows: 1_000_000,
            result_bytes: 64 * 1024 * 1024,
            cell_bytes: 8 * 1024 * 1024,
            columns: 128,
            sql_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanOutcome {
    pub rows: u64,
    /// False when the visitor deliberately stops. A budget violation is an
    /// error, never a successful truncated query.
    pub complete: bool,
}

/// Valid only for the duration of `Connection::inspect_snapshot`'s callback.
/// SQLite fixes the snapshot at its first read. Visitors must not reenter the
/// connection, block, or perform unbounded work. Statements are released when
/// a visitor stops or returns an error.
pub trait SnapshotReader {
    fn visit(
        &mut self,
        sql: &str,
        params: &[Parameter],
        visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
    ) -> Result<ScanOutcome, DactylError>;

    /// The owned compatibility representation, charged to the same budget.
    fn read(&mut self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let mut rows = Vec::new();
        self.visit(sql, params, &mut |row| {
            rows.push(row.clone());
            Ok(true)
        })?;
        Ok(Rows(rows))
    }
}
