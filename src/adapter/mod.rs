//! Adapter trait — internal to dactyl.
//!
//! Both `SqliteAdapter` and `NeonAdapter` live behind their respective
//! feature gates and are reachable as `dactyl::adapter::sqlite::*` /
//! `dactyl::adapter::neon::*`. Nothing else at the crate root re-exports
//! the underlying types.

use crate::error::DactylError;
use crate::rows::Rows;

/// Internal trait every adapter implements.
///
/// `name()` is for diagnostics and the (currently unused) registry. The
/// public read/write facade constructs adapters lazily and never inspects
/// the name.
pub trait Adapter: Send + Sync {
    /// Execute a query against the backing datastore.
    fn execute(
        &self,
        query: &str,
        params: Option<&serde_json::Value>,
        optimize: bool,
        write: bool,
    ) -> Result<Rows, DactylError>;
}

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "neon")]
pub mod neon;
