//! Public error type for dactyl.

use thiserror::Error;

use crate::query::Construct;

/// All errors dactyl can raise on its public surface.
#[derive(Debug, Error)]
pub enum DactylError {
    /// The query contains a dialect-specific construct the active adapter
    /// does not support and `optimize = false` was passed.
    #[error("dialect mismatch: construct `{construct:?}` not supported")]
    Unsupported {
        /// The unsupported construct.
        construct: Construct,
    },

    /// The query selected a datastore that does not match the connection
    /// route, or an inline override could not be resolved to a configured
    /// route.
    #[error("datastore routing error: {0}")]
    Routing(String),

    /// The requested backend-neutral operation is not available for the
    /// selected adapter.
    #[error("unsupported datastore operation: {0}")]
    UnsupportedOperation(String),

    /// The query string failed to parse under the analyzer's lexical rules.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Adapter-level failure. Wraps the underlying error message.
    #[error("adapter error: {0}")]
    Adapter(String),

    /// Requested column or index was not found in the row.
    #[error("column not found: {0}")]
    ColumnNotFound(String),

    /// Type conversion of column value failed.
    #[error("conversion error: {0}")]
    Conversion(String),
}
