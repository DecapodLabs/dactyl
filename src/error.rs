//! Public error type for dactyl.

use thiserror::Error;

use crate::query::Construct;

/// All errors dactyl can raise on its public surface.
#[derive(Debug, Error)]
pub enum DactylError {
    /// `init` was called twice with different datastores, or the requested
    /// datastore was not registered.
    #[error("unknown datastore: {0}")]
    UnknownDatastore(String),

    /// `init` was never called (or was called with an unsupported config) and
    /// the caller tried to read or write.
    #[error("dactyl is not initialized; call `dactyl::init` first")]
    Uninitialized,

    /// The query contains a construct the active datastore does not support
    /// and `optimize = false` was passed.
    #[error("dialect mismatch on `{datastore}`: construct `{construct:?}` not supported")]
    DialectMismatch {
        /// Datastore the caller routed to.
        datastore: String,
        /// The unsupported construct.
        construct: Construct,
    },

    /// The query string failed to parse under the analyzer's lexical rules.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Adapter-level failure. Wraps the underlying error message.
    #[error("adapter error: {0}")]
    Adapter(String),

    /// `init` was called with an unsupported configuration (no adapter built).
    #[error("config error: {0}")]
    Config(String),
}
