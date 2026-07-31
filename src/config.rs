//! Dactyl configuration.

/// Configuration passed to [`crate::init`].
///
/// `DactylConfig` carries the opaque credentials and endpoints each adapter
/// needs in order to talk to its backing datastore. The fields are inert
/// strings from dactyl's perspective; the adapter interprets them.
///
/// Auth for the Neon adapter is owned by Propodus; dactyl only forwards the
/// opaque token in [`DactylConfig::neon`].
#[derive(Debug, Clone)]
pub enum DactylConfig {
    /// Boot a SQLite adapter against the given file path. The parent directory
    /// must exist; the file is opened (and the store schema bootstrapped) on
    /// `init`.
    Sqlite {
        /// Path to the SQLite file. Created if absent.
        path: String,
    },
    /// Boot a Neon (Postgres over HTTP) adapter against a Propodus endpoint.
    Neon {
        /// Propodus endpoint URL. Dactyl treats this as opaque.
        endpoint: String,
        /// Opaque bearer token forwarded on each request. Owned by Propodus.
        bearer: Option<String>,
        /// Optional transport hint: `"http"` (default) or `"https"`.
        transport: Option<String>,
    },
}

impl DactylConfig {
    /// Convenience constructor for the SQLite adapter.
    pub fn sqlite(path: impl Into<String>) -> Self {
        DactylConfig::Sqlite { path: path.into() }
    }

    /// Convenience constructor for the Neon adapter.
    pub fn neon(
        endpoint: impl Into<String>,
        bearer: Option<String>,
        transport: Option<String>,
    ) -> Self {
        DactylConfig::Neon {
            endpoint: endpoint.into(),
            bearer,
            transport,
        }
    }

    /// Logical datastore name this config binds to.
    pub fn datastore(&self) -> &'static str {
        match self {
            DactylConfig::Sqlite { .. } => "sqlite",
            DactylConfig::Neon { .. } => "neon",
        }
    }
}
