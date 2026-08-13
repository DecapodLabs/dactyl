//! Dactyl — a lightweight Rust storage driver for local and Neon routes.
//!
//! Dactyl owns backend selection, parameter binding, response normalization,
//! physical atomic batches, access mode, and local durability. It does not own
//! schema policy, migration ids/order, retries, analytics, or business logic.

mod adapter;
mod contract;
mod rows;
mod schema;

pub mod error;

pub use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    StorageContext, WriteResult, STORAGE_CONTEXT_VERSION,
};
pub use crate::error::{AdapterErrorKind, DactylError};
pub use crate::rows::{Parameter, Row, Rows};
pub use crate::schema::{
    ColumnSchema, ForeignKeyAction, ForeignKeySchema, IndexSchema, StoreSchema, TableSchema,
};

use crate::adapter::Adapter;

/// A supported application datastore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Datastore {
    Sqlite,
    Neon,
}

/// The route needed to send an application read or write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastoreRoute {
    datastore: Datastore,
    route: String,
    token: Option<String>,
}

const DATASTORE_ENV: &str = "DATASTORE";
const DATASTORE_ROUTE_ENV: &str = "DATASTORE_ROUTE";
const DATASTORE_TOKEN_ENV: &str = "DATASTORE_TOKEN";

impl DatastoreRoute {
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self {
            datastore: Datastore::Sqlite,
            route: path.into(),
            token: None,
        }
    }

    pub fn neon(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            datastore: Datastore::Neon,
            route: endpoint.into(),
            token,
        }
    }

    pub fn datastore(&self) -> Datastore {
        self.datastore
    }

    pub fn route(&self) -> &str {
        &self.route
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Resolve the ambient route configuration.
    ///
    /// `DATASTORE` is the sole selector. `DATASTORE_ROUTE` is required for
    /// the selected backend, and `DATASTORE_TOKEN` is read only for Neon.
    /// There is deliberately no implicit local fallback: a missing or empty
    /// selector/route fails before an adapter is constructed.
    pub fn from_env() -> Result<Self, DactylError> {
        let datastore = std::env::var(DATASTORE_ENV)
            .map_err(|_| DactylError::Config("DATASTORE is not set: use sqlite or neon".into()))?;
        let route = std::env::var(DATASTORE_ROUTE_ENV)
            .map_err(|_| DactylError::Config("DATASTORE_ROUTE is not set".into()))?;
        Self::from_env_values(
            Some(&datastore),
            Some(&route),
            std::env::var(DATASTORE_TOKEN_ENV).ok().as_deref(),
        )
    }

    fn from_env_values(
        datastore: Option<&str>,
        route: Option<&str>,
        token: Option<&str>,
    ) -> Result<Self, DactylError> {
        let datastore = datastore.ok_or_else(|| {
            DactylError::Config("DATASTORE is not set: use sqlite or neon".into())
        })?;
        let route =
            route.ok_or_else(|| DactylError::Config("DATASTORE_ROUTE is not set".into()))?;
        if route.trim().is_empty() {
            return Err(DactylError::Config(
                "DATASTORE_ROUTE must not be empty".into(),
            ));
        }

        match datastore {
            "sqlite" => Ok(Self::sqlite(route)),
            "neon" => Ok(Self::neon(
                route,
                token
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned),
            )),
            other => Err(DactylError::Config(format!(
                "invalid DATASTORE value {other:?}: use sqlite or neon"
            ))),
        }
    }
}

/// A route-scoped application driver. Backend handles remain private.
pub struct Connection {
    adapter: Box<dyn Adapter>,
    route: DatastoreRoute,
    context: Option<StorageContext>,
}

impl Connection {
    pub fn open(route: DatastoreRoute) -> Result<Self, DactylError> {
        Self::open_with_options_and_context(route, OpenOptions::default(), None)
    }

    pub fn open_with_options(
        route: DatastoreRoute,
        options: OpenOptions,
    ) -> Result<Self, DactylError> {
        Self::open_with_options_and_context(route, options, None)
    }

    /// Open a route with an optional caller-owned storage context.
    ///
    /// Local routes ignore the context. Neon routes require it for every
    /// operation and forward it without interpreting its payload.
    pub fn open_with_context(
        route: DatastoreRoute,
        context: Option<StorageContext>,
    ) -> Result<Self, DactylError> {
        Self::open_with_options_and_context(route, OpenOptions::default(), context)
    }

    pub fn open_with_options_and_context(
        route: DatastoreRoute,
        options: OpenOptions,
        context: Option<StorageContext>,
    ) -> Result<Self, DactylError> {
        if let Some(context) = &context {
            context.validate()?;
        }
        let adapter = build_adapter(&route, options, context.clone())?;
        Ok(Self {
            adapter,
            route,
            context,
        })
    }

    pub fn from_env() -> Result<Self, DactylError> {
        Self::open(DatastoreRoute::from_env()?)
    }

    pub fn datastore(&self) -> Datastore {
        self.route.datastore
    }

    pub fn route(&self) -> &DatastoreRoute {
        &self.route
    }

    pub fn context(&self) -> Option<&StorageContext> {
        self.context.as_ref()
    }

    /// Read application rows from the selected backend.
    pub fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        self.adapter.read(sql, params)
    }

    /// Write application data and return the explicit physical result.
    pub fn write_result(
        &self,
        sql: &str,
        params: &[Parameter],
    ) -> Result<WriteResult, DactylError> {
        self.adapter.write(sql, params)
    }

    /// Write application data and return the affected count for compatibility.
    pub fn write(&self, sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        Ok(self.write_result(sql, params)?.affected_rows)
    }

    pub fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        self.adapter.atomic(operations)
    }

    pub fn access_mode(&self) -> AccessMode {
        self.adapter.access_mode()
    }

    /// Inspect the local SQLite catalog through the backend-neutral schema type.
    pub fn inspect_schema(&self) -> Result<StoreSchema, DactylError> {
        self.adapter.inspect_schema()
    }
}

/// Alias that makes the application-driver role explicit.
pub type Driver = Connection;

pub fn read(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    Connection::from_env()?.read(sql, params)
}

pub fn read_with_context(
    context: Option<StorageContext>,
    sql: &str,
    params: &[Parameter],
) -> Result<Rows, DactylError> {
    Connection::open_with_context(DatastoreRoute::from_env()?, context)?.read(sql, params)
}

pub fn write(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    Connection::from_env()?.write(sql, params)
}

pub fn write_with_context(
    context: Option<StorageContext>,
    sql: &str,
    params: &[Parameter],
) -> Result<u64, DactylError> {
    Connection::open_with_context(DatastoreRoute::from_env()?, context)?.write(sql, params)
}

#[deprecated(note = "use dactyl_db::read")]
pub fn query(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    read(sql, params)
}

#[deprecated(note = "use dactyl_db::write")]
pub fn execute(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    write(sql, params)
}

fn build_adapter(
    route: &DatastoreRoute,
    _options: OpenOptions,
    _context: Option<StorageContext>,
) -> Result<Box<dyn Adapter>, DactylError> {
    match route.datastore {
        Datastore::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                Ok(Box::new(
                    crate::adapter::sqlite::SqliteAdapter::open_with_options(
                        &route.route,
                        _options,
                    )?,
                ))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(DactylError::Config(
                    "sqlite support is disabled; enable the `sqlite` feature".into(),
                ))
            }
        }
        Datastore::Neon => {
            #[cfg(feature = "neon")]
            {
                Ok(Box::new(
                    crate::adapter::neon::NeonAdapter::new_with_options(
                        &route.route,
                        route.token.clone(),
                        _options,
                        _context,
                    ),
                ))
            }
            #[cfg(not(feature = "neon"))]
            {
                Err(DactylError::Config(
                    "neon support is disabled; enable the `neon` feature".into(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Datastore, DatastoreRoute};

    #[test]
    fn ambient_route_requires_selector_and_non_empty_route() {
        assert!(DatastoreRoute::from_env_values(None, Some("/tmp/app.db"), None).is_err());
        assert!(DatastoreRoute::from_env_values(Some("sqlite"), None, None).is_err());
        assert!(DatastoreRoute::from_env_values(Some("sqlite"), Some("  "), None).is_err());
        assert!(DatastoreRoute::from_env_values(Some("unknown"), Some("route"), None).is_err());
    }

    #[test]
    fn ambient_selector_controls_backend_and_token_is_neon_only() {
        let sqlite =
            DatastoreRoute::from_env_values(Some("sqlite"), Some("/tmp/app.db"), Some("secret"))
                .unwrap();
        assert_eq!(sqlite.datastore(), Datastore::Sqlite);
        assert_eq!(sqlite.route(), "/tmp/app.db");
        assert_eq!(sqlite.token(), None);

        let neon = DatastoreRoute::from_env_values(
            Some("neon"),
            Some("https://propodus.example"),
            Some("secret"),
        )
        .unwrap();
        assert_eq!(neon.datastore(), Datastore::Neon);
        assert_eq!(neon.route(), "https://propodus.example");
        assert_eq!(neon.token(), Some("secret"));

        let blank_token = DatastoreRoute::from_env_values(
            Some("neon"),
            Some("https://propodus.example"),
            Some("  "),
        )
        .unwrap();
        assert_eq!(blank_token.token(), None);
    }
}
