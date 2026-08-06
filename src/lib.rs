//! Dactyl — the application-layer read/write driver for SQLite and Neon.
//!
//! Dactyl owns only backend selection, parameter binding, and response
//! normalization. It forwards raw application SQL to the selected database;
//! schema administration, migrations, transactions, analytics, retries, and
//! business intelligence stay outside this crate.

mod adapter;
mod rows;

pub mod error;

pub use crate::error::{AdapterErrorKind, DactylError};
pub use crate::rows::{Parameter, Row, Rows};

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

    /// Resolve `DATASTORE`, `DATASTORE_ROUTE`, and `DATASTORE_TOKEN`.
    pub fn from_env() -> Result<Self, DactylError> {
        let datastore = std::env::var("DATASTORE")
            .map_err(|_| DactylError::Config("DATASTORE is not set: use sqlite or neon".into()))?;
        let route = std::env::var("DATASTORE_ROUTE")
            .map_err(|_| DactylError::Config("DATASTORE_ROUTE is not set".into()))?;
        match datastore.as_str() {
            "sqlite" => Ok(Self::sqlite(route)),
            "neon" => Ok(Self::neon(route, std::env::var("DATASTORE_TOKEN").ok())),
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
}

impl Connection {
    pub fn open(route: DatastoreRoute) -> Result<Self, DactylError> {
        let adapter = build_adapter(&route)?;
        Ok(Self { adapter, route })
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

    /// Read application rows from the selected backend.
    pub fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        self.adapter.read(sql, params)
    }

    /// Write application data and return the backend-reported affected count.
    pub fn write(&self, sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        self.adapter.write(sql, params)
    }
}

/// Alias that makes the application-driver role explicit.
pub type Driver = Connection;

pub fn read(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    Connection::from_env()?.read(sql, params)
}

pub fn write(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    Connection::from_env()?.write(sql, params)
}

#[deprecated(note = "use dactyl_db::read")]
pub fn query(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    read(sql, params)
}

#[deprecated(note = "use dactyl_db::write")]
pub fn execute(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    write(sql, params)
}

fn build_adapter(route: &DatastoreRoute) -> Result<Box<dyn Adapter>, DactylError> {
    match route.datastore {
        Datastore::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                Ok(Box::new(crate::adapter::sqlite::SqliteAdapter::open(
                    &route.route,
                )?))
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
                Ok(Box::new(crate::adapter::neon::NeonAdapter::new(
                    &route.route,
                    route.token.clone(),
                )))
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
