//! Dactyl — the backend-neutral datastore boundary for Decapod.
//!
//! The public API contains no `rusqlite` or HTTP adapter types. Callers can
//! use one-shot functions for small operations or retain a [`Connection`] for
//! migrations, validation probes, and several operations against one route.
//! Both paths use the same parameter, row, transaction, and SQL-analysis
//! contracts.

mod adapter;
mod rows;

pub mod error;
pub mod query;

#[doc(hidden)]
pub mod __private;

pub use dactyl_db_macros::query;

pub use crate::error::DactylError;
pub use crate::query::{Construct, Dialect, QueryAnalyzer};
pub use crate::rows::{Parameter, Row, Rows};

use crate::adapter::Adapter;
use std::time::Duration;

/// A backend selected by a [`DatastoreRoute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Datastore {
    Sqlite,
    Neon,
}

/// SQLite journal policy. `Wal` is the default for concurrent local readers;
/// the adapter falls back to `Delete` if the filesystem cannot support WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteJournalMode {
    Wal,
    Delete,
    Memory,
    Off,
}

impl SqliteJournalMode {
    #[cfg(feature = "sqlite")]
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Delete => "DELETE",
            Self::Memory => "MEMORY",
            Self::Off => "OFF",
        }
    }
}

impl Datastore {
    fn dialect(self) -> Dialect {
        match self {
            Datastore::Sqlite => Dialect::Sqlite,
            Datastore::Neon => Dialect::Postgres,
        }
    }
}

/// Explicit route information for a backend connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastoreRoute {
    datastore: Datastore,
    route: String,
    token: Option<String>,
}

impl DatastoreRoute {
    /// Route a connection to a local SQLite file.
    pub fn sqlite(path: impl Into<String>) -> Self {
        Self {
            datastore: Datastore::Sqlite,
            route: path.into(),
            token: None,
        }
    }

    /// Route a connection to a Neon/Propodus SQL-over-HTTP endpoint.
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

    /// Resolve the active route from `DATASTORE`, `DATASTORE_ROUTE`, and the
    /// optional `DATASTORE_TOKEN` environment variables.
    pub fn from_env() -> Result<Self, DactylError> {
        let datastore = std::env::var("DATASTORE").map_err(|_| {
            DactylError::Adapter("DATASTORE is not set: set DATASTORE and DATASTORE_ROUTE".into())
        })?;
        let kind = match datastore.as_str() {
            "sqlite" => Datastore::Sqlite,
            "neon" => Datastore::Neon,
            other => Err(DactylError::Adapter(format!(
                "invalid DATASTORE value {other:?}: must be 'sqlite' or 'neon'"
            )))?,
        };
        let route = std::env::var("DATASTORE_ROUTE").map_err(|_| {
            DactylError::Adapter(
                "DATASTORE_ROUTE is not set: set DATASTORE and DATASTORE_ROUTE".into(),
            )
        })?;
        Ok(match kind {
            Datastore::Sqlite => Self::sqlite(route),
            Datastore::Neon => Self::neon(route, std::env::var("DATASTORE_TOKEN").ok()),
        })
    }
}

/// Per-connection behavior that is safe to expose across all adapters.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// Permit only dactyl's explicitly safe dialect rewrites.
    pub allow_rewrites: bool,
    /// Open SQLite in read-only mode. Ignored by Neon.
    pub read_only: bool,
    /// Busy timeout for SQLite lock contention. Ignored by Neon.
    pub busy_timeout: Duration,
    /// Enable SQLite foreign-key enforcement. Ignored by Neon.
    pub foreign_keys: bool,
    /// SQLite journal mode. Read-only connections leave the existing mode
    /// unchanged. Ignored by Neon.
    pub journal_mode: Option<SqliteJournalMode>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            allow_rewrites: false,
            read_only: false,
            busy_timeout: Duration::from_secs(5),
            foreign_keys: true,
            journal_mode: Some(SqliteJournalMode::Wal),
        }
    }
}

impl ConnectionOptions {
    pub fn with_rewrites(mut self, allow: bool) -> Self {
        self.allow_rewrites = allow;
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    fn from_env() -> Self {
        let allow_rewrites = std::env::var("DATASTORE_REWRITE")
            .ok()
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        Self {
            allow_rewrites,
            ..Self::default()
        }
    }
}

/// A connection-scoped backend-neutral datastore handle.
pub struct Connection {
    adapter: Box<dyn Adapter>,
    route: DatastoreRoute,
    options: ConnectionOptions,
}

impl Connection {
    /// Open a route with the default policy.
    pub fn open(route: DatastoreRoute) -> Result<Self, DactylError> {
        Self::open_with_options(route, ConnectionOptions::default())
    }

    /// Open a route with explicit connection policy.
    #[cfg(any(feature = "sqlite", feature = "neon"))]
    pub fn open_with_options(
        route: DatastoreRoute,
        options: ConnectionOptions,
    ) -> Result<Self, DactylError> {
        let adapter: Box<dyn Adapter> = match route.datastore {
            Datastore::Sqlite => {
                #[cfg(feature = "sqlite")]
                {
                    Box::new(
                        crate::adapter::sqlite::SqliteAdapter::open_with_options(
                            &route.route,
                            options.read_only,
                            options.busy_timeout,
                            options.foreign_keys,
                            options.journal_mode,
                        )
                        .map_err(|e| DactylError::Adapter(format!("sqlite open: {e}")))?,
                    )
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(DactylError::Adapter(
                        "sqlite adapter requested but `sqlite` feature is disabled".into(),
                    ));
                }
            }
            Datastore::Neon => {
                #[cfg(feature = "neon")]
                {
                    Box::new(crate::adapter::neon::NeonAdapter::new(
                        &route.route,
                        route.token.clone(),
                    ))
                }
                #[cfg(not(feature = "neon"))]
                {
                    return Err(DactylError::Adapter(
                        "neon adapter requested but `neon` feature is disabled".into(),
                    ));
                }
            }
        };
        Ok(Self {
            adapter,
            route,
            options,
        })
    }

    /// Keep the crate's no-feature build useful for analysis-only consumers.
    #[cfg(not(any(feature = "sqlite", feature = "neon")))]
    pub fn open_with_options(
        _route: DatastoreRoute,
        _options: ConnectionOptions,
    ) -> Result<Self, DactylError> {
        Err(DactylError::Adapter(
            "no datastore adapter feature is enabled; enable `sqlite` or `neon`".into(),
        ))
    }

    /// Open the active environment-selected route.
    pub fn from_env() -> Result<Self, DactylError> {
        Self::open_with_options(DatastoreRoute::from_env()?, ConnectionOptions::from_env())
    }

    pub fn datastore(&self) -> Datastore {
        self.route.datastore
    }

    pub fn dialect(&self) -> Dialect {
        self.route.datastore.dialect()
    }

    pub fn route(&self) -> &DatastoreRoute {
        &self.route
    }

    pub fn options(&self) -> &ConnectionOptions {
        &self.options
    }

    /// Execute a query or write and return its projected rows.
    pub fn query(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let prepared = self.prepare(sql)?;
        self.adapter.execute(&prepared, params)
    }

    /// Execute one DDL/migration/affected-row statement.
    pub fn execute(&self, sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
        let prepared = self.prepare(sql)?;
        self.adapter.execute_raw(&prepared, params)
    }

    /// Execute a caller-owned SQL script containing multiple statements.
    pub fn execute_batch(&self, sql: &str) -> Result<(), DactylError> {
        let prepared = self.prepare(sql)?;
        self.adapter.execute_script(&prepared)
    }

    /// Execute an all-or-nothing batch of parameterized statements.
    pub fn transaction(&self, statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = statements
            .iter()
            .map(|statement| {
                Ok(Statement {
                    sql: self.prepare(&statement.sql)?,
                    params: statement.params.clone(),
                })
            })
            .collect::<Result<Vec<_>, DactylError>>()?;
        self.adapter.execute_batch(&prepared)
    }

    /// Return the adapter's last generated insert id when available.
    pub fn last_insert_id(&self) -> Result<i64, DactylError> {
        self.adapter.last_insert_id()
    }

    /// Execute the operation-based thin-waist contract used by callers that
    /// must support both local and remote storage without closure types tied to
    /// SQLite.
    pub fn execute_op(&self, op: StorageOp) -> Result<StorageResult, DactylError> {
        match op {
            StorageOp::Query { sql, params } => self.query(&sql, &params).map(StorageResult::Rows),
            StorageOp::Execute { sql, params } => {
                self.execute(&sql, &params).map(StorageResult::Affected)
            }
            StorageOp::Script { sql } => self.execute_batch(&sql).map(|()| StorageResult::Unit),
            StorageOp::Transaction { statements } => {
                self.transaction(&statements).map(StorageResult::Batch)
            }
            StorageOp::LastInsertId => self.last_insert_id().map(StorageResult::LastInsertId),
        }
    }

    fn prepare(&self, sql: &str) -> Result<String, DactylError> {
        QueryAnalyzer::new().prepare(sql, self.dialect(), self.options.allow_rewrites)
    }
}

/// Operation-based storage contract for integration boundaries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum StorageOp {
    Query { sql: String, params: Vec<Parameter> },
    Execute { sql: String, params: Vec<Parameter> },
    Script { sql: String },
    Transaction { statements: Vec<Statement> },
    LastInsertId,
}

/// Results for [`StorageOp`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum StorageResult {
    Rows(Rows),
    Affected(u64),
    Unit,
    Batch(Vec<Rows>),
    LastInsertId(i64),
}

/// A parameterized SQL statement for batch execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Parameter>,
}

impl Statement {
    pub fn new(sql: &str, params: Vec<Parameter>) -> Self {
        Self {
            sql: sql.to_string(),
            params,
        }
    }
}

/// Test-only compatibility helper. Connections are explicit and there is no
/// process-wide cache to clear.
#[doc(hidden)]
pub fn reset() {}

fn connection_for_query(sql: &str) -> Result<Connection, DactylError> {
    let route = route_for_query(sql)?;
    Connection::open_with_options(route, ConnectionOptions::from_env())
}

fn route_for_query(sql: &str) -> Result<DatastoreRoute, DactylError> {
    let active = DatastoreRoute::from_env()?;
    let analyzed = QueryAnalyzer::new().analyze(sql);
    let Some(inline) = analyzed.inline_override else {
        return Ok(active);
    };
    let Some(inline_datastore) = (match inline {
        "sqlite" => Some(Datastore::Sqlite),
        "neon" => Some(Datastore::Neon),
        _ => None,
    }) else {
        return Err(DactylError::Routing(format!(
            "unknown inline datastore {inline:?}"
        )));
    };
    if inline_datastore == active.datastore {
        return Ok(active);
    }

    let (route_var, token_var) = match inline_datastore {
        Datastore::Sqlite => ("DATASTORE_SQLITE_ROUTE", None),
        Datastore::Neon => ("DATASTORE_NEON_ROUTE", Some("DATASTORE_NEON_TOKEN")),
    };
    let route = std::env::var(route_var).map_err(|_| {
        DactylError::Routing(format!(
            "inline datastore {inline:?} requires {route_var} when it differs from DATASTORE"
        ))
    })?;
    let token = token_var
        .and_then(|name| std::env::var(name).ok())
        .or_else(|| {
            (inline_datastore == Datastore::Neon)
                .then(|| std::env::var("DATASTORE_TOKEN").ok())
                .flatten()
        });
    Ok(match inline_datastore {
        Datastore::Sqlite => DatastoreRoute::sqlite(route),
        Datastore::Neon => DatastoreRoute::neon(route, token),
    })
}

/// Execute any query against the active environment-selected datastore.
pub fn query(sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
    connection_for_query(sql)?.query(sql, params)
}

/// Execute one DDL/migration/affected-row statement.
pub fn execute(sql: &str, params: &[Parameter]) -> Result<u64, DactylError> {
    connection_for_query(sql)?.execute(sql, params)
}

/// Execute a caller-owned SQL script.
pub fn execute_batch(sql: &str) -> Result<(), DactylError> {
    connection_for_query(sql)?.execute_batch(sql)
}

/// Execute an atomic batch of parameterized statements.
pub fn transaction(statements: &[Statement]) -> Result<Vec<Rows>, DactylError> {
    if statements.is_empty() {
        return Ok(Vec::new());
    }
    connection_for_query(&statements[0].sql)?.transaction(statements)
}
