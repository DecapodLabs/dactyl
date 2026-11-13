//! Explicit, versioned resource budgets for storage, wire, and result growth.
//!
//! These budgets bound per-operation and per-batch resource consumption inside
//! Dactyl without changing [`crate::OpenOptions`] struct-literal compatibility
//! or introducing a hidden scheduler, retry loop, or tenant policy engine.

use std::time::Duration;

use crate::contract::{Operation, StorageContext};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::Parameter;

/// Current version of the public [`ResourceBudget`] contract.
pub const RESOURCE_BUDGET_VERSION: u16 = 1;

const DEFAULT_MAX_SQL_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_CONTEXT_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_PARAM_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_MAX_CELL_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_MAX_COLUMNS: u32 = 256;
const DEFAULT_MAX_ROWS: u64 = 100_000;
const DEFAULT_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_WIRE_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_OPERATIONS: usize = 1_024;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[cfg(any(feature = "neon", feature = "supabase", test))]
const MAX_JSON_DEPTH: usize = 64;

/// Where a storage or resource failure originated.
///
/// Callers can distinguish local preflight/result limits from remote service
/// policy denials and from malformed or oversized remote responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureOrigin {
    /// Enforced locally by Dactyl's configured [`ResourceBudget`] or snapshot budget.
    LocalBudget,
    /// Rejected by the remote service's authorization, rate, or quota policy.
    RemotePolicy,
    /// Remote response violated wire-size or JSON envelope invariants.
    MalformedResponse,
    /// Engine, lock, query, configuration, or transport failure outside budget/policy classes.
    Other,
}

/// Physical mutation state guaranteed when an operation returns an error.
///
/// A retryable error category is never permission to replay an [`Self::Uncertain`]
/// write without caller/service idempotency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOutcome {
    /// Preflight or capability check rejected the request before any physical mutation.
    NotAttempted,
    /// Local transaction rolled back or the service returned an explicit unpersisted failure.
    RolledBack,
    /// Failure occurred after sending a mutation or during commit/response delivery;
    /// durability cannot be disproved by the client alone.
    Uncertain,
}

/// Additive per-connection resource budget (contract version 1).
///
/// Guarantees are per operation (or per `atomic` batch) on a single [`crate::Connection`];
/// bounding aggregate process memory across concurrent connections requires caller
/// concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceBudget {
    version: u16,
    max_sql_bytes: usize,
    max_context_bytes: usize,
    max_param_bytes: u64,
    max_cell_bytes: u64,
    max_columns: u32,
    max_rows: u64,
    max_result_bytes: u64,
    max_wire_bytes: u64,
    max_batch_operations: usize,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self::v1()
    }
}

impl ResourceBudget {
    /// Construct the default version-1 resource budget.
    pub const fn v1() -> Self {
        Self {
            version: RESOURCE_BUDGET_VERSION,
            max_sql_bytes: DEFAULT_MAX_SQL_BYTES,
            max_context_bytes: DEFAULT_MAX_CONTEXT_BYTES,
            max_param_bytes: DEFAULT_MAX_PARAM_BYTES,
            max_cell_bytes: DEFAULT_MAX_CELL_BYTES,
            max_columns: DEFAULT_MAX_COLUMNS,
            max_rows: DEFAULT_MAX_ROWS,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            max_wire_bytes: DEFAULT_MAX_WIRE_BYTES,
            max_batch_operations: DEFAULT_MAX_BATCH_OPERATIONS,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn max_sql_bytes(&self) -> usize {
        self.max_sql_bytes
    }

    pub const fn max_context_bytes(&self) -> usize {
        self.max_context_bytes
    }

    pub const fn max_param_bytes(&self) -> u64 {
        self.max_param_bytes
    }

    pub const fn max_cell_bytes(&self) -> u64 {
        self.max_cell_bytes
    }

    pub const fn max_columns(&self) -> u32 {
        self.max_columns
    }

    pub const fn max_rows(&self) -> u64 {
        self.max_rows
    }

    pub const fn max_result_bytes(&self) -> u64 {
        self.max_result_bytes
    }

    pub const fn max_wire_bytes(&self) -> u64 {
        self.max_wire_bytes
    }

    pub const fn max_batch_operations(&self) -> usize {
        self.max_batch_operations
    }

    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub const fn with_max_sql_bytes(mut self, bytes: usize) -> Self {
        self.max_sql_bytes = bytes;
        self
    }

    pub const fn with_max_context_bytes(mut self, bytes: usize) -> Self {
        self.max_context_bytes = bytes;
        self
    }

    pub const fn with_max_param_bytes(mut self, bytes: u64) -> Self {
        self.max_param_bytes = bytes;
        self
    }

    pub const fn with_max_cell_bytes(mut self, bytes: u64) -> Self {
        self.max_cell_bytes = bytes;
        self
    }

    pub const fn with_max_columns(mut self, columns: u32) -> Self {
        self.max_columns = columns;
        self
    }

    pub const fn with_max_rows(mut self, rows: u64) -> Self {
        self.max_rows = rows;
        self
    }

    pub const fn with_max_result_bytes(mut self, bytes: u64) -> Self {
        self.max_result_bytes = bytes;
        self
    }

    pub const fn with_max_wire_bytes(mut self, bytes: u64) -> Self {
        self.max_wire_bytes = bytes;
        self
    }

    pub const fn with_max_batch_operations(mut self, operations: usize) -> Self {
        self.max_batch_operations = operations;
        self
    }

    pub const fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Validate version and timeout invariants before opening a connection.
    pub fn validate(&self) -> Result<(), DactylError> {
        if self.version != RESOURCE_BUDGET_VERSION {
            return Err(DactylError::Config(format!(
                "unsupported resource budget version {}; supported version is {}",
                self.version, RESOURCE_BUDGET_VERSION
            )));
        }
        if self.connect_timeout.is_zero() || self.request_timeout.is_zero() {
            return Err(DactylError::Config(
                "resource budget timeouts must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) const LOCAL_PREFLIGHT_PREFIX: &str = "local preflight budget exceeded:";
pub(crate) const LOCAL_RESULT_PREFIX: &str = "local result budget exceeded:";
pub(crate) const LOCAL_ROLLBACK_PREFIX: &str = "local transaction rolled back:";
pub(crate) const REMOTE_UNCERTAIN_PREFIX: &str =
    "remote post-send failure (commit outcome uncertain):";

pub(crate) fn unwrap_adapter_prefixes(message: &str) -> &str {
    message
        .strip_prefix(LOCAL_ROLLBACK_PREFIX)
        .map(str::trim_start)
        .or_else(|| {
            message
                .strip_prefix(REMOTE_UNCERTAIN_PREFIX)
                .map(str::trim_start)
        })
        .unwrap_or(message)
}

pub(crate) fn preflight_limit(code: &'static str, detail: &'static str) -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Quota,
        code,
        format!("{LOCAL_PREFLIGHT_PREFIX} {detail}"),
    )
}

#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
pub(crate) fn result_limit(code: &'static str, detail: &'static str) -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Quota,
        code,
        format!("{LOCAL_RESULT_PREFIX} {detail}"),
    )
}

#[cfg_attr(not(any(feature = "sqlite", test)), allow(dead_code))]
pub(crate) fn mark_local_rollback(error: DactylError) -> DactylError {
    match error {
        DactylError::Adapter {
            kind,
            code,
            message,
        } if !message.starts_with(LOCAL_ROLLBACK_PREFIX) => DactylError::Adapter {
            kind,
            code,
            message: format!("{LOCAL_ROLLBACK_PREFIX} {message}"),
        },
        other => other,
    }
}

#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
pub(crate) fn mark_remote_uncertain(error: DactylError) -> DactylError {
    match error {
        DactylError::Adapter {
            kind,
            code,
            message,
        } if !message.starts_with(REMOTE_UNCERTAIN_PREFIX) => DactylError::Adapter {
            kind,
            code,
            message: format!("{REMOTE_UNCERTAIN_PREFIX} {message}"),
        },
        other => other,
    }
}

/// Validate serialized [`StorageContext`] size without allocating an unbounded buffer.
pub(crate) fn preflight_context(
    budget: &ResourceBudget,
    context: Option<&StorageContext>,
) -> Result<(), DactylError> {
    let Some(context) = context else {
        return Ok(());
    };
    let mut writer = BoundedVecWriter::new(budget.max_context_bytes as u64);
    serde_json::to_writer(&mut writer, context).map_err(|error| {
        if is_wire_limit_error(&error) {
            preflight_limit(
                "context_budget_exceeded",
                "storage context exceeds max_context_bytes",
            )
        } else {
            DactylError::adapter_with_code(
                AdapterErrorKind::Protocol,
                "invalid_context",
                "storage context could not be serialized",
            )
        }
    })?;
    Ok(())
}

/// Validate SQL and parameter sizes before preparing a local statement or serializing a request.
#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
pub(crate) fn preflight_statement(
    budget: &ResourceBudget,
    sql: &str,
    params: &[Parameter],
) -> Result<u64, DactylError> {
    if sql.len() > budget.max_sql_bytes {
        return Err(preflight_limit(
            "sql_budget_exceeded",
            "SQL statement exceeds max_sql_bytes",
        ));
    }
    let mut param_bytes = 0_u64;
    for param in params {
        let cell_bytes = match param {
            Parameter::Null => 0_u64,
            Parameter::Bool(_) => 1_u64,
            Parameter::Integer(_) | Parameter::Real(_) => 8_u64,
            Parameter::Text(value) => value.len() as u64,
            Parameter::Blob(value) => value.len() as u64,
        };
        if cell_bytes > budget.max_cell_bytes {
            return Err(preflight_limit(
                "cell_budget_exceeded",
                "parameter value exceeds max_cell_bytes",
            ));
        }
        param_bytes = param_bytes.checked_add(cell_bytes).ok_or_else(|| {
            preflight_limit(
                "parameter_budget_exceeded",
                "parameter byte count overflowed u64",
            )
        })?;
        if param_bytes > budget.max_param_bytes {
            return Err(preflight_limit(
                "parameter_budget_exceeded",
                "aggregate parameter bytes exceed max_param_bytes",
            ));
        }
    }
    Ok(param_bytes)
}

/// Validate an atomic batch's operation count, SQL budgets, and aggregate parameter bytes
/// before starting a local transaction or issuing a remote request.
#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
pub(crate) fn preflight_batch(
    budget: &ResourceBudget,
    operations: &[Operation],
) -> Result<(), DactylError> {
    if operations.len() > budget.max_batch_operations {
        return Err(preflight_limit(
            "batch_budget_exceeded",
            "atomic batch operation count exceeds max_batch_operations",
        ));
    }
    let mut total_param_bytes = 0_u64;
    for operation in operations {
        let op_bytes = preflight_statement(budget, operation.sql(), operation.params())?;
        total_param_bytes = total_param_bytes.checked_add(op_bytes).ok_or_else(|| {
            preflight_limit(
                "parameter_budget_exceeded",
                "batch parameter byte count overflowed u64",
            )
        })?;
        if total_param_bytes > budget.max_param_bytes {
            return Err(preflight_limit(
                "parameter_budget_exceeded",
                "atomic batch parameter bytes exceed max_param_bytes",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct WireLimitExceeded;

impl std::fmt::Display for WireLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("wire byte limit exceeded")
    }
}

impl std::error::Error for WireLimitExceeded {}

struct BoundedVecWriter {
    buf: Vec<u8>,
    max_bytes: u64,
}

impl BoundedVecWriter {
    fn new(max_bytes: u64) -> Self {
        let initial = usize::try_from(max_bytes.min(4096)).unwrap_or(4096);
        Self {
            buf: Vec::with_capacity(initial),
            max_bytes,
        }
    }
}

impl std::io::Write for BoundedVecWriter {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        let next = (self.buf.len() as u64)
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, WireLimitExceeded)
            })?;
        if next > self.max_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                WireLimitExceeded,
            ));
        }
        self.buf.extend_from_slice(chunk);
        Ok(chunk.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn is_wire_limit_error(error: &serde_json::Error) -> bool {
    error.is_io() && error.to_string().contains("wire byte limit exceeded")
}

/// Serialize a request body into JSON while aborting immediately if `max_wire_bytes` is exceeded.
#[cfg(any(feature = "neon", feature = "supabase", test))]
pub(crate) fn serialize_bounded_json(
    value: &impl serde::Serialize,
    max_wire_bytes: u64,
) -> Result<Vec<u8>, DactylError> {
    let mut writer = BoundedVecWriter::new(max_wire_bytes);
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        if is_wire_limit_error(&error) {
            preflight_limit(
                "payload_too_large",
                "serialized request exceeds max_wire_bytes",
            )
        } else {
            DactylError::adapter(AdapterErrorKind::Value, "invalid request value")
        }
    })?;
    Ok(writer.buf)
}

#[cfg(any(feature = "neon", feature = "supabase", test))]
#[derive(Debug)]
pub(crate) enum BoundedReadError {
    TooLarge,
    Io(std::io::Error),
}

/// Read an HTTP response stream in fixed chunks without trusting `Content-Length`,
/// failing at `max_wire_bytes + 1` before allocating an unbounded response buffer.
#[cfg(any(feature = "neon", feature = "supabase", test))]
pub(crate) fn read_bounded_body(
    mut reader: impl std::io::Read,
    max_wire_bytes: u64,
) -> Result<Vec<u8>, BoundedReadError> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(BoundedReadError::Io(error)),
        };
        let next_len = (bytes.len() as u64)
            .checked_add(read as u64)
            .ok_or(BoundedReadError::TooLarge)?;
        if next_len > max_wire_bytes {
            return Err(BoundedReadError::TooLarge);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes)
}

#[cfg(any(feature = "neon", feature = "supabase", test))]
pub(crate) fn ensure_json_depth(value: &serde_json::Value) -> Result<(), DactylError> {
    fn check(value: &serde_json::Value, depth: usize) -> bool {
        if depth > MAX_JSON_DEPTH {
            return false;
        }
        match value {
            serde_json::Value::Array(items) => items.iter().all(|item| check(item, depth + 1)),
            serde_json::Value::Object(map) => map.values().all(|item| check(item, depth + 1)),
            _ => true,
        }
    }
    if check(value, 0) {
        Ok(())
    } else {
        Err(DactylError::adapter_with_code(
            AdapterErrorKind::Protocol,
            "malformed_response",
            "remote JSON nesting exceeds maximum supported depth",
        ))
    }
}

/// Conservative decoded-result accounting shared across a query or atomic batch.
#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
pub(crate) struct DecodedResultTracker {
    budget: ResourceBudget,
    rows: u64,
    bytes: u64,
}

#[cfg_attr(
    not(any(feature = "sqlite", feature = "neon", feature = "supabase", test)),
    allow(dead_code)
)]
impl DecodedResultTracker {
    pub(crate) fn new(budget: ResourceBudget) -> Self {
        Self {
            budget,
            rows: 0,
            bytes: 0,
        }
    }

    pub(crate) fn charge_columns(&mut self, columns: &[String]) -> Result<(), DactylError> {
        if u32::try_from(columns.len()).map_or(true, |count| count > self.budget.max_columns) {
            return Err(result_limit(
                "column_budget_exceeded",
                "result column count exceeds max_columns",
            ));
        }
        for name in columns {
            let cost = (name.len() as u64).checked_add(32).ok_or_else(|| {
                result_limit(
                    "result_budget_exceeded",
                    "column metadata charge overflowed",
                )
            })?;
            self.charge_bytes(cost)?;
        }
        Ok(())
    }

    pub(crate) fn charge_row_overhead(&mut self, columns: &[String]) -> Result<(), DactylError> {
        self.rows = self.rows.checked_add(1).ok_or_else(|| {
            result_limit("row_budget_exceeded", "decoded row count overflowed u64")
        })?;
        if self.rows > self.budget.max_rows {
            return Err(result_limit(
                "row_budget_exceeded",
                "decoded row count exceeds max_rows",
            ));
        }
        let mut overhead = 64_u64;
        for col in columns {
            overhead = overhead
                .checked_add((col.len() as u64).saturating_add(24))
                .ok_or_else(|| {
                    result_limit("result_budget_exceeded", "row column charge overflowed")
                })?;
        }
        self.charge_bytes(overhead)
    }

    pub(crate) fn charge_raw_cell(
        &mut self,
        raw_bytes: u64,
        is_blob: bool,
    ) -> Result<(), DactylError> {
        if raw_bytes > self.budget.max_cell_bytes {
            return Err(result_limit(
                "cell_budget_exceeded",
                "decoded cell exceeds max_cell_bytes",
            ));
        }
        let multiplier = if is_blob { 32_u64 } else { 1_u64 };
        let decoded = raw_bytes
            .checked_mul(multiplier)
            .and_then(|v| v.checked_add(32))
            .ok_or_else(|| {
                result_limit("result_budget_exceeded", "decoded cell charge overflowed")
            })?;
        self.charge_bytes(decoded)
    }

    #[cfg(any(feature = "neon", feature = "supabase", test))]
    pub(crate) fn charge_json_value(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<(), DactylError> {
        ensure_json_depth(value)?;
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                self.charge_raw_cell(8, false)
            }
            serde_json::Value::String(text) => self.charge_raw_cell(text.len() as u64, false),
            serde_json::Value::Array(items) => {
                // Byte arrays represent BLOBs on the wire; nested JSON arrays also
                // expand per element. Check raw count against max_cell_bytes first.
                let is_byte_array = items
                    .iter()
                    .all(|item| item.as_u64().is_some_and(|b| b <= 255));
                if is_byte_array {
                    self.charge_raw_cell(items.len() as u64, true)
                } else {
                    let serialized = serde_json::to_vec(value)
                        .map_err(|_| {
                            DactylError::adapter_with_code(
                                AdapterErrorKind::Protocol,
                                "malformed_response",
                                "invalid JSON array cell",
                            )
                        })?
                        .len() as u64;
                    self.charge_raw_cell(serialized, false)?;
                    for item in items {
                        self.charge_bytes(32)?;
                        let _ = item;
                    }
                    Ok(())
                }
            }
            serde_json::Value::Object(_) => {
                let serialized = serde_json::to_vec(value)
                    .map_err(|_| {
                        DactylError::adapter_with_code(
                            AdapterErrorKind::Protocol,
                            "malformed_response",
                            "invalid JSON object cell",
                        )
                    })?
                    .len() as u64;
                self.charge_raw_cell(serialized, false)
            }
        }
    }

    pub(crate) fn charge_bytes(&mut self, count: u64) -> Result<(), DactylError> {
        self.bytes = self.bytes.checked_add(count).ok_or_else(|| {
            result_limit(
                "result_budget_exceeded",
                "aggregate decoded result bytes overflowed u64",
            )
        })?;
        if self.bytes > self.budget.max_result_bytes {
            return Err(result_limit(
                "result_budget_exceeded",
                "aggregate decoded result bytes exceed max_result_bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bounded_json_writer_stops_before_allocating_oversized_copy() {
        let large = json!({ "payload": "x".repeat(1024 * 1024) });
        let error = serialize_bounded_json(&large, 1024).unwrap_err();
        assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Quota));
        assert_eq!(error.adapter_code(), Some("payload_too_large"));
        assert_eq!(error.failure_origin(), FailureOrigin::LocalBudget);
        assert_eq!(error.mutation_outcome(), MutationOutcome::NotAttempted);
    }

    #[test]
    fn bounded_body_reader_accepts_exact_limit_and_rejects_limit_plus_one() {
        let exact = vec![b'a'; 16_384];
        let got = read_bounded_body(exact.as_slice(), 16_384).unwrap();
        assert_eq!(got.len(), 16_384);

        let over = vec![b'a'; 16_385];
        assert!(matches!(
            read_bounded_body(over.as_slice(), 16_384),
            Err(BoundedReadError::TooLarge)
        ));
    }

    #[test]
    fn checked_arithmetic_catches_overflow_in_result_tracker() {
        let budget = ResourceBudget::v1()
            .with_max_cell_bytes(u64::MAX)
            .with_max_result_bytes(u64::MAX);
        let mut tracker = DecodedResultTracker::new(budget);
        let error = tracker.charge_raw_cell(u64::MAX / 16, true).unwrap_err();
        assert_eq!(error.adapter_code(), Some("result_budget_exceeded"));
        assert_eq!(error.failure_origin(), FailureOrigin::LocalBudget);
    }

    #[test]
    fn budget_validation_rejects_zero_timeouts() {
        assert!(ResourceBudget::v1()
            .with_connect_timeout(Duration::ZERO)
            .validate()
            .is_err());
        assert!(ResourceBudget::v1()
            .with_request_timeout(Duration::ZERO)
            .validate()
            .is_err());
    }

    #[test]
    fn unit_helpers_cover_preflight_depth_io_and_tracker_paths() {
        struct FailingReader;
        impl std::io::Read for FailingReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "simulated body timeout",
                ))
            }
        }

        match read_bounded_body(FailingReader, 1024) {
            Err(BoundedReadError::Io(err)) => {
                assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
            }
            other => panic!("expected BoundedReadError::Io, got {other:?}"),
        }

        let budget = ResourceBudget::v1()
            .with_max_sql_bytes(16)
            .with_max_param_bytes(16)
            .with_max_cell_bytes(8)
            .with_max_batch_operations(2)
            .with_max_columns(2)
            .with_max_rows(2)
            .with_max_result_bytes(4096);

        assert_eq!(
            preflight_statement(&budget, "SELECT 1", &[Parameter::Text("abcd".into())]).unwrap(),
            4
        );
        assert!(preflight_statement(&budget, "SELECT 0123456789ABCDEF", &[]).is_err());
        assert!(preflight_batch(
            &budget,
            &[
                Operation::read("SELECT 1", []),
                Operation::write("DELETE FROM t", []),
            ]
        )
        .is_ok());

        let rolled_back = mark_local_rollback(result_limit("row_budget_exceeded", "too many rows"));
        assert_eq!(rolled_back.mutation_outcome(), MutationOutcome::RolledBack);
        assert_eq!(rolled_back.failure_origin(), FailureOrigin::LocalBudget);

        let uncertain = mark_remote_uncertain(DactylError::adapter(
            AdapterErrorKind::Timeout,
            "request timed out",
        ));
        assert_eq!(uncertain.mutation_outcome(), MutationOutcome::Uncertain);

        let mut nested = json!("leaf");
        for _ in 0..64 {
            nested = json!([nested]);
        }
        assert!(ensure_json_depth(&nested).is_ok());
        nested = json!([nested]);
        assert_eq!(
            ensure_json_depth(&nested).unwrap_err().failure_origin(),
            FailureOrigin::MalformedResponse
        );

        let mut tracker = DecodedResultTracker::new(budget);
        let cols = vec!["id".to_owned(), "payload".to_owned()];
        tracker.charge_columns(&cols).unwrap();
        tracker.charge_row_overhead(&cols).unwrap();
        tracker.charge_json_value(&json!(42)).unwrap();
        tracker.charge_json_value(&json!([1, 2, 3])).unwrap();
    }
}
