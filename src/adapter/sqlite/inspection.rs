//! Scoped SQLite read transaction and incremental, budgeted row decoding.

use std::cell::Cell;
use std::ffi::{c_int, c_void, CStr};
use std::time::{Duration, Instant};

use super::{exec_sql, ffi, SqliteConnection, Statement};
use crate::{
    AdapterErrorKind, DactylError, Parameter, Row, ScanOutcome, SnapshotLimits, SnapshotReader,
};

fn limit_error() -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Quota,
        "inspection_budget_exceeded",
        "inspection budget exhausted; reduce the scan or explicitly authorize a larger budget",
    )
}

struct Budget {
    deadline: Instant,
    steps: Cell<u64>,
    queries: Cell<u64>,
    rows: Cell<u64>,
    bytes: Cell<u64>,
    exhausted: Cell<bool>,
}

impl Budget {
    fn check(&self) -> Result<(), DactylError> {
        if self.exhausted.get() || Instant::now() >= self.deadline {
            self.exhausted.set(true);
            Err(limit_error())
        } else {
            Ok(())
        }
    }

    fn charge(&self, remaining: &Cell<u64>, count: u64) -> Result<(), DactylError> {
        self.check()?;
        match remaining.get().checked_sub(count) {
            Some(value) => {
                remaining.set(value);
                Ok(())
            }
            None => {
                self.exhausted.set(true);
                Err(limit_error())
            }
        }
    }
}

unsafe extern "C" fn progress(context: *mut c_void) -> c_int {
    // The pinned Box and registration are owned by Guard, which unregisters
    // before dropping it on success, error and unwind. No SQLite reentry.
    let budget = &*context.cast::<Budget>();
    i32::from(budget.charge(&budget.steps, 1000).is_err())
}

struct Guard<'a> {
    connection: &'a SqliteConnection,
    budget: Box<Budget>,
    previous_limits: [(c_int, c_int); 3],
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        unsafe {
            self.connection.api.progress_handler(
                self.connection.database,
                0,
                None,
                std::ptr::null_mut(),
            );
            for (kind, old) in self.previous_limits {
                self.connection
                    .api
                    .limit(self.connection.database, kind, old);
            }
        }
        // Always terminate the read transaction, including callback errors and
        // deliberate early stop. No commit or WAL checkpoint is performed.
        let _ = exec_sql(self.connection, "ROLLBACK");
    }
}

pub(super) fn inspect_snapshot(
    connection: &SqliteConnection,
    limits: SnapshotLimits,
    inspect: &mut dyn FnMut(&mut dyn SnapshotReader) -> Result<(), DactylError>,
) -> Result<(), DactylError> {
    if limits.duration.is_zero()
        || limits.duration > Duration::from_secs(300)
        || limits.vm_steps < 1000
        || limits.queries == 0
        || limits.rows == 0
        || limits.result_bytes == 0
        || limits.cell_bytes == 0
        || limits.columns == 0
        || limits.sql_bytes == 0
        || limits.cell_bytes > c_int::MAX as u64
        || limits.sql_bytes > c_int::MAX as usize
        || limits.columns > c_int::MAX as u32
    {
        return Err(DactylError::adapter_with_code(
            AdapterErrorKind::InvalidOperation,
            "invalid_inspection_budget",
            "inspection limits must be positive, representable and at most 300 seconds",
        ));
    }
    exec_sql(connection, "BEGIN DEFERRED")
        .map_err(|error| connection.error_from_failure("begin inspection snapshot", error))?;
    let budget = Box::new(Budget {
        deadline: Instant::now() + limits.duration,
        steps: Cell::new(limits.vm_steps),
        queries: Cell::new(limits.queries),
        rows: Cell::new(limits.rows),
        bytes: Cell::new(limits.result_bytes),
        exhausted: Cell::new(false),
    });
    let mut previous_limits = [(0, 0); 3];
    unsafe {
        for (slot, (kind, value)) in previous_limits.iter_mut().zip([
            (0, limits.cell_bytes as c_int), // SQLITE_LIMIT_LENGTH (also row bytes)
            (1, limits.sql_bytes as c_int),  // SQLITE_LIMIT_SQL_LENGTH
            (2, limits.columns as c_int),    // SQLITE_LIMIT_COLUMN
        ]) {
            let old = connection.api.limit(connection.database, kind, -1);
            connection
                .api
                .limit(connection.database, kind, old.min(value));
            *slot = (kind, old);
        }
        connection.api.progress_handler(
            connection.database,
            1000,
            Some(progress),
            (&*budget as *const Budget).cast_mut().cast(),
        );
    }
    let guard = Guard {
        connection,
        budget,
        previous_limits,
    };
    let mut reader = Reader {
        guard: &guard,
        limits,
    };
    inspect(&mut reader)?;
    guard.budget.check()
}

struct Reader<'a, 'b> {
    guard: &'a Guard<'b>,
    limits: SnapshotLimits,
}

impl SnapshotReader for Reader<'_, '_> {
    fn visit(
        &mut self,
        sql: &str,
        params: &[Parameter],
        visitor: &mut dyn FnMut(&Row) -> Result<bool, DactylError>,
    ) -> Result<ScanOutcome, DactylError> {
        let budget = &self.guard.budget;
        budget.charge(&budget.queries, 1)?;
        if sql.len() > self.limits.sql_bytes {
            return Err(limit_error());
        }
        // PRAGMA may have side effects at prepare time; it is deliberately not
        // accepted. Use SQLite's read-only pragma table-valued SELECTs instead.
        let first = sql
            .trim_start()
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or("");
        if !first.eq_ignore_ascii_case("select") && !first.eq_ignore_ascii_case("with") {
            return Err(DactylError::adapter_with_code(
                AdapterErrorKind::ReadOnly,
                "inspection_select_required",
                "inspection accepts SELECT/WITH only",
            ));
        }
        for param in params {
            let size = match param {
                Parameter::Text(value) => value.len() as u64,
                Parameter::Blob(value) => value.len() as u64,
                _ => 8,
            };
            if size > self.limits.cell_bytes {
                return Err(limit_error());
            }
            budget.charge(&budget.bytes, size)?;
        }
        let connection = self.guard.connection;
        let result = (|| {
            let mut statement = Statement::prepare(connection, sql)?;
            if !unsafe { connection.api.stmt_readonly(statement.statement) } {
                return Err(DactylError::adapter_with_code(
                    AdapterErrorKind::ReadOnly,
                    "inspection_write_rejected",
                    "inspection cannot execute a mutation",
                ));
            }
            statement.bind_all(params)?;
            let count = unsafe { connection.api.column_count(statement.statement) };
            if count < 0 || count as u32 > self.limits.columns {
                return Err(limit_error());
            }
            let mut columns = Vec::with_capacity(count as usize);
            for index in 0..count {
                let pointer = unsafe { connection.api.column_name(statement.statement, index) };
                if pointer.is_null() {
                    return Err(limit_error());
                }
                let name = unsafe { CStr::from_ptr(pointer) };
                budget.charge(&budget.bytes, name.to_bytes().len() as u64 + 32)?;
                columns.push(name.to_string_lossy().into_owned());
            }
            let mut rows = 0;
            while statement.step()? == ffi::SQLITE_ROW {
                budget.charge(&budget.rows, 1)?;
                let mut charge = 64_u64;
                for index in 0..count {
                    let size = unsafe { connection.api.column_bytes(statement.statement, index) }
                        .max(0) as u64;
                    if size > self.limits.cell_bytes {
                        return Err(limit_error());
                    }
                    // Legacy blobs expand to per-byte JSON Values. Charge that
                    // representation before allocation, including row metadata.
                    let multiplier =
                        if unsafe { connection.api.column_type(statement.statement, index) }
                            == ffi::SQLITE_BLOB
                        {
                            32
                        } else {
                            1
                        };
                    charge = charge
                        .checked_add(
                            size.checked_mul(multiplier).ok_or_else(limit_error)?
                                + 32
                                + columns[index as usize].len() as u64,
                        )
                        .ok_or_else(limit_error)?;
                }
                budget.charge(&budget.bytes, charge)?;
                let values = (0..count)
                    .map(|index| statement.value(index))
                    .collect::<Result<Vec<_>, _>>()?;
                let row = Row {
                    columns: columns.clone(),
                    values,
                };
                rows += 1;
                if !visitor(&row)? {
                    return Ok(ScanOutcome {
                        rows,
                        complete: false,
                    });
                }
            }
            Ok(ScanOutcome {
                rows,
                complete: true,
            })
        })();
        if budget.exhausted.get()
            || result.as_ref().err().and_then(DactylError::adapter_code) == Some("value_too_large")
        {
            Err(limit_error())
        } else {
            result
        }
    }
}
