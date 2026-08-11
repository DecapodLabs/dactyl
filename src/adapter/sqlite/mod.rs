//! Lightweight Dactyl-owned local storage.
//!
//! The module name is retained for route compatibility. This implementation
//! has no SQLite dependency and does not read or write SQLite files. It uses a
//! versioned JSON snapshot, a checksummed sidecar journal, and a lock file.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::adapter::Adapter;
use crate::contract::{
    AccessMode, AtomicResult, GeneratedKey, OpenOptions, Operation, OperationKind, OperationResult,
    WriteResult,
};
use crate::error::{AdapterErrorKind, DactylError};
use crate::rows::{Parameter, Row, Rows};

const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Store {
    format_version: u32,
    tables: BTreeMap<String, Table>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            tables: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Vec<serde_json::Value>>,
    next_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Column {
    name: String,
    primary_key: bool,
    unique: bool,
    not_null: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Journal {
    format_version: u32,
    checksum: u64,
    store: Store,
}

pub struct SqliteAdapter {
    path: Option<PathBuf>,
    options: OpenOptions,
    state: Arc<Mutex<Store>>,
}

impl SqliteAdapter {
    pub fn open_with_options(path: &str, options: OpenOptions) -> Result<Self, DactylError> {
        if path == ":memory:" {
            return Ok(Self {
                path: None,
                options,
                state: Arc::new(Mutex::new(Store::default())),
            });
        }
        let path = PathBuf::from(path);
        if options.access_mode == AccessMode::ReadWrite {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent).map_err(|e| storage_error("create parent", e))?;
            }
        } else if !path.exists() {
            return Err(adapter_error(
                AdapterErrorKind::ReadOnly,
                "read-only route cannot open a missing store",
            ));
        }
        let store = load_store(&path, options.access_mode)?;
        Ok(Self {
            path: Some(path),
            options,
            state: Arc::new(Mutex::new(store)),
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, Store>, DactylError> {
        self.state
            .lock()
            .map_err(|_| adapter_error(AdapterErrorKind::Storage, "store lock poisoned"))
    }

    fn file_lock(&self) -> Result<Option<FileLock>, DactylError> {
        match &self.path {
            Some(path) => FileLock::acquire(&lock_path(path), self.options.lock_timeout).map(Some),
            None => Ok(None),
        }
    }
}

impl Adapter for SqliteAdapter {
    fn read(&self, sql: &str, params: &[Parameter]) -> Result<Rows, DactylError> {
        let mut state = self.state()?;
        match execute_sql(&mut state, sql, params, OperationKind::Read)? {
            OperationResult::Rows(rows) => Ok(rows),
            OperationResult::Write(_) => Err(adapter_error(
                AdapterErrorKind::InvalidOperation,
                "read requires SELECT",
            )),
        }
    }

    fn write(&self, sql: &str, params: &[Parameter]) -> Result<WriteResult, DactylError> {
        ensure_writable(self.options.access_mode)?;
        let _lock = self.file_lock()?;
        let mut state = self.state()?;
        if let Some(path) = &self.path {
            *state = load_store(path, AccessMode::ReadWrite)?;
        }
        let mut candidate = state.clone();
        let result = execute_sql(&mut candidate, sql, params, OperationKind::Write)?;
        let result = match result {
            OperationResult::Write(result) => result,
            OperationResult::Rows(_) => {
                return Err(adapter_error(
                    AdapterErrorKind::InvalidOperation,
                    "write requires a mutating statement",
                ))
            }
        };
        if let Some(path) = &self.path {
            persist_store(path, &candidate)?;
        }
        *state = candidate;
        Ok(result)
    }

    fn atomic(&self, operations: &[Operation]) -> Result<AtomicResult, DactylError> {
        if operations.is_empty() {
            return Ok(AtomicResult::default());
        }
        let mut mutates = false;
        for operation in operations {
            if operation.kind != OperationKind::Read {
                mutates = true;
            }
        }
        if mutates {
            ensure_writable(self.options.access_mode)?;
        }
        let _lock = if mutates { self.file_lock()? } else { None };
        let mut state = self.state()?;
        if mutates {
            if let Some(path) = &self.path {
                *state = load_store(path, AccessMode::ReadWrite)?;
            }
        }
        let mut candidate = state.clone();
        let mut results = Vec::with_capacity(operations.len());
        for operation in operations {
            results.push(execute_operation(&mut candidate, operation)?);
        }
        if mutates {
            if let Some(path) = &self.path {
                persist_store(path, &candidate)?;
            }
            *state = candidate;
        }
        Ok(AtomicResult { results })
    }

    fn access_mode(&self) -> AccessMode {
        self.options.access_mode
    }
}

fn execute_operation(
    store: &mut Store,
    operation: &Operation,
) -> Result<OperationResult, DactylError> {
    let first = first_word(&operation.sql);
    if operation.kind == OperationKind::Read && first.as_deref() != Some("select") {
        return Err(adapter_error(
            AdapterErrorKind::InvalidOperation,
            "read operation requires SELECT",
        ));
    }
    if operation.kind == OperationKind::Schema
        && !matches!(first.as_deref(), Some("create" | "alter" | "drop"))
    {
        return Err(adapter_error(
            AdapterErrorKind::InvalidOperation,
            "schema operation requires schema SQL",
        ));
    }
    execute_sql(store, &operation.sql, &operation.params, operation.kind)
}

fn execute_sql(
    store: &mut Store,
    sql: &str,
    params: &[Parameter],
    kind: OperationKind,
) -> Result<OperationResult, DactylError> {
    let mut parser = Parser::new(sql)?;
    match parser.word()?.as_str() {
        "select" => select(store, &mut parser, params),
        "insert" => {
            reject_read(kind)?;
            insert(store, &mut parser, params)
        }
        "update" => {
            reject_read(kind)?;
            update(store, &mut parser, params)
        }
        "delete" => {
            reject_read(kind)?;
            delete(store, &mut parser, params)
        }
        "create" => {
            reject_read(kind)?;
            create_table(store, &mut parser)
        }
        "alter" => {
            reject_read(kind)?;
            alter_table(store, &mut parser)
        }
        "drop" => {
            reject_read(kind)?;
            drop_table(store, &mut parser)
        }
        "begin" | "commit" | "rollback" => Err(adapter_error(
            AdapterErrorKind::InvalidOperation,
            "use Connection::atomic for transaction boundaries",
        )),
        other => Err(adapter_error(
            AdapterErrorKind::Capability,
            format!("unsupported SQL statement {other:?}"),
        )),
    }
}

fn select(
    store: &Store,
    parser: &mut Parser,
    params: &[Parameter],
) -> Result<OperationResult, DactylError> {
    let mut projections = Vec::new();
    loop {
        let expr = if parser.symbol('*') {
            Expr::Star
        } else {
            parser.expr()?
        };
        let label = if parser.word_is("as") {
            parser.word()?
        } else {
            expr.label()
        };
        projections.push((expr, label));
        if !parser.symbol(',') {
            break;
        }
    }
    let table = if parser.word_is("from") {
        Some(get_table(store, &parser.word()?)?)
    } else {
        None
    };
    let condition = if parser.word_is("where") {
        Some(parser.condition()?)
    } else {
        None
    };
    let order = if parser.word_is("order") {
        parser.expect_word("by")?;
        let column = parser.word()?;
        let descending = if parser.word_is("desc") {
            true
        } else {
            parser.word_is("asc");
            false
        };
        Some((column, descending))
    } else {
        None
    };
    let limit = if parser.word_is("limit") {
        Some(parser.expr()?)
    } else {
        None
    };
    parser.finish()?;

    let (columns, mut source) = match table {
        Some(table) => (
            table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            table.rows.clone(),
        ),
        None => (Vec::new(), vec![Vec::new()]),
    };
    source.retain(|row| {
        condition
            .as_ref()
            .map_or(true, |c| test_condition(c, table, row, params))
    });
    if let Some((column, descending)) = order {
        let table = table.ok_or_else(|| query_error("ORDER BY requires FROM"))?;
        let index = column_index(table, &column)?;
        source.sort_by(|left, right| {
            let order = compare(&left[index], &right[index]);
            if descending {
                order.reverse()
            } else {
                order
            }
        });
    }
    if let Some(limit) = limit {
        let value = eval(
            &limit,
            table,
            source.first().map_or(&[], Vec::as_slice),
            params,
        )?;
        let value = value
            .as_i64()
            .ok_or_else(|| adapter_error(AdapterErrorKind::Value, "LIMIT must be an integer"))?;
        source.truncate(value.max(0) as usize);
    }
    let projections = if projections.len() == 1 && matches!(projections[0].0, Expr::Star) {
        columns
            .iter()
            .map(|c| (Expr::Column(c.clone()), c.clone()))
            .collect::<Vec<_>>()
    } else {
        projections
    };
    let rows = source
        .iter()
        .map(|row| {
            Ok(Row {
                columns: projections.iter().map(|(_, label)| label.clone()).collect(),
                values: projections
                    .iter()
                    .map(|(expr, _)| eval(expr, table, row, params))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect::<Result<Vec<_>, DactylError>>()?;
    Ok(OperationResult::Rows(Rows(rows)))
}

fn insert(
    store: &mut Store,
    parser: &mut Parser,
    params: &[Parameter],
) -> Result<OperationResult, DactylError> {
    let ignore = if parser.word_is("or") {
        parser.expect_word("ignore")?;
        true
    } else {
        false
    };
    parser.expect_word("into")?;
    let name = parser.word()?;
    let table_key = normalize(&name);
    let table = store
        .tables
        .get_mut(&table_key)
        .ok_or_else(|| missing_table(&name))?;
    let columns = if parser.symbol('(') {
        let mut columns = Vec::new();
        loop {
            columns.push(parser.word()?);
            if !parser.symbol(',') {
                break;
            }
        }
        parser.expect_symbol(')')?;
        columns
    } else {
        table.columns.iter().map(|c| c.name.clone()).collect()
    };
    parser.expect_word("values")?;
    let mut rows = Vec::new();
    loop {
        parser.expect_symbol('(')?;
        let mut values = Vec::new();
        loop {
            values.push(parser.expr()?);
            if !parser.symbol(',') {
                break;
            }
        }
        parser.expect_symbol(')')?;
        rows.push(values);
        if !parser.symbol(',') {
            break;
        }
    }
    parser.finish()?;
    let indices = columns
        .iter()
        .map(|c| column_index(table, c))
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = WriteResult::default();
    for expressions in rows {
        if expressions.len() != indices.len() {
            return Err(query_error("INSERT value count does not match columns"));
        }
        let mut row = vec![serde_json::Value::Null; table.columns.len()];
        for (index, expr) in expressions.iter().enumerate() {
            row[indices[index]] = eval(expr, Some(table), &row, params)?;
        }
        let mut generated = None;
        if let Some(index) = table.columns.iter().position(|c| c.primary_key) {
            if row[index].is_null() {
                row[index] = table.next_id.into();
                generated = Some(table.next_id);
                table.next_id += 1;
            } else if let Some(value) = row[index].as_i64() {
                table.next_id = table.next_id.max(value + 1);
            }
        }
        if let Err(error) = validate(table, &row, None) {
            if ignore
                && matches!(
                    error,
                    DactylError::Adapter {
                        kind: AdapterErrorKind::Constraint,
                        ..
                    }
                )
            {
                continue;
            }
            return Err(error);
        }
        table.rows.push(row);
        result.affected_rows += 1;
        if let Some(key) = generated {
            result.generated_keys.push(GeneratedKey::Integer(key));
        }
    }
    Ok(OperationResult::Write(result))
}

fn update(
    store: &mut Store,
    parser: &mut Parser,
    params: &[Parameter],
) -> Result<OperationResult, DactylError> {
    let name = parser.word()?;
    let key = normalize(&name);
    let table = store
        .tables
        .get_mut(&key)
        .ok_or_else(|| missing_table(&name))?;
    parser.expect_word("set")?;
    let mut assignments = Vec::new();
    loop {
        let column = parser.word()?;
        parser.expect_operator("=")?;
        assignments.push((column, parser.expr()?));
        if !parser.symbol(',') {
            break;
        }
    }
    let condition = if parser.word_is("where") {
        Some(parser.condition()?)
    } else {
        None
    };
    parser.finish()?;
    for (column, _) in &assignments {
        column_index(table, column)?;
    }
    let snapshot = table.clone();
    let mut affected = 0;
    for (row_index, original) in snapshot.rows.iter().enumerate() {
        if !condition.as_ref().map_or(true, |c| {
            test_condition(c, Some(&snapshot), original, params)
        }) {
            continue;
        }
        let mut row = original.clone();
        for (column, expr) in &assignments {
            let index = column_index(table, column)?;
            row[index] = eval(expr, Some(&snapshot), &row, params)?;
        }
        validate(table, &row, Some(row_index))?;
        table.rows[row_index] = row;
        affected += 1;
    }
    Ok(OperationResult::Write(WriteResult {
        affected_rows: affected,
        generated_keys: Vec::new(),
    }))
}

fn delete(
    store: &mut Store,
    parser: &mut Parser,
    params: &[Parameter],
) -> Result<OperationResult, DactylError> {
    parser.expect_word("from")?;
    let name = parser.word()?;
    let key = normalize(&name);
    let table = store
        .tables
        .get_mut(&key)
        .ok_or_else(|| missing_table(&name))?;
    let condition = if parser.word_is("where") {
        Some(parser.condition()?)
    } else {
        None
    };
    parser.finish()?;
    let snapshot = table.clone();
    table.rows.retain(|row| {
        !condition
            .as_ref()
            .map_or(true, |c| test_condition(c, Some(&snapshot), row, params))
    });
    Ok(OperationResult::Write(WriteResult {
        affected_rows: (snapshot.rows.len() - table.rows.len()) as u64,
        generated_keys: Vec::new(),
    }))
}

fn create_table(store: &mut Store, parser: &mut Parser) -> Result<OperationResult, DactylError> {
    parser.expect_word("table")?;
    let if_not_exists = if parser.word_is("if") {
        parser.expect_word("not")?;
        parser.expect_word("exists")?;
        true
    } else {
        false
    };
    let name = parser.word()?;
    let key = normalize(&name);
    if store.tables.contains_key(&key) {
        if if_not_exists {
            return Ok(OperationResult::Write(WriteResult::default()));
        }
        return Err(adapter_error(
            AdapterErrorKind::Constraint,
            format!("table {name:?} already exists"),
        ));
    }
    parser.expect_symbol('(')?;
    let mut columns = Vec::new();
    let mut table_primary = Vec::new();
    let mut table_unique = Vec::new();
    loop {
        if parser.word_is("primary") {
            parser.expect_word("key")?;
            table_primary = parser.name_list()?;
        } else if parser.word_is("unique") {
            table_unique.extend(parser.name_list()?);
        } else {
            let name = parser.word()?;
            let mut column = Column {
                name: normalize(&name),
                primary_key: false,
                unique: false,
                not_null: false,
            };
            while !parser.peek_symbol(',') && !parser.peek_symbol(')') {
                if parser.word_is("primary") {
                    parser.expect_word("key")?;
                    column.primary_key = true;
                } else if parser.word_is("unique") {
                    column.unique = true;
                } else if parser.word_is("not") {
                    parser.expect_word("null")?;
                    column.not_null = true;
                } else {
                    parser
                        .next()
                        .ok_or_else(|| query_error("invalid column definition"))?;
                }
            }
            columns.push(column);
        }
        if parser.symbol(',') {
            continue;
        }
        parser.expect_symbol(')')?;
        break;
    }
    parser.finish()?;
    for name in table_primary {
        if let Some(c) = columns.iter_mut().find(|c| c.name == normalize(&name)) {
            c.primary_key = true;
        }
    }
    for name in table_unique {
        if let Some(c) = columns.iter_mut().find(|c| c.name == normalize(&name)) {
            c.unique = true;
        }
    }
    if columns.is_empty() {
        return Err(query_error("table requires a column"));
    }
    store.tables.insert(
        key,
        Table {
            name,
            columns,
            rows: Vec::new(),
            next_id: 1,
        },
    );
    Ok(OperationResult::Write(WriteResult::default()))
}

fn alter_table(store: &mut Store, parser: &mut Parser) -> Result<OperationResult, DactylError> {
    parser.expect_word("table")?;
    let name = parser.word()?;
    parser.expect_word("add")?;
    let _ = parser.word_is("column");
    let column_name = parser.word()?;
    parser.finish()?;
    let table = store
        .tables
        .get_mut(&normalize(&name))
        .ok_or_else(|| missing_table(&name))?;
    if has_column(table, &column_name) {
        return Err(adapter_error(
            AdapterErrorKind::Constraint,
            "column already exists",
        ));
    }
    table.columns.push(Column {
        name: normalize(&column_name),
        primary_key: false,
        unique: false,
        not_null: false,
    });
    for row in &mut table.rows {
        row.push(serde_json::Value::Null);
    }
    Ok(OperationResult::Write(WriteResult::default()))
}

fn drop_table(store: &mut Store, parser: &mut Parser) -> Result<OperationResult, DactylError> {
    parser.expect_word("table")?;
    let if_exists = if parser.word_is("if") {
        parser.expect_word("exists")?;
        true
    } else {
        false
    };
    let name = parser.word()?;
    parser.finish()?;
    if store.tables.remove(&normalize(&name)).is_none() && !if_exists {
        return Err(missing_table(&name));
    }
    Ok(OperationResult::Write(WriteResult::default()))
}

fn validate(
    table: &Table,
    row: &[serde_json::Value],
    except: Option<usize>,
) -> Result<(), DactylError> {
    for (index, column) in table.columns.iter().enumerate() {
        if (column.primary_key || column.not_null) && row[index].is_null() {
            return Err(adapter_error(
                AdapterErrorKind::Constraint,
                format!("column {} may not be null", column.name),
            ));
        }
        if column.primary_key || column.unique {
            for (other_index, other) in table.rows.iter().enumerate() {
                if except == Some(other_index) {
                    continue;
                }
                if !row[index].is_null() && row[index] == other[index] {
                    return Err(adapter_error(
                        AdapterErrorKind::Constraint,
                        format!("unique constraint failed: {}", column.name),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum Expr {
    Literal(serde_json::Value),
    Param(usize),
    Column(String),
    Star,
}
impl Expr {
    fn label(&self) -> String {
        match self {
            Self::Column(name) => name.clone(),
            Self::Literal(_) => "value".into(),
            Self::Param(index) => format!("param_{}", index + 1),
            Self::Star => "*".into(),
        }
    }
}
#[derive(Debug, Clone)]
enum Condition {
    Compare(Expr, String, Expr),
    IsNull(Expr, bool),
    And(Box<Condition>, Box<Condition>),
}

fn test_condition(
    condition: &Condition,
    table: Option<&Table>,
    row: &[serde_json::Value],
    params: &[Parameter],
) -> bool {
    match condition {
        Condition::Compare(left, op, right) => {
            let left = eval(left, table, row, params).unwrap_or(serde_json::Value::Null);
            let right = eval(right, table, row, params).unwrap_or(serde_json::Value::Null);
            let order = compare(&left, &right);
            match op.as_str() {
                "=" => left == right,
                "<>" | "!=" => left != right,
                "<" => order == Ordering::Less,
                ">" => order == Ordering::Greater,
                "<=" => order != Ordering::Greater,
                ">=" => order != Ordering::Less,
                _ => false,
            }
        }
        Condition::IsNull(expr, negated) => {
            eval(expr, table, row, params).map_or(true, |value| value.is_null()) != *negated
        }
        Condition::And(left, right) => {
            test_condition(left, table, row, params) && test_condition(right, table, row, params)
        }
    }
}

fn eval(
    expr: &Expr,
    table: Option<&Table>,
    row: &[serde_json::Value],
    params: &[Parameter],
) -> Result<serde_json::Value, DactylError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Param(index) => {
            let value = params
                .get(*index)
                .ok_or_else(|| query_error("missing SQL parameter"))?;
            Ok(match value {
                Parameter::Null => serde_json::Value::Null,
                Parameter::Bool(value) => (*value).into(),
                Parameter::Integer(value) => (*value).into(),
                Parameter::Real(value) => serde_json::Number::from_f64(*value)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                Parameter::Text(value) => value.clone().into(),
                Parameter::Blob(value) => serde_json::Value::Array(
                    value
                        .iter()
                        .map(|v| serde_json::Value::Number((*v as u64).into()))
                        .collect(),
                ),
            })
        }
        Expr::Column(name) => {
            let table = table.ok_or_else(|| DactylError::ColumnNotFound(name.clone()))?;
            Ok(row[column_index(table, name)?].clone())
        }
        Expr::Star => Err(query_error("star is only valid as a projection")),
    }
}

fn compare(left: &serde_json::Value, right: &serde_json::Value) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        return left.partial_cmp(&right).unwrap_or(Ordering::Equal);
    }
    left.to_string().cmp(&right.to_string())
}

#[derive(Debug, Clone)]
enum Token {
    Word(String),
    Number(String),
    String(String),
    Param(Option<usize>),
    Symbol(char),
    Operator(String),
}
struct Parser {
    tokens: Vec<Token>,
    index: usize,
    positional: usize,
}
impl Parser {
    fn new(sql: &str) -> Result<Self, DactylError> {
        Ok(Self {
            tokens: lex(sql)?,
            index: 0,
            positional: 0,
        })
    }
    fn next(&mut self) -> Option<Token> {
        let result = self.tokens.get(self.index).cloned();
        if result.is_some() {
            self.index += 1;
        }
        result
    }
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }
    fn done(&self) -> bool {
        self.index == self.tokens.len()
    }
    fn word(&mut self) -> Result<String, DactylError> {
        match self.next() {
            Some(Token::Word(value)) => Ok(value),
            _ => Err(query_error("expected SQL word")),
        }
    }
    fn expect_word(&mut self, expected: &str) -> Result<(), DactylError> {
        let value = self.word()?;
        if value == expected {
            Ok(())
        } else {
            Err(query_error(format!("expected {expected:?}")))
        }
    }
    fn word_is(&mut self, expected: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(value)) if value == expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }
    fn symbol(&mut self, expected: char) -> bool {
        if self.peek_symbol(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }
    fn peek_symbol(&self, expected: char) -> bool {
        matches!(self.peek(), Some(Token::Symbol(value)) if *value == expected)
    }
    fn expect_symbol(&mut self, expected: char) -> Result<(), DactylError> {
        if self.symbol(expected) {
            Ok(())
        } else {
            Err(query_error(format!("expected {expected:?}")))
        }
    }
    fn expect_operator(&mut self, expected: &str) -> Result<(), DactylError> {
        match self.next() {
            Some(Token::Operator(value)) if value == expected => Ok(()),
            _ => Err(query_error("expected comparison operator")),
        }
    }
    fn expr(&mut self) -> Result<Expr, DactylError> {
        match self.next() {
            Some(Token::String(value)) => Ok(Expr::Literal(value.into())),
            Some(Token::Number(value)) if value.contains('.') => Ok(Expr::Literal(
                serde_json::Number::from_f64(
                    value.parse().map_err(|_| query_error("invalid number"))?,
                )
                .ok_or_else(|| query_error("invalid number"))?
                .into(),
            )),
            Some(Token::Number(value)) => Ok(Expr::Literal(
                value
                    .parse::<i64>()
                    .map_err(|_| query_error("invalid integer"))?
                    .into(),
            )),
            Some(Token::Param(position)) => {
                let index = position.unwrap_or_else(|| {
                    let index = self.positional;
                    self.positional += 1;
                    index
                });
                Ok(Expr::Param(index))
            }
            Some(Token::Word(value)) if value == "null" => {
                Ok(Expr::Literal(serde_json::Value::Null))
            }
            Some(Token::Word(value)) if value == "true" => Ok(Expr::Literal(true.into())),
            Some(Token::Word(value)) if value == "false" => Ok(Expr::Literal(false.into())),
            Some(Token::Word(value)) => Ok(Expr::Column(value)),
            _ => Err(query_error("expected expression")),
        }
    }
    fn condition(&mut self) -> Result<Condition, DactylError> {
        let mut condition = self.simple_condition()?;
        while self.word_is("and") {
            condition = Condition::And(Box::new(condition), Box::new(self.simple_condition()?));
        }
        Ok(condition)
    }
    fn simple_condition(&mut self) -> Result<Condition, DactylError> {
        let left = self.expr()?;
        if self.word_is("is") {
            let negated = self.word_is("not");
            self.expect_word("null")?;
            return Ok(Condition::IsNull(left, negated));
        }
        let operator = match self.next() {
            Some(Token::Operator(value)) => value,
            _ => return Err(query_error("expected comparison operator")),
        };
        Ok(Condition::Compare(left, operator, self.expr()?))
    }
    fn name_list(&mut self) -> Result<Vec<String>, DactylError> {
        self.expect_symbol('(')?;
        let mut names = Vec::new();
        loop {
            names.push(self.word()?);
            if !self.symbol(',') {
                break;
            }
        }
        self.expect_symbol(')')?;
        Ok(names)
    }
    fn finish(&self) -> Result<(), DactylError> {
        if self.done() {
            Ok(())
        } else {
            Err(query_error("unsupported trailing SQL"))
        }
    }
}

fn lex(sql: &str) -> Result<Vec<Token>, DactylError> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            i += 1;
            let mut value = String::new();
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        value.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    value.push(bytes[i] as char);
                    i += 1;
                }
            }
            tokens.push(Token::String(value));
            continue;
        }
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(Token::Word(
                String::from_utf8_lossy(&bytes[start..i]).to_ascii_lowercase(),
            ));
            continue;
        }
        if bytes[i].is_ascii_digit()
            || (bytes[i] == b'-' && bytes.get(i + 1).is_some_and(|b| b.is_ascii_digit()))
        {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                i += 1;
            }
            tokens.push(Token::Number(
                String::from_utf8_lossy(&bytes[start..i]).into_owned(),
            ));
            continue;
        }
        if bytes[i] == b'$' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let value = String::from_utf8_lossy(&bytes[start..i])
                .parse::<usize>()
                .map_err(|_| query_error("invalid parameter"))?;
            tokens.push(Token::Param(value.checked_sub(1)));
            continue;
        }
        if bytes[i] == b'?' {
            tokens.push(Token::Param(None));
            i += 1;
            continue;
        }
        if b",()*".contains(&bytes[i]) {
            tokens.push(Token::Symbol(bytes[i] as char));
            i += 1;
            continue;
        }
        if b"=<>!".contains(&bytes[i]) {
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i] == b'=' {
                i += 1;
            }
            tokens.push(Token::Operator(
                String::from_utf8_lossy(&bytes[start..i]).into_owned(),
            ));
            continue;
        }
        return Err(query_error(format!(
            "unsupported SQL character {:?}",
            bytes[i] as char
        )));
    }
    Ok(tokens)
}

fn get_table<'a>(store: &'a Store, name: &str) -> Result<&'a Table, DactylError> {
    store
        .tables
        .get(&normalize(name))
        .ok_or_else(|| missing_table(name))
}
fn column_index(table: &Table, name: &str) -> Result<usize, DactylError> {
    table
        .columns
        .iter()
        .position(|c| c.name == normalize(name))
        .ok_or_else(|| DactylError::ColumnNotFound(name.to_owned()))
}
fn has_column(table: &Table, name: &str) -> bool {
    table.columns.iter().any(|c| c.name == normalize(name))
}
fn normalize(value: &str) -> String {
    value.trim_matches('\"').to_ascii_lowercase()
}
fn first_word(sql: &str) -> Option<String> {
    sql.split_whitespace().next().map(str::to_ascii_lowercase)
}
fn reject_read(kind: OperationKind) -> Result<(), DactylError> {
    if kind == OperationKind::Read {
        Err(adapter_error(
            AdapterErrorKind::InvalidOperation,
            "read operation cannot mutate",
        ))
    } else {
        Ok(())
    }
}
fn ensure_writable(mode: AccessMode) -> Result<(), DactylError> {
    if mode == AccessMode::ReadOnly {
        Err(adapter_error(
            AdapterErrorKind::ReadOnly,
            "route is read-only",
        ))
    } else {
        Ok(())
    }
}
fn adapter_error(kind: AdapterErrorKind, message: impl Into<String>) -> DactylError {
    DactylError::adapter(kind, message)
}
fn query_error(message: impl Into<String>) -> DactylError {
    adapter_error(AdapterErrorKind::Query, message)
}
fn missing_table(name: &str) -> DactylError {
    query_error(format!("table {name:?} does not exist"))
}
fn storage_error(operation: &str, error: impl std::fmt::Display) -> DactylError {
    adapter_error(AdapterErrorKind::Storage, format!("{operation}: {error}"))
}

fn lock_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.lock", path.display()))
}
fn journal_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.wal", path.display()))
}
fn load_store(path: &Path, mode: AccessMode) -> Result<Store, DactylError> {
    let journal = journal_path(path);
    if journal.exists() {
        if mode == AccessMode::ReadOnly {
            return Err(adapter_error(
                AdapterErrorKind::ReadOnly,
                "read-only route will not recover a journal",
            ));
        }
        let bytes = fs::read(&journal).map_err(|e| storage_error("read journal", e))?;
        let journal: Journal =
            serde_json::from_slice(&bytes).map_err(|e| storage_error("decode journal", e))?;
        if journal.format_version != FORMAT_VERSION || checksum(&journal.store)? != journal.checksum
        {
            return Err(adapter_error(
                AdapterErrorKind::Storage,
                "journal checksum or version mismatch",
            ));
        }
        persist_store(path, &journal.store)?;
        fs::remove_file(journal_path(path)).map_err(|e| storage_error("remove journal", e))?;
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|e| storage_error("open store", e))?
        .read_to_end(&mut bytes)
        .map_err(|e| storage_error("read store", e))?;
    if bytes.is_empty() {
        return Ok(Store::default());
    }
    if bytes.starts_with(b"SQLite format 3") {
        return Err(adapter_error(
            AdapterErrorKind::Capability,
            "SQLite files are not accepted; import into the Dactyl format",
        ));
    }
    let store: Store =
        serde_json::from_slice(&bytes).map_err(|e| storage_error("decode store", e))?;
    if store.format_version != FORMAT_VERSION {
        return Err(adapter_error(
            AdapterErrorKind::Capability,
            "unsupported Dactyl store format",
        ));
    }
    Ok(store)
}
fn persist_store(path: &Path, store: &Store) -> Result<(), DactylError> {
    let journal = Journal {
        format_version: FORMAT_VERSION,
        checksum: checksum(store)?,
        store: store.clone(),
    };
    let journal_bytes =
        serde_json::to_vec(&journal).map_err(|e| storage_error("encode journal", e))?;
    let journal_path = journal_path(path);
    let mut file = FsOpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&journal_path)
        .map_err(|e| storage_error("open journal", e))?;
    file.write_all(&journal_bytes)
        .map_err(|e| storage_error("write journal", e))?;
    file.sync_all()
        .map_err(|e| storage_error("sync journal", e))?;
    let store_bytes = serde_json::to_vec(store).map_err(|e| storage_error("encode store", e))?;
    let temp_path = PathBuf::from(format!("{}.tmp", path.display()));
    let mut temp = FsOpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(|e| storage_error("open store temp", e))?;
    temp.write_all(&store_bytes)
        .map_err(|e| storage_error("write store", e))?;
    temp.sync_all()
        .map_err(|e| storage_error("sync store", e))?;
    fs::rename(&temp_path, path).map_err(|e| storage_error("replace store", e))?;
    fs::remove_file(journal_path).map_err(|e| storage_error("remove journal", e))?;
    Ok(())
}
fn checksum(store: &Store) -> Result<u64, DactylError> {
    let bytes = serde_json::to_vec(store).map_err(|e| storage_error("checksum", e))?;
    Ok(bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    }))
}
struct FileLock {
    path: PathBuf,
    _file: File,
}
impl FileLock {
    fn acquire(path: &Path, timeout: Duration) -> Result<Self, DactylError> {
        let started = Instant::now();
        loop {
            match FsOpenOptions::new().write(true).create_new(true).open(path) {
                Ok(file) => {
                    return Ok(Self {
                        path: path.to_owned(),
                        _file: file,
                    })
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::AlreadyExists
                        && started.elapsed() < timeout =>
                {
                    thread::sleep(Duration::from_millis(2))
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(adapter_error(
                        AdapterErrorKind::Timeout,
                        "local store lock timeout",
                    ))
                }
                Err(e) => return Err(storage_error("acquire store lock", e)),
            }
        }
    }
}
impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
