//! Row projection returned by [`crate::read`].
//!
//! # Named-column projection contract (dactyl #25)
//!
//! Each [`Row`] owns its column names and cell values as JSON. Named lookup is
//! stable across SQLite and Neon because both adapters normalize cells into
//! this shape before returning.
//!
//! | Concern | Semantics |
//! |---|---|
//! | Integer | JSON number that fits `i64` (`get_int` / `get::<i64>`) |
//! | Real | JSON number (`get_real` / `get::<f64>`; integers are accepted as reals) |
//! | Boolean | JSON `true`/`false`, or integer `0`/`1` via `get_bool` (SQLite stores bools as integers) |
//! | Text | JSON string (`get_str` / `get_str_ref` / `get::<String>`) |
//! | JSON / text | Low-level cell via `get_json` / `get_json_ref`; text payloads stay strings until the caller parses them |
//! | SQL NULL | JSON `null`. Non-`Option` typed getters return [`DactylError::Conversion`]; `get::<Option<T>>` yields `None`. `is_null` / `get_json` surface null without converting. |
//! | Missing column | [`DactylError::ColumnNotFound`] |
//! | Duplicate aliases | Left-to-right **first match**. `select a as x, b as x` resolves `get("x")` to the first `x`. Positional indexes still reach later duplicates. |
//! | Conversion failure | [`DactylError::Conversion`] with the column key and a reason string |
//! | Ownership | `get`, `get_*` (except `*_ref`) return **owned** values independent of the row. `get_str_ref` / `get_json_ref` borrow from `&self` for the row lifetime. A `Row` outlives the adapter connection. |

use crate::error::DactylError;
use serde::{Deserialize, Serialize};

/// A collection of result rows.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Rows(pub Vec<Row>);

impl Rows {
    /// Borrow the rows as a slice.
    pub fn as_slice(&self) -> &[Row] {
        &self.0
    }

    /// Iterate over rows.
    pub fn iter(&self) -> std::slice::Iter<'_, Row> {
        self.0.iter()
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the result is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One result row. Carries the column names plus the per-cell JSON values.
///
/// The row **owns** both vectors. After `read` returns, the
/// short-lived adapter is dropped; callers may keep `Row` values indefinitely.
/// Borrowed accessors (`get_str_ref`, `get_json_ref`) are tied to `&self` only.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Row {
    /// Column names, in the order the adapter emitted them.
    ///
    /// Duplicate names are allowed (SQL aliases). Named getters resolve the
    /// **first** occurrence left-to-right; use a positional [`usize`] index to
    /// reach a later duplicate.
    pub columns: Vec<String>,
    /// Per-cell values, parallel to `columns`.
    pub values: Vec<serde_json::Value>,
}

/// A unified database parameter value.
#[derive(Debug, Clone, PartialEq)]
pub enum Parameter {
    Null,
    Bool(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl serde::Serialize for Parameter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Parameter::Null => serializer.serialize_unit(),
            Parameter::Bool(b) => serializer.serialize_bool(*b),
            Parameter::Integer(i) => serializer.serialize_i64(*i),
            Parameter::Real(f) => serializer.serialize_f64(*f),
            Parameter::Text(s) => serializer.serialize_str(s),
            Parameter::Blob(bytes) => serializer.serialize_bytes(bytes),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Parameter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ParameterVisitor;
        impl<'de> serde::de::Visitor<'de> for ParameterVisitor {
            type Value = Parameter;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a database parameter value")
            }
            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Null)
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Null)
            }
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Integer(v))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Integer(v as i64))
            }
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Real(v))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Text(v.to_string()))
            }
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Text(v))
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Blob(v.to_vec()))
            }
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Parameter::Blob(v))
            }
        }
        deserializer.deserialize_any(ParameterVisitor)
    }
}

impl From<i64> for Parameter {
    fn from(v: i64) -> Self {
        Parameter::Integer(v)
    }
}
impl From<i32> for Parameter {
    fn from(v: i32) -> Self {
        Parameter::Integer(v as i64)
    }
}
impl From<u32> for Parameter {
    fn from(v: u32) -> Self {
        Parameter::Integer(v as i64)
    }
}
impl From<usize> for Parameter {
    fn from(v: usize) -> Self {
        Parameter::Integer(v as i64)
    }
}
impl From<bool> for Parameter {
    fn from(v: bool) -> Self {
        Parameter::Bool(v)
    }
}
impl From<f64> for Parameter {
    fn from(v: f64) -> Self {
        Parameter::Real(v)
    }
}
impl From<String> for Parameter {
    fn from(v: String) -> Self {
        Parameter::Text(v)
    }
}
impl From<&str> for Parameter {
    fn from(v: &str) -> Self {
        Parameter::Text(v.to_string())
    }
}

impl From<Vec<u8>> for Parameter {
    fn from(v: Vec<u8>) -> Self {
        Parameter::Blob(v)
    }
}

impl From<Option<String>> for Parameter {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(s) => Parameter::Text(s),
            None => Parameter::Null,
        }
    }
}

impl From<Option<&str>> for Parameter {
    fn from(v: Option<&str>) -> Self {
        match v {
            Some(s) => Parameter::Text(s.to_string()),
            None => Parameter::Null,
        }
    }
}

impl From<Option<i64>> for Parameter {
    fn from(v: Option<i64>) -> Self {
        match v {
            Some(i) => Parameter::Integer(i),
            None => Parameter::Null,
        }
    }
}

impl From<Option<bool>> for Parameter {
    fn from(v: Option<bool>) -> Self {
        match v {
            Some(b) => Parameter::Bool(b),
            None => Parameter::Null,
        }
    }
}

/// Helper trait for row indexing by position or column name.
///
/// Named indexes (`&str`, `String`) resolve **left-to-right first match** when
/// a result set contains duplicate column aliases.
pub trait RowIndex: std::fmt::Debug {
    /// Return the resolved index in `row`, or `None` if the name is absent.
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl RowIndex for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl RowIndex for &str {
    fn idx(&self, row: &Row) -> Option<usize> {
        // First match wins for duplicate aliases (documented contract).
        row.columns.iter().position(|c| c == self)
    }
}

impl RowIndex for String {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c == self)
    }
}

impl Row {
    /// Strict typed extraction via `serde` into an **owned** `T`.
    ///
    /// - Missing column → [`DactylError::ColumnNotFound`].
    /// - SQL NULL into non-`Option` `T` → [`DactylError::Conversion`].
    /// - SQL NULL into `Option<T>` → `Ok(None)`.
    /// - Type mismatch → [`DactylError::Conversion`].
    /// - Duplicate aliases → first matching column (left-to-right).
    ///
    /// Prefer [`Self::get_bool`] for portable bools: SQLite stores booleans as
    /// integers `0`/`1`, which strict `get::<bool>` rejects. For lenient
    /// portable shapes use [`Self::get_bool`] / [`Self::get_int`] /
    /// [`Self::get_real`] / [`Self::get_str`] / [`Self::get_json`].
    pub fn get<I: RowIndex, T: serde::de::DeserializeOwned>(
        &self,
        index: I,
    ) -> Result<T, DactylError> {
        let i = self.idx(&index)?;
        let val = &self.values[i];
        serde_json::from_value(val.clone()).map_err(|e| {
            if val.is_null() {
                DactylError::Conversion(format!(
                    "column {:?} is NULL; use Option<T> with get for nullable columns",
                    index
                ))
            } else {
                DactylError::Conversion(format!(
                    "failed to convert column {:?} to target type: {}",
                    index, e
                ))
            }
        })
    }

    /// Alias for [`Self::get`]. Named for callers who prefer a `try_*` style.
    pub fn try_get<I: RowIndex, T: serde::de::DeserializeOwned>(
        &self,
        index: I,
    ) -> Result<T, DactylError> {
        self.get(index)
    }

    /// Whether the cell is SQL NULL (`serde_json::Value::Null`).
    ///
    /// Missing column → [`DactylError::ColumnNotFound`].
    pub fn is_null<I: RowIndex>(&self, index: I) -> Result<bool, DactylError> {
        let i = self.idx(&index)?;
        Ok(self.values[i].is_null())
    }

    /// Lenient **owned** `bool`: accepts JSON `true`/`false` or integer `0`/`1`.
    ///
    /// NULL → [`DactylError::Conversion`].
    pub fn get_bool<I: RowIndex>(&self, index: I) -> Result<bool, DactylError> {
        let i = self.idx(&index)?;
        match &self.values[i] {
            serde_json::Value::Null => Err(Self::null_err(&index)),
            serde_json::Value::Bool(b) => Ok(*b),
            serde_json::Value::Number(n) if n.as_i64() == Some(0) => Ok(false),
            serde_json::Value::Number(n) if n.as_i64() == Some(1) => Ok(true),
            other => Err(DactylError::Conversion(format!(
                "cannot read {other:?} as bool at column {:?}",
                index
            ))),
        }
    }

    /// Lenient **owned** `i64`: accepts JSON integers (not fractional reals).
    ///
    /// NULL → [`DactylError::Conversion`].
    pub fn get_int<I: RowIndex>(&self, index: I) -> Result<i64, DactylError> {
        let i = self.idx(&index)?;
        match &self.values[i] {
            serde_json::Value::Null => Err(Self::null_err(&index)),
            serde_json::Value::Number(n) => n.as_i64().ok_or_else(|| {
                DactylError::Conversion(format!("value is not i64 at column {:?}", index))
            }),
            other => Err(DactylError::Conversion(format!(
                "cannot read {other:?} as i64 at column {:?}",
                index
            ))),
        }
    }

    /// Lenient **owned** `f64`: accepts any JSON number (integers and reals).
    ///
    /// NULL → [`DactylError::Conversion`].
    pub fn get_real<I: RowIndex>(&self, index: I) -> Result<f64, DactylError> {
        let i = self.idx(&index)?;
        match &self.values[i] {
            serde_json::Value::Null => Err(Self::null_err(&index)),
            serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| {
                DactylError::Conversion(format!("value is not f64 at column {:?}", index))
            }),
            other => Err(DactylError::Conversion(format!(
                "cannot read {other:?} as f64 at column {:?}",
                index
            ))),
        }
    }

    /// Lenient **owned** `String`: accepts JSON string (clones the cell).
    ///
    /// NULL → [`DactylError::Conversion`]. Prefer [`Self::get_str_ref`] to borrow.
    pub fn get_str<I: RowIndex>(&self, index: I) -> Result<String, DactylError> {
        self.get_str_ref(index).map(str::to_owned)
    }

    /// Borrowed `&str` tied to the row lifetime. Accepts JSON string only.
    ///
    /// NULL → [`DactylError::Conversion`]. The reference is valid while `self` lives.
    pub fn get_str_ref<I: RowIndex>(&self, index: I) -> Result<&str, DactylError> {
        let i = self.idx(&index)?;
        match &self.values[i] {
            serde_json::Value::Null => Err(Self::null_err(&index)),
            serde_json::Value::String(s) => Ok(s.as_str()),
            other => Err(DactylError::Conversion(format!(
                "cannot read {other:?} as str at column {:?}",
                index
            ))),
        }
    }

    /// Owned clone of the raw JSON cell (including `Null`).
    pub fn get_json<I: RowIndex>(&self, index: I) -> Result<serde_json::Value, DactylError> {
        self.get_json_ref(index).cloned()
    }

    /// Borrowed raw JSON cell tied to the row lifetime (including `Null`).
    pub fn get_json_ref<I: RowIndex>(&self, index: I) -> Result<&serde_json::Value, DactylError> {
        let i = self.idx(&index)?;
        Ok(&self.values[i])
    }

    fn null_err<I: std::fmt::Debug>(index: &I) -> DactylError {
        DactylError::Conversion(format!(
            "column {:?} is NULL; use Option<T> with get for nullable columns",
            index
        ))
    }

    fn idx<I: RowIndex>(&self, index: &I) -> Result<usize, DactylError> {
        index
            .idx(self)
            .ok_or_else(|| DactylError::ColumnNotFound(format!("{:?}", index)))
            .and_then(|i| {
                if i < self.values.len() {
                    Ok(i)
                } else {
                    Err(DactylError::ColumnNotFound(format!(
                        "index {i} out of bounds"
                    )))
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_row() -> Row {
        Row {
            columns: vec![
                "id".into(),
                "flag".into(),
                "ratio".into(),
                "label".into(),
                "payload".into(),
                "nullable".into(),
                "name".into(), // first of duplicate alias pair
                "name".into(), // second alias — positional only via index
            ],
            values: vec![
                json!(42),
                json!(true),
                json!(1.5),
                json!("hello"),
                json!({"k": 1}),
                json!(null),
                json!("first"),
                json!("second"),
            ],
        }
    }

    #[test]
    fn scalar_matrix_strict_and_lenient() {
        let row = sample_row();

        assert_eq!(row.get::<_, i64>("id").unwrap(), 42);
        assert_eq!(row.try_get::<_, i64>("id").unwrap(), 42);
        assert_eq!(row.get_int("id").unwrap(), 42);
        assert_eq!(row.get_real("id").unwrap(), 42.0);

        assert!(row.get_bool("flag").unwrap());
        assert!(row.get::<_, bool>("flag").unwrap());

        assert_eq!(row.get::<_, f64>("ratio").unwrap(), 1.5);
        assert_eq!(row.get_real("ratio").unwrap(), 1.5);
        assert!(matches!(
            row.get_int("ratio"),
            Err(DactylError::Conversion(_))
        ));

        assert_eq!(row.get_str("label").unwrap(), "hello");
        assert_eq!(row.get_str_ref("label").unwrap(), "hello");
        assert_eq!(row.get::<_, String>("label").unwrap(), "hello");

        let payload = row.get_json("payload").unwrap();
        assert_eq!(payload, json!({"k": 1}));
        assert_eq!(row.get_json_ref("payload").unwrap(), &json!({"k": 1}));
        #[derive(Deserialize)]
        struct Payload {
            k: i64,
        }
        assert_eq!(row.get::<_, Payload>("payload").unwrap().k, 1);
    }

    #[test]
    fn sqlite_style_bool_as_integer() {
        let row = Row {
            columns: vec!["flag".into(), "off".into()],
            values: vec![json!(1), json!(0)],
        };
        assert!(row.get_bool("flag").unwrap());
        assert!(!row.get_bool("off").unwrap());
        // Strict serde bool rejects integer encoding.
        assert!(matches!(
            row.get::<_, bool>("flag"),
            Err(DactylError::Conversion(_))
        ));
    }

    #[test]
    fn null_semantics() {
        let row = sample_row();
        assert!(row.is_null("nullable").unwrap());
        assert!(!row.is_null("id").unwrap());
        assert!(row.get::<_, Option<i64>>("nullable").unwrap().is_none());
        assert!(row.get::<_, Option<String>>("nullable").unwrap().is_none());

        let null_errs: Vec<DactylError> = vec![
            row.get::<_, i64>("nullable").unwrap_err(),
            row.get_int("nullable").unwrap_err(),
            row.get_bool("nullable").unwrap_err(),
            row.get_real("nullable").unwrap_err(),
            row.get_str("nullable").unwrap_err(),
            row.get_str_ref("nullable").unwrap_err(),
        ];
        for err in null_errs {
            match err {
                DactylError::Conversion(msg) => {
                    assert!(msg.contains("NULL"), "expected NULL hint, got {msg}");
                }
                other => panic!("expected Conversion for NULL, got {other:?}"),
            }
        }

        assert_eq!(row.get_json("nullable").unwrap(), json!(null));
        assert!(row.get_json_ref("nullable").unwrap().is_null());
    }

    #[test]
    fn missing_column_is_column_not_found() {
        let row = sample_row();
        assert!(matches!(
            row.get::<_, i64>("nope"),
            Err(DactylError::ColumnNotFound(_))
        ));
        assert!(matches!(
            row.get_int("nope"),
            Err(DactylError::ColumnNotFound(_))
        ));
        assert!(matches!(
            row.is_null("nope"),
            Err(DactylError::ColumnNotFound(_))
        ));
        assert!(matches!(
            row.get_json_ref("nope"),
            Err(DactylError::ColumnNotFound(_))
        ));
        assert!(matches!(
            row.get::<_, i64>(99usize),
            Err(DactylError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn conversion_failures() {
        let row = sample_row();
        assert!(matches!(
            row.get::<_, bool>("label"),
            Err(DactylError::Conversion(_))
        ));
        assert!(matches!(
            row.get_int("label"),
            Err(DactylError::Conversion(_))
        ));
        assert!(matches!(row.get_str("id"), Err(DactylError::Conversion(_))));
        assert!(matches!(
            row.get_bool("ratio"),
            Err(DactylError::Conversion(_))
        ));
    }

    #[test]
    fn duplicate_alias_first_match_and_positional() {
        let row = sample_row();
        // Named lookup: left-to-right first match.
        assert_eq!(row.get_str("name").unwrap(), "first");
        assert_eq!(row.get_str_ref("name").unwrap(), "first");
        assert_eq!(row.get_int(0usize).unwrap(), 42);
        // Positional index reaches the second "name" column.
        let second_name_idx = row
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| *c == "name")
            .nth(1)
            .map(|(i, _)| i)
            .expect("second name");
        assert_eq!(row.get_str(second_name_idx).unwrap(), "second");
        assert_eq!(row.get_json_ref(second_name_idx).unwrap(), &json!("second"));
    }

    #[test]
    fn borrowed_values_tied_to_row_lifetime() {
        let row = sample_row();
        let s: &str = row.get_str_ref("label").unwrap();
        let j: &serde_json::Value = row.get_json_ref("payload").unwrap();
        // Still usable while `row` is in scope (compile-time lifetime proof
        // plus runtime equality).
        assert_eq!(s, "hello");
        assert_eq!(j["k"], 1);
        // Owned getters return independent values.
        let owned = row.get_str("label").unwrap();
        drop(row);
        assert_eq!(owned, "hello");
    }
}
