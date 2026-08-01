//! Row projection returned by [`crate::read`] and [`crate::write`].

use crate::error::DactylError;
use serde::{Deserialize, Serialize};

/// A collection of result rows.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// One result row. Carries the column names (shared across the result) plus
/// the per-cell JSON values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Column names, in the order the adapter emitted them.
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
pub trait RowIndex: std::fmt::Debug {
    /// Return index in row.
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl RowIndex for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl RowIndex for &str {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c == self)
    }
}

impl RowIndex for String {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c == self)
    }
}

impl Row {
    /// Extract named column value type-safely.
    pub fn get<I: RowIndex, T: serde::de::DeserializeOwned>(
        &self,
        index: I,
    ) -> Result<T, DactylError> {
        let idx = index
            .idx(self)
            .ok_or_else(|| DactylError::ColumnNotFound(format!("{:?}", index)))?;
        if idx >= self.values.len() {
            return Err(DactylError::ColumnNotFound(format!(
                "index {} out of bounds",
                idx
            )));
        }
        let val = &self.values[idx];
        serde_json::from_value(val.clone()).map_err(|e| {
            DactylError::Conversion(format!(
                "failed to convert column {:?} to target type: {}",
                index, e
            ))
        })
    }
}
