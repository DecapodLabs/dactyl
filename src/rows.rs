//! Row projection returned by [`crate::read`] and [`crate::write`].

use serde::Serialize;

/// A collection of result rows.
#[derive(Debug, Clone, Default, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
pub struct Row {
    /// Column names, in the order the adapter emitted them.
    pub columns: Vec<String>,
    /// Per-cell values, parallel to `columns`.
    pub values: Vec<serde_json::Value>,
}

impl Row {
    /// Look up a cell by column name.
    pub fn get(&self, column: &str) -> Option<&serde_json::Value> {
        let idx = self.columns.iter().position(|c| c == column)?;
        self.values.get(idx)
    }
}
