//! Dialect detection for the lexical analyzer.
//!
//! The analyzer recognizes the explicit list of constructs the project ships
//! with and treats anything else as portable SQL. A full SQL parser is
//! intentionally out of scope; unsafe constructs fail closed instead of being
//! guessed into a rewrite.

/// SQL dialect an adapter speaks natively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Constructs supported by both shipped SQL adapters.
    Portable,
    /// Local file-backed SQLite.
    Sqlite,
    /// Remote Postgres via Neon HTTP (Propodus).
    Postgres,
}

/// A single dialect-specific construct the analyzer found in a query.
///
/// Anything not enumerated here is treated as portable SQL and never produces a
/// `Construct`. This keeps the dialect-mismatch check tight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Construct {
    // SQLite-only
    JsonEach,
    JsonTree,
    WithoutRowId,
    Strict,
    // Portable or Postgres-only
    JsonArrowText,
    JsonArrow,
    JsonContains,
    JsonContained,
    JsonExists,
    Jsonb,
    Returning,
    Ilike,
    GenRandomUuid,
    NowFn,
}

impl Construct {
    /// Return the lexeme that triggered detection.
    pub fn lexeme(self) -> &'static str {
        match self {
            Construct::JsonEach => "json_each",
            Construct::JsonTree => "json_tree",
            Construct::WithoutRowId => "without rowid",
            Construct::Strict => "strict",
            Construct::JsonArrowText => "->>",
            Construct::JsonArrow => "->",
            Construct::JsonContains => "@>",
            Construct::JsonContained => "<@",
            Construct::JsonExists => "?",
            Construct::Jsonb => "jsonb",
            Construct::Returning => "returning",
            Construct::Ilike => "ilike",
            Construct::GenRandomUuid => "gen_random_uuid",
            Construct::NowFn => "now",
        }
    }

    /// The dialect this construct belongs to.
    pub fn dialect(self) -> Dialect {
        match self {
            Construct::JsonEach
            | Construct::JsonTree
            | Construct::WithoutRowId
            | Construct::Strict => Dialect::Sqlite,
            Construct::JsonArrowText | Construct::JsonArrow | Construct::Returning => {
                Dialect::Portable
            }
            _ => Dialect::Postgres,
        }
    }

    /// Whether the dialect natively supports this construct.
    pub fn supported_by(self, dialect: Dialect) -> bool {
        self.dialect() == Dialect::Portable || self.dialect() == dialect
    }
}

/// Decide whether the construct list is portable enough for the active dialect.
///
/// Returns the first unsupported construct, or `None` if every construct is
/// supported (or the list is empty).
pub fn first_unsupported(constructs: &[Construct], dialect: Dialect) -> Option<Construct> {
    constructs
        .iter()
        .copied()
        .find(|c| !c.supported_by(dialect))
}
