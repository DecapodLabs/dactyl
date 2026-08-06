//! Query analysis and bounded dialect preparation.
//!
//! Dactyl uses a total lexical scanner rather than pretending to be a full SQL
//! parser. It rejects constructs that cannot be translated safely and applies
//! only explicit, semantics-bounded rewrites when the caller enables them.

mod dialect;
mod lexer;

use crate::error::DactylError;

pub use dialect::{first_unsupported, Construct, Dialect};

/// Outcome of analyzing a query without selecting an adapter.
#[derive(Debug, Clone)]
pub struct Analyzed {
    /// All dialect-specific constructs the lexer found, in source order.
    pub constructs: Vec<Construct>,
    /// Inline `-- dactyl: <datastore>` override, if any.
    pub inline_override: Option<&'static str>,
    /// A directive-stripping rewrite plan. Adapter-specific rewrites are
    /// selected by [`QueryAnalyzer::prepare`].
    pub rewrite: Rewrite,
}

/// A no-op or directive-stripping rewrite plan.
#[derive(Debug, Clone)]
pub enum Rewrite {
    /// Pass the query through unchanged.
    Identity,
    /// A rewritten query string.
    Replaced(String),
}

impl Rewrite {
    /// Apply the rewrite and return the SQL string to execute.
    pub fn apply(&self, original: &str) -> String {
        match self {
            Rewrite::Identity => original.to_string(),
            Rewrite::Replaced(s) => s.clone(),
        }
    }
}

/// The analyzer. Cheap to construct; carries no state.
#[derive(Debug, Default, Clone, Copy)]
pub struct QueryAnalyzer;

impl QueryAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Lex a SQL string and produce an [`Analyzed`] value.
    pub fn analyze(&self, query: &str) -> Analyzed {
        let (inline_override, remainder) = lexer::strip_dactyl_directive(query);
        let tokens = lexer::tokenize(&remainder);
        let constructs = detect_constructs(&tokens);
        let rewrite = if constructs.is_empty() && inline_override.is_none() {
            Rewrite::Identity
        } else {
            Rewrite::Replaced(remainder)
        };
        Analyzed {
            constructs,
            inline_override,
            rewrite,
        }
    }

    /// Analyze and prepare SQL for one concrete adapter dialect.
    ///
    /// Inline directives are validated against the connection's selected
    /// dialect. Unsupported constructs are rejected by default. When
    /// `allow_rewrites` is true, only the small set of loss-bounded rewrites
    /// below is applied; constructs with no safe translation still fail.
    pub fn prepare(
        &self,
        query: &str,
        dialect: Dialect,
        allow_rewrites: bool,
    ) -> Result<String, DactylError> {
        let analyzed = self.analyze(query);
        if let Some(override_ds) = analyzed.inline_override {
            let override_dialect = dialect_of(override_ds).ok_or_else(|| {
                DactylError::Routing(format!("unknown inline datastore {override_ds:?}"))
            })?;
            if override_dialect != dialect {
                return Err(DactylError::Routing(format!(
                    "inline datastore {override_ds:?} does not match the active connection"
                )));
            }
        }

        let unsupported = first_unsupported(&analyzed.constructs, dialect);
        let (_, remainder) = lexer::strip_dactyl_directive(query);
        if unsupported.is_none() {
            return Ok(remainder);
        }
        if !allow_rewrites {
            return Err(DactylError::Unsupported {
                construct: unsupported.expect("checked above"),
            });
        }

        rewrite_for_dialect(&remainder, &analyzed.constructs, dialect).ok_or_else(|| {
            DactylError::Unsupported {
                construct: unsupported.expect("checked above"),
            }
        })
    }
}

/// Walk the token stream and collect dialect-specific constructs.
fn detect_constructs(tokens: &[lexer::Token]) -> Vec<Construct> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if let lexer::Token::Word(w) = &tokens[i] {
            match w.as_str() {
                "json_each" => out.push(Construct::JsonEach),
                "json_tree" => out.push(Construct::JsonTree),
                "without" => {
                    if let Some(lexer::Token::Word(n)) = tokens.get(i + 1) {
                        if n == "rowid" {
                            out.push(Construct::WithoutRowId);
                            i += 1;
                        }
                    }
                }
                "strict" => out.push(Construct::Strict),
                "jsonb" => out.push(Construct::Jsonb),
                "returning" => out.push(Construct::Returning),
                "ilike" => out.push(Construct::Ilike),
                "gen_random_uuid" => out.push(Construct::GenRandomUuid),
                "now" => out.push(Construct::NowFn),
                _ => {}
            }
        } else if let lexer::Token::Op(o) = &tokens[i] {
            match o.as_str() {
                "->>" => out.push(Construct::JsonArrowText),
                "->" => out.push(Construct::JsonArrow),
                "@>" => out.push(Construct::JsonContains),
                "<@" => out.push(Construct::JsonContained),
                _ => {}
            }
        }
        // `?` is also a valid SQLite parameter placeholder. The lexical
        // scanner therefore never classifies it as JSON existence syntax.
        i += 1;
    }
    out
}

/// Apply only rewrites whose semantics are stable for the supported adapters.
/// Table-valued JSON functions, JSON containment, and UUID generation remain
/// explicit errors because a lexical substitution would silently change data.
fn rewrite_for_dialect(sql: &str, constructs: &[Construct], dialect: Dialect) -> Option<String> {
    let mut rewritten = sql.to_string();
    for &construct in constructs {
        match (dialect, construct) {
            (Dialect::Sqlite, Construct::Ilike) => {
                rewritten = replace_word(&rewritten, "ilike", "like");
            }
            (Dialect::Sqlite, Construct::NowFn) => {
                rewritten = replace_now_function(&rewritten);
            }
            (Dialect::Sqlite, Construct::Jsonb) => {
                rewritten = replace_word(&rewritten, "jsonb", "text");
            }
            (Dialect::Postgres, Construct::Strict) => {
                rewritten = replace_word(&rewritten, "strict", "");
            }
            (_, Construct::JsonEach)
            | (_, Construct::JsonTree)
            | (_, Construct::WithoutRowId)
            | (_, Construct::JsonContains)
            | (_, Construct::JsonContained)
            | (_, Construct::JsonExists)
            | (_, Construct::GenRandomUuid) => return None,
            (_, Construct::JsonArrowText | Construct::JsonArrow | Construct::Returning) => {
                if !construct.supported_by(dialect) {
                    return None;
                }
            }
            (_, Construct::Ilike | Construct::NowFn | Construct::Jsonb | Construct::Strict) => {
                if !construct.supported_by(dialect) {
                    return None;
                }
            }
        }
    }
    Some(rewritten)
}

fn replace_word(input: &str, needle: &str, replacement: &str) -> String {
    rewrite_unquoted(input, |segment| {
        replace_word_unquoted(segment, needle, replacement)
    })
}

fn replace_word_unquoted(input: &str, needle: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let lower = input.to_ascii_lowercase();
    let bytes = input.as_bytes();
    let needle_bytes = needle.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let end = i + needle_bytes.len();
        let boundary_before =
            i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
        let boundary_after =
            end >= bytes.len() || (!bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_');
        if end <= bytes.len()
            && boundary_before
            && boundary_after
            && &lower.as_bytes()[i..end] == needle_bytes
        {
            out.push_str(replacement);
            i = end;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn replace_now_function(input: &str) -> String {
    rewrite_unquoted(input, replace_now_function_unquoted)
}

fn replace_now_function_unquoted(input: &str) -> String {
    let bytes = input.as_bytes();
    let lower = input.to_ascii_lowercase();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if lower.as_bytes()[i..].starts_with(b"now")
            && (i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_'))
            && (i + 3 == bytes.len()
                || (!bytes[i + 3].is_ascii_alphanumeric() && bytes[i + 3] != b'_'))
        {
            let mut j = i + 3;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j + 1 < bytes.len() && bytes[j] == b'(' && bytes[j + 1] == b')' {
                out.push_str("CURRENT_TIMESTAMP");
                i = j + 2;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Apply a transformation only to SQL code, preserving string literals and
/// comments byte-for-byte. This keeps dialect rewrites from changing data or
/// documentation embedded in the query.
fn rewrite_unquoted<F>(input: &str, mut transform: F) -> String
where
    F: FnMut(&str) -> String,
{
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut segment_start = 0;
    let mut i = 0;
    while i < bytes.len() {
        let quote = if bytes[i] == b'\'' || bytes[i] == b'"' {
            Some(bytes[i])
        } else {
            None
        };
        let comment = (bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-')
            || (bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*');
        if quote.is_none() && !comment {
            i += 1;
            continue;
        }

        out.push_str(&transform(&input[segment_start..i]));
        let protected_start = i;
        if let Some(delimiter) = quote {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == delimiter {
                    if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
        } else if bytes[i] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            } else {
                i = bytes.len();
            }
        }
        out.push_str(&input[protected_start..i]);
        segment_start = i;
    }
    out.push_str(&transform(&input[segment_start..]));
    out
}

/// Map a datastore name to its native dialect.
pub fn dialect_of(datastore: &str) -> Option<Dialect> {
    match datastore {
        "sqlite" => Some(Dialect::Sqlite),
        "neon" | "postgres" | "postgresql" | "pg" => Some(Dialect::Postgres),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_strips_directive_and_rejects_unsafe_mismatch() {
        let analyzer = QueryAnalyzer::new();
        let sql = analyzer
            .prepare("-- dactyl: sqlite\nselect ?1", Dialect::Sqlite, false)
            .unwrap();
        assert_eq!(sql, "select ?1");
        assert!(matches!(
            analyzer.prepare("select data @> $1", Dialect::Sqlite, true),
            Err(DactylError::Unsupported { .. })
        ));
    }

    #[test]
    fn safe_rewrites_are_explicit() {
        let analyzer = QueryAnalyzer::new();
        let sql = analyzer
            .prepare(
                "select now() where name ilike $1 and note = 'now() ilike'",
                Dialect::Sqlite,
                true,
            )
            .unwrap();
        assert!(sql.contains("CURRENT_TIMESTAMP"));
        assert!(sql.contains("like"));
        assert!(sql.contains("'now() ilike'"));
    }

    #[test]
    fn question_placeholders_are_not_json_operators() {
        let analyzed = QueryAnalyzer::new().analyze("select ?1, ? from values");
        assert!(analyzed.constructs.is_empty());
    }
}
