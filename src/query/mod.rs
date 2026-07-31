//! Query analysis pipeline.
//!
//! [`QueryAnalyzer`] is a tiny lexical scanner that:
//!   1. Detects the `-- dactyl: <datastore>` inline directive.
//!   2. Detects a fixed list of dialect-specific constructs.
//!   3. Returns [`Analyzed`] so the caller (lib.rs / the `query!` macro) can
//!      decide whether to rewrite, error, or pass through.
//!
//! The rewriter itself is intentionally a no-op for the first pass — see the
//! follow-up issue for the full plan. The pipeline is wired so swapping in the
//! real rewriter is a one-function change.

mod dialect;
mod lexer;

pub use dialect::{first_unsupported, Construct, Dialect};

/// Outcome of analyzing a single query.
#[derive(Debug, Clone)]
pub struct Analyzed {
    /// All dialect-specific constructs the lexer found, in source order.
    pub constructs: Vec<Construct>,
    /// Inline `-- dactyl: <datastore>` override, if any.
    pub inline_override: Option<&'static str>,
    /// A rewriter plan. The first pass emits identity-only rewrites; callers
    /// that enable `optimize = true` can apply it transparently.
    pub rewrite: Rewrite,
}

/// A no-op or trivial rewrite plan.
///
/// The rewriter is a follow-up; for the first pass, the only non-identity
/// action is dropping whitespace. Future passes will fill in the structured
/// transformations.
#[derive(Debug, Clone)]
pub enum Rewrite {
    /// Pass the query through unchanged.
    Identity,
    /// A rewritten query string. Identity in this pass.
    Replaced(String),
}

impl Rewrite {
    /// Apply the rewrite and return the SQL string to execute.
    ///
    /// We always return an owned `String` to avoid lifetime gymnastics in the
    /// caller — the analyzer already materializes the rewritten string.
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
    /// Create a new analyzer.
    pub fn new() -> Self {
        Self
    }

    /// Lex a SQL string and produce an [`Analyzed`] value.
    ///
    /// The analyzer never raises an error: invalid SQL is treated as portable
    /// SQL and produces an empty `constructs` list. Errors are produced
    /// downstream when the adapter fails to parse or execute the string.
    pub fn analyze(&self, query: &str) -> Analyzed {
        let (inline_override, remainder) = lexer::strip_dactyl_directive(query);
        let tokens = lexer::tokenize(&remainder);
        let constructs = detect_constructs(&tokens);
        let rewrite = if constructs.is_empty() {
            Rewrite::Identity
        } else {
            // first-pass: identity rewrite. The plumbing is here so callers
            // can already exercise the optimize=true code path.
            Rewrite::Replaced(remainder)
        };
        Analyzed {
            constructs,
            inline_override,
            rewrite,
        }
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
                    // WITHOUT ROWID is two tokens
                    if i + 1 < tokens.len() {
                        if let lexer::Token::Word(n) = &tokens[i + 1] {
                            if n == "rowid" {
                                out.push(Construct::WithoutRowId);
                                i += 1;
                            }
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
        } else if matches!(tokens[i], lexer::Token::Question) {
            // `?` is the JSON existence operator in postgres JSONB context.
            // The lexer is context-free; we conservatively flag every bare `?`
            // so callers can decide.
            out.push(Construct::JsonExists);
        }
        i += 1;
    }
    out
}

/// Map a datastore name to its native dialect. Returns `None` for unknown
/// names; callers should treat that as `DactylError::UnknownDatastore`.
pub fn dialect_of(datastore: &str) -> Option<Dialect> {
    match datastore {
        "sqlite" => Some(Dialect::Sqlite),
        "neon" | "postgres" | "postgresql" | "pg" => Some(Dialect::Postgres),
        _ => None,
    }
}
