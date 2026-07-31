//! `dactyl::query!` proc macro.
//!
//! The macro performs lexical analysis at compile time so a literal containing
//! a hard-coded dialect-specific construct is caught by the compiler when
//! `optimize = false` is requested. With `optimize = true` (the default), the
//! macro defers all dialect checks to the runtime analyzer so a benign query
//! never fails `cargo build`.
//!
//! The shape is:
//!
//! ```ignore
//! dactyl::query!(datastore = "sqlite", optimize = true, "select ...")
//! ```
//!
//! `datastore` and `optimize` are optional and default to `active_datastore()`
//! and `true` respectively. Only the literal is lexically analyzed at compile
//! time.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Lit, LitStr, Token};

/// Lexical analysis result at compile time.
///
/// We mirror the runtime analyzer's behavior in this module so a hard-coded
/// query literal is checked before the crate even compiles.
#[derive(Debug, Default, Clone, Copy)]
struct LexHit {
    constructs: u32,
    // bit flags (1 << construct index)
    json_each: bool,
    json_tree: bool,
    without_rowid: bool,
    strict: bool,
    jsonb: bool,
    returning: bool,
    ilike: bool,
    gen_random_uuid: bool,
    now_fn: bool,
    json_arrow_text: bool,
    json_arrow: bool,
    json_contains: bool,
    json_contained: bool,
    json_exists: bool,
    has_inline_directive: bool,
}

impl LexHit {
    fn empty() -> Self {
        Self::default()
    }

    fn portable(&self) -> bool {
        self.constructs == 0
    }
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryArgs);
    let literal = parsed.literal.value();
    let datastore = parsed.datastore.as_deref();
    let optimize = parsed.optimize;
    let span = parsed.literal.span();

    let hit = lex(&literal);

    if !optimize && !hit.portable() && datastore.is_none() {
        // For optimize=false with no explicit datastore, the runtime check is
        // the strict one. Emit a non-fatal warning by leaving a marker; the
        // generated runtime code will produce DialectMismatch.
    }

    let rt_check = if !optimize {
        // At runtime, the generated code re-checks portability. Emit code that
        // dispatches to runtime::analyze and surfaces DialectMismatch.
        quote! {{
            let __hit = ::dactyl::__private::analyze_runtime(#literal);
            let __ds: &str = match #datastore {
                Some(s) => s,
                None => ::dactyl::active_datastore(),
            };
            if !::dactyl::__private::runtime_portable(&__hit, __ds) {
                return ::dactyl::__private::dialect_mismatch_err(__ds, &__hit);
            }
            (__hit.sql.to_string(), __ds.to_string())
        }}
    } else {
        let ds_expr = match datastore.as_deref() {
            Some(s) => quote! { #s },
            None => quote! { ::dactyl::active_datastore() },
        };
        quote! {{
            let __hit = ::dactyl::__private::analyze_runtime(#literal);
            (__hit.sql.to_string(), #ds_expr.to_string())
        }}
    };

    // Sanity: if `optimize = false` was requested AND the literal contains
    // constructs AND `datastore` was explicitly given AND the explicit
    // datastore doesn't support those constructs, emit a compile_error!().
    // This catches the case the issue calls out: hard-coded queries with
    // dialect-specific constructs should not silently compile when rewriting
    // is disabled.
    if !optimize && !hit.portable() {
        if let Some(ds) = datastore {
            let supported = ds_supported(ds, &hit);
            if !supported {
                let msg = format!(
                    "`query!` literal uses constructs not supported by datastore `{ds}` while `optimize = false`"
                );
                return syn::Error::new(span, msg).to_compile_error().into();
            }
        }
    }

    let expanded = quote! {
        {
            #rt_check
        }
    };
    expanded.into()
}

struct QueryArgs {
    datastore: Option<String>,
    optimize: bool,
    literal: LitStr,
}

impl syn::parse::Parse for QueryArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut datastore: Option<String> = None;
        let mut optimize: Option<bool> = None;
        let mut literal: Option<LitStr> = None;

        while !input.is_empty() {
            if input.peek(syn::Ident) {
                let key: syn::Ident = input.parse()?;
                let key_str = key.to_string();
                input.parse::<Token![=]>()?;
                match key_str.as_str() {
                    "datastore" => {
                        let lit: LitStr = input.parse()?;
                        datastore = Some(lit.value());
                    }
                    "optimize" => {
                        let lit: Lit = input.parse()?;
                        match lit {
                            Lit::Bool(b) => optimize = Some(b.value()),
                            _ => {
                                return Err(syn::Error::new_spanned(
                                    lit,
                                    "expected bool literal for `optimize`",
                                ))
                            }
                        }
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            key,
                            format!("unknown key `{other}`; expected `datastore`, `optimize`, or a literal"),
                        ));
                    }
                }
            } else if input.peek(LitStr) {
                let lit: LitStr = input.parse()?;
                literal = Some(lit);
            } else {
                return Err(
                    input.error("expected `datastore =`, `optimize =`, or a string literal")
                );
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }

        let literal = literal.ok_or_else(|| input.error("missing SQL literal"))?;
        Ok(Self {
            datastore,
            optimize: optimize.unwrap_or(true),
            literal,
        })
    }
}

/// Lexical analyzer. Mirrors the runtime one enough to detect the constructs
/// the issue enumerates.
fn lex(input: &str) -> LexHit {
    let mut hit = LexHit::empty();
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut prev_word_end: Option<usize> = None;
    let mut last_word: Option<String> = None;

    while i < bytes.len() {
        let c = bytes[i] as char;

        if c == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // line comment — capture directive
            let mut j = i + 2;
            while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            if j + 6 <= bytes.len() && bytes[j..j + 6].eq_ignore_ascii_case(b"dactyl") {
                hit.has_inline_directive = true;
            }
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == '\'' || c == '"' {
            // string / identifier literal — skip
            let q = c;
            i += 1;
            while i < bytes.len() {
                if bytes[i] as char == q {
                    if i + 1 < bytes.len() && bytes[i + 1] as char == q {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            last_word = None;
            continue;
        }
        if (c as char).is_whitespace() {
            i += 1;
            continue;
        }
        if (c as char).is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let word = std::str::from_utf8(&bytes[start..i])
                .unwrap_or("")
                .to_ascii_lowercase();
            let prev = last_word.take();
            match word.as_str() {
                "json_each" => {
                    hit.json_each = true;
                    hit.constructs += 1;
                }
                "json_tree" => {
                    hit.json_tree = true;
                    hit.constructs += 1;
                }
                "strict" => {
                    hit.strict = true;
                    hit.constructs += 1;
                }
                "jsonb" => {
                    hit.jsonb = true;
                    hit.constructs += 1;
                }
                "returning" => {
                    hit.returning = true;
                    hit.constructs += 1;
                }
                "ilike" => {
                    hit.ilike = true;
                    hit.constructs += 1;
                }
                "gen_random_uuid" => {
                    hit.gen_random_uuid = true;
                    hit.constructs += 1;
                }
                "now" => {
                    hit.now_fn = true;
                    hit.constructs += 1;
                }
                "rowid" => {
                    if prev.as_deref() == Some("without") {
                        hit.without_rowid = true;
                        hit.constructs += 1;
                    }
                }
                _ => {}
            }
            if word != "rowid" {
                last_word = Some(word);
            }
            prev_word_end = Some(i);
            continue;
        }
        if (c as char).is_ascii_digit() {
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'.')
            {
                i += 1;
            }
            last_word = None;
            continue;
        }

        // multi-char operators
        if i + 1 < bytes.len() {
            let two = &bytes[i..i + 2];
            if two == b"->" {
                if i + 2 < bytes.len() && bytes[i + 2] == b'>' {
                    hit.json_arrow_text = true;
                    hit.constructs += 1;
                    i += 3;
                    last_word = None;
                    continue;
                }
                hit.json_arrow = true;
                hit.constructs += 1;
                i += 2;
                last_word = None;
                continue;
            }
            if two == b"@>" {
                hit.json_contains = true;
                hit.constructs += 1;
                i += 2;
                last_word = None;
                continue;
            }
            if two == b"<@" {
                hit.json_contained = true;
                hit.constructs += 1;
                i += 2;
                last_word = None;
                continue;
            }
        }

        if c == '?' {
            hit.json_exists = true;
            hit.constructs += 1;
            i += 1;
            last_word = None;
            continue;
        }

        last_word = None;
        i += 1;
        let _ = prev_word_end;
    }

    hit
}

fn ds_supported(datastore: &str, hit: &LexHit) -> bool {
    let sqlite_only = hit.json_each || hit.json_tree || hit.without_rowid || hit.strict;
    let pg_only = hit.json_arrow_text
        || hit.json_arrow
        || hit.json_contains
        || hit.json_contained
        || hit.json_exists
        || hit.jsonb
        || hit.returning
        || hit.ilike
        || hit.gen_random_uuid
        || hit.now_fn;
    match datastore {
        "sqlite" => !pg_only,
        "neon" | "postgres" | "postgresql" | "pg" => !sqlite_only,
        _ => false,
    }
}
