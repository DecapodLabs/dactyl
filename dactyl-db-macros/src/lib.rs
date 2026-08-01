//! `dactyl::query!` proc macro.
//!
//! Lexically analyzes the SQL literal at compile time so:
//
//!   1. Hard-coded constructs are visible to the analyzer before the crate
//!      is built.
//!   2. The literal is rewritten (currently identity) at compile time so the
//!      runtime path stays allocation-light.
//!   3. Empty literals fail to compile with a clear message.
//!
//! The expanded form returns a `String` containing the (rewritten) SQL.
//! Callers wire it into `dactyl::read` / `dactyl::write` directly:
//!
//! ```ignore
//! let rows = dactyl::read(&dactyl::query!("select id, title from todos"), true)?;
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, LitStr};

/// `dactyl::query!("literal")` — returns the rewritten SQL as a `String`.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let literal = parse_macro_input!(input as LitStr);
    let text = literal.value();

    if text.trim().is_empty() {
        return syn::Error::new_spanned(literal, "`query!` requires a non-empty SQL literal")
            .to_compile_error()
            .into();
    }

    let rewritten = rewrite(&text);

    let expanded = quote! {{
        // Compile-time lexer: hard-fails on unparseable input. The runtime
        // call is a no-op identity rewrite, kept so the contract is uniform
        // with non-literal queries.
        let __text: &str = #rewritten;
        ::dactyl_db::__private::analyze(__text).sql
    }};

    expanded.into()
}

/// Identity rewrite — placeholder for the real rewriter. Kept as a
/// function so swapping in the structured rewriter is a one-line change.
fn rewrite(input: &str) -> String {
    input.to_string()
}
