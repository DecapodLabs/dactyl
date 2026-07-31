//! Tiny runnable example referenced from README.
//!
//! Run with:
//!
//! ```text
//! cargo run --features sqlite --example readme_example
//! ```
//!
//! Boots dactyl against `.decapod/data/todos.db` (auto-derived from the
//! `from todos` clause in the query) and prints the rows.

use dactyl::Row;

fn main() -> Result<(), dactyl::DactylError> {
    for row in dactyl::read("select id, title, status from todos", true)?.iter() {
        print_row(row);
    }
    Ok(())
}

fn print_row(r: &Row) {
    for (c, v) in r.columns.iter().zip(r.values.iter()) {
        println!("{c} = {v}");
    }
    println!("---");
}
