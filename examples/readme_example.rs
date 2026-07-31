//! Tiny runnable example referenced from README.
//!
//! Boots dactyl against an in-tempdir SQLite file and runs a universal query
//! through the `read` facade. Run with:
//!
//! ```text
//! cargo run --features sqlite --example readme_example
//! ```

use dactyl::{DactylConfig, DactylError, Row};

fn main() -> Result<(), DactylError> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db = tmp.path().join("todos.db");
    dactyl::init(DactylConfig::sqlite(db.to_str().unwrap()))?;

    for row in dactyl::read("sqlite", "select id, title from todos", true)?.iter() {
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
