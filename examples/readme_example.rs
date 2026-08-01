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

fn main() -> Result<(), dactyl_db::DactylError> {
    for row in dactyl_db::read("select id, title, status from todos", &[], true)?.iter() {
        let id: i64 = row.get("id")?;
        let title: String = row.get("title")?;
        let status: String = row.get("status")?;
        println!("todo {id}: {title} [{status}]");
    }
    Ok(())
}
