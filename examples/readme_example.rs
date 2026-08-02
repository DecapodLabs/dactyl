//! Tiny runnable example referenced from README.
//!
//! Run with:
//!
//! ```text
//! DATASTORE=sqlite DATASTORE_ROUTE=/tmp/dactyl-example.db \
//!   cargo run --features sqlite --example readme_example
//! ```
//!
//! The example creates a caller-owned table via `dactyl::execute`, inserts a
//! row with bound parameters, then reads it back with `dactyl::query`.
//! dactyl never silently bootstraps schema — the caller owns it.

fn main() -> Result<(), dactyl_db::DactylError> {
    dactyl_db::execute(
        "create table if not exists todos (id integer primary key, title text not null, status text not null)",
        &[],
    )?;
    dactyl_db::execute(
        "insert into todos (id, title, status) values ($1, $2, $3)",
        &[
            dactyl_db::Parameter::Integer(1),
            dactyl_db::Parameter::Text("ship dactyl 0.1.7".into()),
            dactyl_db::Parameter::Text("open".into()),
        ],
    )?;

    let sql = dactyl_db::query!("select id, title, status from todos");
    for row in dactyl_db::query(&sql, &[])?.iter() {
        let id: i64 = row.try_get("id")?;
        let title: String = row.get("title")?;
        let status: String = row.get_str("status")?;
        // Borrowed accessor is valid for the row lifetime.
        let title_ref: &str = row.get_str_ref("title")?;
        println!("todo {id}: {title} ({title_ref}) [{status}]");
    }
    Ok(())
}
