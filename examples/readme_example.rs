//! Tiny application read/write example.
//!
//! The `app_events` table is assumed to be owned and created by the backend.
//! Dactyl only writes application data and reads it back.

fn main() -> Result<(), dactyl_db::DactylError> {
    dactyl_db::write(
        "insert into app_events (name) values ($1)",
        &[dactyl_db::Parameter::Text("opened".into())],
    )?;

    let rows = dactyl_db::read("select name from app_events order by id", &[])?;
    for row in rows.iter() {
        println!("{}", row.get_str("name")?);
    }
    Ok(())
}
