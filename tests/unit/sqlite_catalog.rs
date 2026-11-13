use super::CATALOG_TEST_PREPARES;
use crate::{Connection, DatastoreRoute};

#[test]
fn predicates_prepare_one_statement_instead_of_inspecting_every_table() {
    let db = Connection::open(DatastoreRoute::sqlite(":memory:")).unwrap();
    for n in 0..32 {
        db.write(
            &format!("CREATE TABLE fixture_{n}(id TEXT PRIMARY KEY, value TEXT)"),
            &[],
        )
        .unwrap();
    }
    CATALOG_TEST_PREPARES.with(|count| count.set(0));
    assert_eq!(db.inspect_schema().unwrap().tables.len(), 32);
    let legacy = CATALOG_TEST_PREPARES.with(|count| count.replace(0));
    assert!(legacy > 100);
    assert!(db.has_table("fixture_31").unwrap());
    assert_eq!(CATALOG_TEST_PREPARES.with(|count| count.replace(0)), 1);
    assert!(db.has_column("fixture_31", "value").unwrap());
    assert_eq!(CATALOG_TEST_PREPARES.with(|count| count.replace(0)), 1);
    assert!(!db.has_column("missing", "value").unwrap());
    assert_eq!(CATALOG_TEST_PREPARES.with(|count| count.replace(0)), 1);
    eprintln!("catalog statements: full={legacy}, each predicate=1");
}
