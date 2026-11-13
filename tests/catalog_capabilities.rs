#[cfg(feature = "neon")]
#[test]
fn neon_catalog_predicates_are_local_capability_errors() {
    use dactyl_db::{AdapterErrorKind, Connection, DatastoreRoute, MutationOutcome};

    // No listener is needed: unsupported metadata operations must not attempt
    // a remote read or silently inspect a different local authority.
    let db = Connection::open(DatastoreRoute::neon("http://127.0.0.1:9", None)).unwrap();
    for result in [db.has_table("tasks"), db.has_column("tasks", "id")] {
        let err = result.unwrap_err();
        assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Capability));
        assert_eq!(err.adapter_code(), Some("unsupported_schema_inspection"));
        assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
    }
}

#[cfg(feature = "supabase")]
#[test]
fn supabase_catalog_predicates_are_local_capability_errors() {
    use dactyl_db::{
        AdapterErrorKind, Connection, DatastoreRoute, MutationOutcome, StorageContext,
        SupabaseRpcConfig,
    };
    use serde_json::json;

    let ctx = StorageContext::new(1, json!({"scope": "catalog-capability"})).unwrap();
    let service = Connection::open_with_context(
        DatastoreRoute::supabase("http://127.0.0.1:9", Some("bearer".into())),
        Some(ctx.clone()),
    )
    .unwrap();
    let rpc = Connection::open_with_context(
        DatastoreRoute::supabase_rpc(
            SupabaseRpcConfig::v1("http://127.0.0.1:9", "sb_publishable_test_key"),
            Some("bearer".into()),
        ),
        Some(ctx),
    )
    .unwrap();

    for db in [&service, &rpc] {
        for result in [db.has_table("tasks"), db.has_column("tasks", "id")] {
            let err = result.unwrap_err();
            assert_eq!(err.adapter_kind(), Some(AdapterErrorKind::Capability));
            assert_eq!(err.adapter_code(), Some("unsupported_schema_inspection"));
            assert_eq!(err.mutation_outcome(), MutationOutcome::NotAttempted);
        }
    }
}
