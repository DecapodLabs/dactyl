#![cfg(not(feature = "supabase"))]

#[test]
fn supabase_disabled_feature_fails_without_fallback() {
    let route =
        dactyl_db::DatastoreRoute::supabase("https://service.example", Some("secret".into()));
    let error = dactyl_db::Connection::open(route).err().unwrap();
    assert!(matches!(error, dactyl_db::DactylError::Config(_)));
    assert!(error.to_string().contains("enable the `supabase` feature"));
    assert!(!error.to_string().contains("secret"));
}

#[test]
fn rpc_disabled_feature_fails_without_fallback() {
    let config =
        dactyl_db::SupabaseRpcConfig::v1("https://project.example", "sb_publishable_fixture");
    let error = dactyl_db::Connection::open(dactyl_db::DatastoreRoute::supabase_rpc(
        config,
        Some("fixture".into()),
    ))
    .err()
    .unwrap();
    assert!(error.to_string().contains("enable the `supabase` feature"));
}

#[test]
fn logical_cloud_disabled_and_local_remains_independent() {
    let error = dactyl_db::DatastoreRoute::for_backend(dactyl_db::Backend::Cloud,"https://project.example",None).unwrap_err();
    assert!(error.to_string().contains("enable the `supabase` feature"));
    let local=dactyl_db::DatastoreRoute::for_backend(dactyl_db::Backend::Local,":memory:",None).unwrap();
    assert_eq!(local.datastore(),dactyl_db::Datastore::Sqlite);
}

