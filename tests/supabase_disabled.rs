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
        config.clone(),
        Some("fixture".into()),
    ))
    .err()
    .unwrap();
    assert!(error.to_string().contains("enable the `supabase` feature"));

    for failed in [
        dactyl_db::LogicalConnector::cloud(),
        dactyl_db::LogicalConnector::cloud_with_origin("https://project.example"),
        dactyl_db::LogicalConnector::cloud_with_config(config),
    ] {
        let err = failed.err().unwrap();
        assert!(matches!(err, dactyl_db::DactylError::Config(_)));
        assert!(err.to_string().contains("enable the `supabase` feature"));
    }
    assert!(matches!(
        dactyl_db::AuthenticationSecrets::generate(),
        Err(dactyl_db::DactylError::Config(_))
    ));
}
