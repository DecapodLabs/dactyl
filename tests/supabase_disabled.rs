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
