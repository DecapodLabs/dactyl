//! Explicit native Supabase RPC configuration; authentication remains caller-owned.

pub const SUPABASE_RPC_CONTRACT_VERSION: u16 = 1;

/// Project configuration is separate from the current user's bearer token.
/// Validation is performed when the route is validated/opened, without network I/O.
#[derive(Clone, PartialEq, Eq)]
pub struct SupabaseRpcConfig {
    project_origin: String,
    publishable_key: String,
    schema: String,
}
impl SupabaseRpcConfig {
    /// Select version 1 and the default `dactyl_api` API schema.
    pub fn v1(origin: impl Into<String>, publishable_key: impl Into<String>) -> Self {
        Self {
            project_origin: origin.into(),
            publishable_key: publishable_key.into(),
            schema: "dactyl_api".into(),
        }
    }
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }
    pub fn project_origin(&self) -> &str {
        &self.project_origin
    }
    /// Explicit access for the credential owner; never log this value.
    pub fn publishable_key(&self) -> &str {
        &self.publishable_key
    }
    pub fn schema(&self) -> &str {
        &self.schema
    }
    pub fn contract_version(&self) -> u16 {
        SUPABASE_RPC_CONTRACT_VERSION
    }
}
impl std::fmt::Debug for SupabaseRpcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupabaseRpcConfig")
            .field("project_origin", &"[redacted]")
            .field("publishable_key", &"[redacted]")
            .field("schema", &"[redacted]")
            .field("contract_version", &SUPABASE_RPC_CONTRACT_VERSION)
            .finish()
    }
}

