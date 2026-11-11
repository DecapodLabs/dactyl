//! Per-client TLS configuration for the existing HTTPS transports.
//!
//! Preserve reqwest's ring provider, WebPKI roots, SNI and safe TLS versions
//! without resolving its unused HTTP/3 graph or installing a global provider.

use std::sync::Arc;

use crate::DactylError;

fn configuration() -> Result<rustls::ClientConfig, DactylError> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| DactylError::Config("the default HTTPS protocol profile is unavailable".into()))?;
    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

pub(crate) fn client_builder() -> Result<reqwest::blocking::ClientBuilder, DactylError> {
    Ok(reqwest::blocking::Client::builder().use_preconfigured_tls(configuration()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clients_use_private_tls_even_with_an_unusable_ambient_provider() {
        const CHILD: &str = "DACTYL_PRIVATE_TLS_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "http_tls::tests::clients_use_private_tls_even_with_an_unusable_ambient_provider",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        // A fresh client must require no process-global provider setup and
        // must not install one as a side effect of client construction.
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
        let client = client_builder().unwrap().build().unwrap();
        drop(client);
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());

        // An unrelated application's global provider cannot change this
        // connector's TLS policy. Isolate this one-shot global in a child.
        let mut ambient = rustls::crypto::ring::default_provider();
        ambient.cipher_suites.clear();
        assert!(ambient.install_default().is_ok());
        let config = configuration().unwrap();
        assert!(config.enable_sni);
        let versions: Vec<_> = config.crypto_provider().cipher_suites.iter()
            .map(|suite| suite.version().version).collect();
        assert!(versions.contains(&rustls::ProtocolVersion::TLSv1_2));
        assert!(versions.contains(&rustls::ProtocolVersion::TLSv1_3));
        assert!(versions.iter().all(|version| matches!(version,
            rustls::ProtocolVersion::TLSv1_2 | rustls::ProtocolVersion::TLSv1_3)));
        assert!(!webpki_roots::TLS_SERVER_ROOTS.is_empty());
        let client = client_builder().unwrap().build().unwrap();
        drop(client);
        assert!(rustls::crypto::CryptoProvider::get_default().unwrap().cipher_suites.is_empty());
    }
}
