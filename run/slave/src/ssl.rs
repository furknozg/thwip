use proxy_common::{Server, SslCipher, SslProtocol};
use rustls::{crypto::ring::cipher_suite, server::ServerConfig, version, SupportedCipherSuite};
use std::{
    fs::File,
    io::{self, BufReader},
    sync::Arc,
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct LoadedSslConfig {
    pub server_config: Arc<ServerConfig>,
    pub handshake_timeout: Duration,
}

/// Loads the SSL/TLS configuration for every virtual server before workers
/// bind their listeners. `None` preserves plaintext behavior for a server.
pub fn load_ssl_configs(servers: &[Server]) -> io::Result<Vec<Option<LoadedSslConfig>>> {
    servers
        .iter()
        .map(|server| server.ssl.as_ref().map(load_ssl_config).transpose())
        .collect()
}

fn load_ssl_config(config: &proxy_common::SslServerConfig) -> io::Result<LoadedSslConfig> {
    let certificates = load_certificates(&config.certificate_path)?;
    let private_key = load_private_key(&config.private_key_path)?;
    let versions = config
        .protocols
        .iter()
        .map(supported_protocol)
        .collect::<Vec<_>>();
    let mut provider = rustls::crypto::ring::default_provider();
    provider.cipher_suites = config.ciphers.iter().map(supported_cipher).collect();

    ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&versions)
        .map_err(ssl_config_error)?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map(|server_config| LoadedSslConfig {
            server_config: Arc::new(server_config),
            handshake_timeout: Duration::from_millis(config.handshake_timeout_ms),
        })
        .map_err(ssl_config_error)
}

fn load_certificates(path: &str) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to open SSL certificate {path:?}: {error}"),
        )
    })?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse SSL certificate {path:?}: {error}"),
            )
        })?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SSL certificate {path:?} contains no certificates"),
        ));
    }
    Ok(certificates)
}

fn load_private_key(path: &str) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to open SSL private key {path:?}: {error}"),
        )
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse SSL private key {path:?}: {error}"),
            )
        })?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SSL private key {path:?} contains no private key"),
            )
        })
}

fn supported_protocol(protocol: &SslProtocol) -> &'static rustls::SupportedProtocolVersion {
    match protocol {
        SslProtocol::Tlsv1_2 => &version::TLS12,
        SslProtocol::Tlsv1_3 => &version::TLS13,
    }
}

fn supported_cipher(cipher: &SslCipher) -> SupportedCipherSuite {
    match cipher {
        SslCipher::Tls13Aes256GcmSha384 => cipher_suite::TLS13_AES_256_GCM_SHA384,
        SslCipher::Tls13Aes128GcmSha256 => cipher_suite::TLS13_AES_128_GCM_SHA256,
        SslCipher::Tls13Chacha20Poly1305Sha256 => cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        SslCipher::TlsEcdheEcdsaWithAes256GcmSha384 => {
            cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
        }
        SslCipher::TlsEcdheEcdsaWithAes128GcmSha256 => {
            cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
        }
        SslCipher::TlsEcdheEcdsaWithChacha20Poly1305Sha256 => {
            cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
        }
        SslCipher::TlsEcdheRsaWithAes256GcmSha384 => {
            cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        }
        SslCipher::TlsEcdheRsaWithAes128GcmSha256 => {
            cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
        }
        SslCipher::TlsEcdheRsaWithChacha20Poly1305Sha256 => {
            cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
        }
    }
}

fn ssl_config_error(error: rustls::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid SSL configuration: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::load_ssl_configs;
    use proxy_common::{Server, SslServerConfig};
    use std::net::SocketAddr;

    #[test]
    fn ssl_startup_rejects_an_unreadable_certificate() {
        let server = Server {
            server_name: None,
            locations: Vec::new(),
            listen: "127.0.0.1:443".parse::<SocketAddr>().unwrap(),
            ssl: Some(SslServerConfig {
                certificate_path: "/definitely/not/a/certificate.pem".to_owned(),
                private_key_path: "/definitely/not/a/private-key.pem".to_owned(),
                handshake_timeout_ms: 10_000,
                protocols: vec![proxy_common::SslProtocol::Tlsv1_3],
                ciphers: vec![proxy_common::SslCipher::Tls13Aes256GcmSha384],
            }),
        };

        let error = load_ssl_configs(&[server]).expect_err("unreadable certificate must fail");
        assert!(error.to_string().contains("failed to open SSL certificate"));
    }

    #[test]
    fn ssl_startup_rejects_malformed_certificate_pem() {
        let path = std::env::temp_dir().join(format!(
            "thwip-invalid-certificate-{}-{}.pem",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, "not a PEM certificate").unwrap();
        let server = Server {
            server_name: None,
            locations: Vec::new(),
            listen: "127.0.0.1:443".parse::<SocketAddr>().unwrap(),
            ssl: Some(SslServerConfig {
                certificate_path: path.to_string_lossy().into_owned(),
                private_key_path: "/unused/key.pem".to_owned(),
                handshake_timeout_ms: 10_000,
                protocols: vec![proxy_common::SslProtocol::Tlsv1_3],
                ciphers: vec![proxy_common::SslCipher::Tls13Aes256GcmSha384],
            }),
        };

        let error = load_ssl_configs(&[server]).expect_err("malformed PEM must fail");
        assert!(error.to_string().contains("contains no certificates"));
        std::fs::remove_file(path).unwrap();
    }
}
