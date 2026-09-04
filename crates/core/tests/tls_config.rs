use proxy_common::Config;

#[test]
fn ssl_server_settings_are_read_from_config() {
    let config = Config::from_toml(
        r#"
[http]

[[http.servers]]
listen = "127.0.0.1:443"
ssl = { certificate_path = "/etc/thwip/fullchain.pem", private_key_path = "/etc/thwip/privkey.pem", handshake_timeout_ms = 5_000, protocols = ["tlsv1_2", "tlsv1_3"], ciphers = ["tls13_aes_256_gcm_sha384", "tls_ecdhe_rsa_with_aes_128_gcm_sha256"] }
locations = []
"#,
    )
    .unwrap();

    let ssl = config.http.servers[0].ssl.as_ref().unwrap();
    assert_eq!(ssl.certificate_path, "/etc/thwip/fullchain.pem");
    assert_eq!(ssl.private_key_path, "/etc/thwip/privkey.pem");
    assert_eq!(ssl.handshake_timeout_ms, 5_000);
    assert_eq!(ssl.protocols.len(), 2);
    assert_eq!(ssl.ciphers.len(), 2);
}

#[test]
fn ssl_defaults_to_tls_1_2_and_1_3_with_secure_ciphers() {
    let config = Config::from_toml(
        r#"
[http]

[[http.servers]]
listen = "127.0.0.1:443"
ssl = { certificate_path = "/etc/thwip/fullchain.pem", private_key_path = "/etc/thwip/privkey.pem" }
locations = []
"#,
    )
    .unwrap();

    assert_eq!(
        config.http.servers[0]
            .ssl
            .as_ref()
            .unwrap()
            .handshake_timeout_ms,
        10_000
    );
    let ssl = config.http.servers[0].ssl.as_ref().unwrap();
    assert_eq!(ssl.protocols.len(), 2);
    assert_eq!(ssl.ciphers.len(), 9);
}

#[test]
fn ssl_rejects_empty_certificate_or_key_path() {
    for ssl in [
        "{ certificate_path = \"\", private_key_path = \"/key.pem\" }",
        "{ certificate_path = \"/cert.pem\", private_key_path = \"  \" }",
    ] {
        let config = format!(
            "[http]\n\n[[http.servers]]\nlisten = \"127.0.0.1:443\"\nssl = {ssl}\nlocations = []"
        );
        let error = Config::from_toml(&config).expect_err("empty TLS paths must be rejected");
        assert!(error.to_string().contains("must not be empty"));
    }
}

#[test]
fn ssl_rejects_zero_handshake_timeout() {
    let error = Config::from_toml(
        r#"
[http]

[[http.servers]]
listen = "127.0.0.1:443"
ssl = { certificate_path = "/cert.pem", private_key_path = "/key.pem", handshake_timeout_ms = 0 }
locations = []
"#,
    )
    .expect_err("zero TLS handshake timeout must be rejected");

    assert!(error.to_string().contains("greater than zero"));
}

#[test]
fn ssl_rejects_an_empty_protocol_or_cipher_list() {
    for setting in ["protocols = []", "ciphers = []"] {
        let config = format!(
            "[http]\n\n[[http.servers]]\nlisten = \"127.0.0.1:443\"\nssl = {{ certificate_path = \"/cert.pem\", private_key_path = \"/key.pem\", {setting} }}\nlocations = []"
        );
        let error = Config::from_toml(&config).expect_err("empty SSL lists must be rejected");
        assert!(error.to_string().contains("must not be empty"));
    }
}
