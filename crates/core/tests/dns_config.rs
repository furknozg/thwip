use proxy_common::Config;

#[test]
fn dns_settings_default_when_section_is_omitted() {
    let config = Config::from_toml("[http]\nservers = []").unwrap();

    assert_eq!(config.dns.resolver_threads, 2);
    assert_eq!(config.dns.timeout_ms, 3_000);
}

#[test]
fn dns_settings_are_read_from_config() {
    let config = Config::from_toml(
        r#"
[dns]
resolver_threads = 4
timeout_ms = 750

[http]
servers = []
"#,
    )
    .unwrap();

    assert_eq!(config.dns.resolver_threads, 4);
    assert_eq!(config.dns.timeout_ms, 750);
}

#[test]
fn dns_settings_reject_zero() {
    for invalid in [
        "[dns]\nresolver_threads = 0\n[http]\nservers = []",
        "[dns]\ntimeout_ms = 0\n[http]\nservers = []",
    ] {
        let error = Config::from_toml(invalid).expect_err("zero DNS setting must be rejected");
        assert!(error.to_string().contains("greater than zero"));
    }
}
