use proxy_common::Config;

#[test]
fn proxy_timeouts_default_when_section_is_omitted() {
    let config = Config::from_toml("[http]\nservers = []").unwrap();

    assert_eq!(config.proxy.connect_timeout_ms, 3_000);
    assert_eq!(config.proxy.write_timeout_ms, 30_000);
    assert_eq!(config.proxy.read_timeout_ms, 30_000);
}

#[test]
fn proxy_timeouts_are_read_from_config() {
    let config = Config::from_toml(
        r#"
[proxy]
connect_timeout_ms = 25
write_timeout_ms = 50
read_timeout_ms = 75

[http]
servers = []
"#,
    )
    .unwrap();

    assert_eq!(config.proxy.connect_timeout_ms, 25);
    assert_eq!(config.proxy.write_timeout_ms, 50);
    assert_eq!(config.proxy.read_timeout_ms, 75);
}

#[test]
fn proxy_timeouts_reject_zero() {
    let error = Config::from_toml(
        r#"
[proxy]
connect_timeout_ms = 0

[http]
servers = []
"#,
    )
    .expect_err("zero proxy timeout must be rejected");

    assert!(error.to_string().contains("greater than zero"));
}
