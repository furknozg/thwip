use proxy_common::{AsyncRuntimeConfig, Config};

#[test]
fn parses_kqueue_runtime_config() {
    let config = Config::from_toml(
        r#"
[runtime]
type = "kqueue"
max_events = 2048

[http]
servers = []
"#,
    )
    .expect("kqueue configuration should parse");

    assert!(matches!(
        config.runtime,
        AsyncRuntimeConfig::Kqueue { max_events: 2048 }
    ));
}
