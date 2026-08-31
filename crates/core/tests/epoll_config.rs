use proxy_common::{AsyncRuntimeConfig, Config};

#[test]
fn parses_epoll_runtime_config() {
    let config = Config::from_toml(
        r#"
worker_count = 4

[runtime]
type = "epoll"
max_events = 2048

[http]
servers = []
"#,
    )
    .expect("epoll configuration should parse");

    assert_eq!(config.worker_count, 4);
    assert!(matches!(
        config.runtime,
        AsyncRuntimeConfig::Epoll { max_events: 2048 }
    ));
}
