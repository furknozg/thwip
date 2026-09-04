use proxy_common::{AsyncRuntimeConfig, Config};

#[test]
fn parses_auto_runtime_config() {
    let config = Config::from_toml(
        r#"
worker_count = 2

[runtime]
type = "auto"
max_events = 2048
sq_entries = 1024
cq_entries = 2048
buf_ring_size = 512
buf_size = 4096

[http]
servers = []
"#,
    )
    .expect("auto runtime configuration should parse");

    assert!(matches!(
        config.runtime,
        AsyncRuntimeConfig::Auto {
            max_events: 2048,
            sq_entries: 1024,
            cq_entries: 2048,
            buf_ring_size: 512,
            buf_size: 4096,
        }
    ));
}

#[test]
fn auto_runtime_uses_backend_defaults() {
    let config = Config::from_toml(
        r#"
[runtime]
type = "auto"

[http]
servers = []
"#,
    )
    .expect("auto runtime defaults should parse");

    assert!(matches!(
        config.runtime,
        AsyncRuntimeConfig::Auto {
            max_events: 1024,
            sq_entries: 4096,
            cq_entries: 8192,
            buf_ring_size: 16_384,
            buf_size: 8192,
        }
    ));
}
