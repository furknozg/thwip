use proxy_common::Config;

#[test]
fn worker_limits_default_when_worker_section_is_omitted() {
    let config = Config::from_toml("[http]\nservers = []").expect("config should parse");

    assert_eq!(config.worker.max_connections, 1_024);
    assert_eq!(config.worker.max_read_buffer_size, 64 * 1024);
    assert_eq!(config.worker.max_write_buffer_size, 8 * 1024 * 1024);
    assert_eq!(config.worker.idle_timeout_ms, 30_000);
    assert_eq!(config.worker.drain_timeout_ms, 10_000);
}

#[test]
fn worker_limits_are_read_from_config() {
    let config = Config::from_toml(
        r#"
[worker]
max_connections = 32
max_read_buffer_size = 1024
max_write_buffer_size = 4096
idle_timeout_ms = 250
drain_timeout_ms = 500

[http]
servers = []
"#,
    )
    .expect("config should parse");

    assert_eq!(config.worker.max_connections, 32);
    assert_eq!(config.worker.max_read_buffer_size, 1024);
    assert_eq!(config.worker.max_write_buffer_size, 4096);
    assert_eq!(config.worker.idle_timeout_ms, 250);
    assert_eq!(config.worker.drain_timeout_ms, 500);
}

#[test]
fn worker_limits_reject_zero_values() {
    let error = Config::from_toml(
        r#"
[worker]
max_connections = 0

[http]
servers = []
"#,
    )
    .expect_err("zero connection limit must be rejected");

    assert!(error.to_string().contains("greater than zero"));
}
