use proxy_common::{AsyncRuntimeConfig, Config};

#[test]
fn parses_io_uring_runtime_config() {
    let config = Config::from_toml(
        r#"
[runtime]
type = "io_uring"
sq_entries = 4096
cq_entries = 8192
buf_ring_size = 16384
buf_size = 8192

[http]
servers = []
"#,
    )
    .expect("io_uring configuration should parse");

    let AsyncRuntimeConfig::IoUring {
        sq_entries,
        cq_entries,
        buf_ring_size,
        buf_size,
    } = config.runtime
    else {
        panic!("expected io_uring runtime");
    };

    assert_eq!(sq_entries, 4096);
    assert_eq!(cq_entries, 8192);
    assert_eq!(buf_ring_size, 16_384);
    assert_eq!(buf_size, 8192);
}
