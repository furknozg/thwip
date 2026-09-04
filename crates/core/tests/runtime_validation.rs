use proxy_common::Config;

fn runtime_error(runtime: &str) -> String {
    Config::from_toml(&format!("{runtime}\n[http]\nservers = []"))
        .expect_err("invalid runtime configuration must fail while loading")
        .to_string()
}

#[test]
fn readiness_event_capacity_must_be_positive() {
    let error = runtime_error("[runtime]\ntype = \"epoll\"\nmax_events = 0");
    assert!(error.contains("greater than zero"), "{error}");
}

#[test]
fn io_uring_queue_depths_and_buffer_size_must_be_positive() {
    for field in ["sq_entries", "cq_entries", "buf_size"] {
        let error = runtime_error(&format!("[runtime]\ntype = \"io_uring\"\n{field} = 0"));
        assert!(error.contains("greater than zero"), "{field}: {error}");
    }
}

#[test]
fn provided_buffer_ring_size_must_be_supported_power_of_two() {
    for size in [0, 3, 65_536] {
        let error = runtime_error(&format!(
            "[runtime]\ntype = \"io_uring\"\nbuf_ring_size = {size}"
        ));
        assert!(
            error.contains("power of two between 1 and 32768"),
            "{error}"
        );
    }
}
