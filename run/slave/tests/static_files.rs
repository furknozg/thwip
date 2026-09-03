use slave::{parse_request_target, serve_static, StaticError};
use std::fs;

#[test]
fn parses_query_and_rejects_traversal() {
    assert_eq!(
        parse_request_target("/assets/a.txt?x=1").unwrap().path,
        "/assets/a.txt"
    );
    assert!(matches!(
        parse_request_target("/%2e%2e/secret"),
        Err(StaticError::Forbidden)
    ));
}

#[test]
fn serves_a_file_and_handles_head() {
    let root = std::env::temp_dir().join(format!("thwip-static-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("hello.txt"), "hello").unwrap();

    let file = serve_static(&root, "GET", "/hello.txt").unwrap();
    assert_eq!(file.bytes, b"hello");
    assert_eq!(file.content_type, "text/plain; charset=utf-8");
    assert_eq!(
        serve_static(&root, "HEAD", "/hello.txt")
            .unwrap()
            .content_length,
        5
    );

    fs::write(root.join("index.html"), "home").unwrap();
    assert_eq!(serve_static(&root, "GET", "/").unwrap().bytes, b"home");
    fs::remove_dir_all(root).unwrap();
}
