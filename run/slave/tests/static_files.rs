use slave::{
    parse_request_target, serve_static, static_stream_response, Header, HttpVersion, RequestHead,
    StaticChunk, StaticError,
};
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

fn request(target: &str, headers: Vec<Header>) -> RequestHead {
    RequestHead {
        method: "GET".to_owned(),
        target: target.to_owned(),
        version: HttpVersion::Http11,
        headers,
    }
}

#[test]
fn streams_ranges_with_cache_and_complete_mime_headers() {
    let root = std::env::temp_dir().join(format!("thwip-static-stream-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let contents = vec![b'x'; 5 * 1024 * 1024];
    fs::write(root.join("video.webm"), &contents).unwrap();

    let ranged = request(
        "/video.webm",
        vec![Header {
            name: "range".to_owned(),
            value: "bytes=1024-66559".to_owned(),
        }],
    );
    let response = static_stream_response(&root, &ranged).unwrap();
    let head = String::from_utf8(response.head).unwrap();
    assert!(head.starts_with("HTTP/1.1 206 Partial Content"));
    assert!(head.contains("Content-Range: bytes 1024-66559/5242880"));
    assert!(head.contains("Content-Type: video/webm"));
    assert!(head.contains("Cache-Control: public, max-age=3600"));
    assert!(head.contains("ETag: \""));

    let stream = response.stream.unwrap();
    let mut streamed = Vec::new();
    loop {
        match stream.try_next().unwrap() {
            StaticChunk::Data(bytes) => {
                assert!(bytes.len() <= 64 * 1024);
                streamed.extend(bytes);
            }
            StaticChunk::Pending => std::thread::yield_now(),
            StaticChunk::Finished => break,
        }
    }
    assert_eq!(streamed, contents[1024..=66559]);
    fs::remove_dir_all(root).unwrap();
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
