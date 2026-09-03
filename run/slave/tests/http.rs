use slave::{parse_request_head, BodyFramingError, HttpVersion, ParseError, RequestHeadParse};

#[test]
fn waits_for_a_complete_request_head() {
    assert_eq!(
        parse_request_head(b"GET / HTTP/1.1\r\nHost: example.com\r\n"),
        Ok(RequestHeadParse::Incomplete)
    );
}

#[test]
fn parses_a_request_head_and_preserves_body_bytes() {
    let bytes = b"POST /api HTTP/1.1\r\nHost: example.com\r\nContent-Length: 2\r\n\r\n{}";
    let RequestHeadParse::Complete { request, consumed } = parse_request_head(bytes).unwrap()
    else {
        panic!("expected a complete request");
    };

    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/api");
    assert_eq!(request.version, HttpVersion::Http11);
    assert_eq!(request.headers[0].name, "host");
    assert_eq!(consumed, bytes.len() - 2);
    assert_eq!(&bytes[consumed..], b"{}");
}

#[test]
fn rejects_a_malformed_header() {
    assert_eq!(
        parse_request_head(b"GET / HTTP/1.1\r\nHost\r\n\r\n").unwrap_err(),
        ParseError::InvalidHeader
    );
}

#[test]
fn validates_content_length_and_rejects_transfer_encoding() {
    let RequestHeadParse::Complete { request, .. } =
        parse_request_head(b"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n")
            .unwrap()
    else {
        panic!("expected complete request head");
    };
    assert_eq!(request.body_length(), Ok(5));

    let RequestHeadParse::Complete { request, .. } =
        parse_request_head(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap()
    else {
        panic!("expected complete request head");
    };
    assert_eq!(
        request.body_length(),
        Err(BodyFramingError::UnsupportedTransferEncoding)
    );
}

#[test]
fn rejects_conflicting_content_lengths() {
    let RequestHeadParse::Complete { request, .. } =
        parse_request_head(b"POST / HTTP/1.1\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\n")
            .unwrap()
    else {
        panic!("expected complete request head");
    };

    assert_eq!(
        request.body_length(),
        Err(BodyFramingError::ConflictingContentLength)
    );
}
