use std::{error::Error, fmt};

pub const MAX_REQUEST_HEAD_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    pub method: String,
    pub target: String,
    pub version: HttpVersion,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpVersion {
    Http10,
    Http11,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Stored in lowercase so later lookups are case-insensitive.
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyFramingError {
    InvalidContentLength,
    ConflictingContentLength,
    UnsupportedTransferEncoding,
}

impl RequestHead {
    /// Returns the request body length for the subset of HTTP/1 framing the
    /// server currently supports. Transfer-Encoding is rejected until a
    /// chunked decoder exists.
    pub fn body_length(&self) -> Result<usize, BodyFramingError> {
        if self
            .headers
            .iter()
            .any(|header| header.name == "transfer-encoding")
        {
            return Err(BodyFramingError::UnsupportedTransferEncoding);
        }

        let mut content_length = None;
        for header in self
            .headers
            .iter()
            .filter(|header| header.name == "content-length")
        {
            if header.value.is_empty() || !header.value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(BodyFramingError::InvalidContentLength);
            }
            let value = header
                .value
                .parse::<usize>()
                .map_err(|_| BodyFramingError::InvalidContentLength)?;
            match content_length {
                Some(previous) if previous != value => {
                    return Err(BodyFramingError::ConflictingContentLength)
                }
                _ => content_length = Some(value),
            }
        }

        Ok(content_length.unwrap_or(0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestHeadParse {
    Incomplete,
    Complete {
        request: RequestHead,
        consumed: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    HeadTooLarge { limit: usize },
    InvalidRequestLine,
    InvalidMethod,
    InvalidTarget,
    UnsupportedVersion,
    InvalidHeader,
    InvalidText,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeadTooLarge { limit } => write!(formatter, "request head exceeds {limit} bytes"),
            Self::InvalidRequestLine => formatter.write_str("invalid HTTP request line"),
            Self::InvalidMethod => formatter.write_str("invalid HTTP method"),
            Self::InvalidTarget => formatter.write_str("invalid HTTP request target"),
            Self::UnsupportedVersion => formatter.write_str("unsupported HTTP version"),
            Self::InvalidHeader => formatter.write_str("invalid HTTP header"),
            Self::InvalidText => formatter.write_str("HTTP request head is not valid text"),
        }
    }
}

impl Error for ParseError {}

/// Parses one HTTP/1.x request head from `bytes` without consuming a possible
/// request body that follows it. Call this after every append to a connection's
/// read buffer; `Incomplete` is normal for fragmented TCP reads.
pub fn parse_request_head(bytes: &[u8]) -> Result<RequestHeadParse, ParseError> {
    let Some(head_end) = find_head_end(bytes) else {
        if bytes.len() > MAX_REQUEST_HEAD_SIZE {
            return Err(ParseError::HeadTooLarge {
                limit: MAX_REQUEST_HEAD_SIZE,
            });
        }
        return Ok(RequestHeadParse::Incomplete);
    };

    if head_end > MAX_REQUEST_HEAD_SIZE {
        return Err(ParseError::HeadTooLarge {
            limit: MAX_REQUEST_HEAD_SIZE,
        });
    }

    let text = std::str::from_utf8(&bytes[..head_end]).map_err(|_| ParseError::InvalidText)?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().ok_or(ParseError::InvalidRequestLine)?;
    let (method, target, version) = parse_request_line(request_line)?;
    let mut headers = Vec::new();

    for line in lines {
        if line.is_empty() {
            continue;
        }
        headers.push(parse_header(line)?);
    }

    Ok(RequestHeadParse::Complete {
        request: RequestHead {
            method: method.to_owned(),
            target: target.to_owned(),
            version,
            headers,
        },
        consumed: head_end + 4,
    })
}

fn find_head_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_request_line(line: &str) -> Result<(&str, &str, HttpVersion), ParseError> {
    let mut parts = line.split_ascii_whitespace();
    let method = parts.next().ok_or(ParseError::InvalidRequestLine)?;
    let target = parts.next().ok_or(ParseError::InvalidRequestLine)?;
    let version = parts.next().ok_or(ParseError::InvalidRequestLine)?;

    if parts.next().is_some() {
        return Err(ParseError::InvalidRequestLine);
    }
    if !method.bytes().all(is_token) {
        return Err(ParseError::InvalidMethod);
    }
    if target.is_empty() || target.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ParseError::InvalidTarget);
    }

    let version = match version {
        "HTTP/1.0" => HttpVersion::Http10,
        "HTTP/1.1" => HttpVersion::Http11,
        _ => return Err(ParseError::UnsupportedVersion),
    };

    Ok((method, target, version))
}

fn parse_header(line: &str) -> Result<Header, ParseError> {
    let (name, value) = line.split_once(':').ok_or(ParseError::InvalidHeader)?;
    if name.is_empty() || !name.bytes().all(is_token) || value.contains(['\r', '\n']) {
        return Err(ParseError::InvalidHeader);
    }

    Ok(Header {
        name: name.to_ascii_lowercase(),
        value: value.trim_matches([' ', '\t']).to_owned(),
    })
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let error = parse_request_head(b"GET / HTTP/1.1\r\nHost\r\n\r\n").unwrap_err();
        assert_eq!(error, ParseError::InvalidHeader);
    }

    #[test]
    fn validates_content_length_and_rejects_transfer_encoding() {
        let RequestHeadParse::Complete { request, .. } = parse_request_head(
            b"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n",
        )
        .unwrap() else {
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
        let RequestHeadParse::Complete { request, .. } = parse_request_head(
            b"POST / HTTP/1.1\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\n",
        )
        .unwrap() else {
            panic!("expected complete request head");
        };

        assert_eq!(
            request.body_length(),
            Err(BodyFramingError::ConflictingContentLength)
        );
    }
}
