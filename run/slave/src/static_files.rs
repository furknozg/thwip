use crate::response_bytes_with_body;
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

pub const MAX_IN_MEMORY_STATIC_FILE_SIZE: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTarget {
    pub path: String,
    pub query: Option<String>,
}

#[derive(Debug)]
pub enum StaticError {
    BadRequest,
    Forbidden,
    MethodNotAllowed,
    NotFound,
    TooLarge,
    Io(io::Error),
}

pub fn static_response_bytes(root: &Path, method: &str, raw_target: &str) -> Vec<u8> {
    match serve_static(root, method, raw_target) {
        Ok(file) => response_bytes_with_body(200, file.content_type, &file.bytes, method != "HEAD"),
        Err(StaticError::BadRequest) => {
            response_bytes_with_body(400, "text/plain; charset=utf-8", b"bad request", true)
        }
        Err(StaticError::Forbidden) => {
            response_bytes_with_body(403, "text/plain; charset=utf-8", b"forbidden", true)
        }
        Err(StaticError::MethodNotAllowed) => response_bytes_with_body(
            405,
            "text/plain; charset=utf-8",
            b"method not allowed",
            true,
        ),
        Err(StaticError::NotFound) => {
            response_bytes_with_body(404, "text/plain; charset=utf-8", b"not found", true)
        }
        Err(StaticError::TooLarge) => {
            response_bytes_with_body(413, "text/plain; charset=utf-8", b"file too large", true)
        }
        Err(StaticError::Io(_)) => response_bytes_with_body(
            500,
            "text/plain; charset=utf-8",
            b"internal server error",
            true,
        ),
    }
}

pub fn serve_static(
    root: &Path,
    method: &str,
    raw_target: &str,
) -> Result<StaticFile, StaticError> {
    if method != "GET" && method != "HEAD" {
        return Err(StaticError::MethodNotAllowed);
    }

    let target = parse_request_target(raw_target)?;
    let root = fs::canonicalize(root).map_err(map_io_error)?;
    let relative = relative_path(&target.path)?;
    let candidate = fs::canonicalize(root.join(relative)).map_err(map_io_error)?;
    if !candidate.starts_with(&root) {
        return Err(StaticError::Forbidden);
    }

    let metadata = fs::metadata(&candidate).map_err(map_io_error)?;
    if !metadata.is_file() {
        return Err(StaticError::NotFound);
    }
    if metadata.len() > MAX_IN_MEMORY_STATIC_FILE_SIZE {
        return Err(StaticError::TooLarge);
    }

    let bytes = fs::read(&candidate).map_err(map_io_error)?;
    Ok(StaticFile {
        bytes,
        content_type: content_type(&candidate),
        content_length: metadata.len(),
    })
}

pub struct StaticFile {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
    pub content_length: u64,
}

pub fn parse_request_target(raw: &str) -> Result<RequestTarget, StaticError> {
    let (raw_path, query) = raw
        .split_once('?')
        .map_or((raw, None), |(path, query)| (path, Some(query.to_owned())));
    if !raw_path.starts_with('/') {
        return Err(StaticError::BadRequest);
    }
    let bytes = percent_decode(raw_path)?;
    let path = String::from_utf8(bytes).map_err(|_| StaticError::BadRequest)?;
    if path
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err(StaticError::BadRequest);
    }
    let _ = relative_path(&path)?;
    Ok(RequestTarget { path, query })
}

fn percent_decode(raw: &str) -> Result<Vec<u8>, StaticError> {
    let mut decoded = Vec::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(StaticError::BadRequest);
            }
            let high = hex(bytes[index + 1]).ok_or(StaticError::BadRequest)?;
            let low = hex(bytes[index + 2]).ok_or(StaticError::BadRequest)?;
            decoded.push(high << 4 | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(decoded)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn relative_path(path: &str) -> Result<PathBuf, StaticError> {
    let mut relative = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => relative.push(part),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(StaticError::Forbidden)
            }
        }
    }
    Ok(relative)
}

fn map_io_error(error: io::Error) -> StaticError {
    match error.kind() {
        io::ErrorKind::NotFound => StaticError::NotFound,
        io::ErrorKind::PermissionDenied => StaticError::Forbidden,
        _ => StaticError::Io(error),
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        fs::remove_dir_all(root).unwrap();
    }
}
