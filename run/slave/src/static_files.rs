use crate::response_bytes_with_body;
use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::mpsc::{self, Receiver, TryRecvError},
    time::UNIX_EPOCH,
};

pub const MAX_IN_MEMORY_STATIC_FILE_SIZE: u64 = 4 * 1024 * 1024;
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

pub struct StaticStream {
    chunks: Receiver<io::Result<Vec<u8>>>,
}

pub enum StaticChunk {
    Data(Vec<u8>),
    Pending,
    Finished,
}

impl StaticStream {
    pub fn try_next(&self) -> io::Result<StaticChunk> {
        match self.chunks.try_recv() {
            Ok(Ok(bytes)) => Ok(StaticChunk::Data(bytes)),
            Ok(Err(error)) => Err(error),
            Err(TryRecvError::Empty) => Ok(StaticChunk::Pending),
            Err(TryRecvError::Disconnected) => Ok(StaticChunk::Finished),
        }
    }
}

pub struct StaticStreamResponse {
    pub head: Vec<u8>,
    pub stream: Option<StaticStream>,
}

pub fn static_stream_response(
    root: &Path,
    request: &crate::RequestHead,
) -> Result<StaticStreamResponse, StaticError> {
    if request.method != "GET" && request.method != "HEAD" {
        return Err(StaticError::MethodNotAllowed);
    }
    let (path, metadata) = resolve_static_path(root, &request.target)?;
    let length = metadata.len();
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_secs());
    let etag = format!("\"{length:x}-{modified:x}\"");
    if request
        .headers
        .iter()
        .any(|header| header.name == "if-none-match" && header.value == etag)
    {
        return Ok(StaticStreamResponse {
            head: format!(
                "HTTP/1.1 304 Not Modified\r\nETag: {etag}\r\nCache-Control: public, max-age=3600\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            stream: None,
        });
    }
    let range = request
        .headers
        .iter()
        .find(|header| header.name == "range")
        .map(|header| parse_range(&header.value, length))
        .transpose()?;
    let (status, start, end) = range.map_or((200, 0, length.saturating_sub(1)), |(start, end)| {
        (206, start, end)
    });
    let response_length = if length == 0 { 0 } else { end - start + 1 };
    let reason = if status == 206 {
        "Partial Content"
    } else {
        "OK"
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {}\r\nContent-Length: {response_length}\r\nAccept-Ranges: bytes\r\nETag: {etag}\r\nCache-Control: public, max-age=3600\r\n",
        content_type(&path)
    );
    if status == 206 {
        head.push_str(&format!("Content-Range: bytes {start}-{end}/{length}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");
    let stream = (request.method == "GET" && response_length > 0)
        .then(|| spawn_file_stream(path, start, response_length));
    Ok(StaticStreamResponse {
        head: head.into_bytes(),
        stream,
    })
}

fn spawn_file_stream(path: PathBuf, start: u64, length: u64) -> StaticStream {
    let (sender, chunks) = mpsc::sync_channel(2);
    std::thread::spawn(move || {
        let result = (|| -> io::Result<()> {
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(start))?;
            let mut remaining = length;
            while remaining > 0 {
                let mut buffer = vec![0; STREAM_CHUNK_SIZE.min(remaining as usize)];
                file.read_exact(&mut buffer)?;
                remaining -= buffer.len() as u64;
                if sender.send(Ok(buffer)).is_err() {
                    return Ok(());
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = sender.send(Err(error));
        }
    });
    StaticStream { chunks }
}

fn parse_range(value: &str, length: u64) -> Result<(u64, u64), StaticError> {
    let value = value
        .strip_prefix("bytes=")
        .ok_or(StaticError::BadRequest)?;
    if value.contains(',') || length == 0 {
        return Err(StaticError::RangeNotSatisfiable);
    }
    let (start, end) = value.split_once('-').ok_or(StaticError::BadRequest)?;
    let (start, end) = if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| StaticError::BadRequest)?;
        if suffix == 0 {
            return Err(StaticError::RangeNotSatisfiable);
        }
        (length.saturating_sub(suffix), length - 1)
    } else {
        let start = start.parse::<u64>().map_err(|_| StaticError::BadRequest)?;
        let end = if end.is_empty() {
            length - 1
        } else {
            end.parse::<u64>().map_err(|_| StaticError::BadRequest)?
        };
        (start, end.min(length - 1))
    };
    if start >= length || start > end {
        Err(StaticError::RangeNotSatisfiable)
    } else {
        Ok((start, end))
    }
}

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
    RangeNotSatisfiable,
    Io(io::Error),
}

pub fn static_response_bytes(root: &Path, method: &str, raw_target: &str) -> Vec<u8> {
    match serve_static(root, method, raw_target) {
        Ok(file) => {
            response_bytes_with_body(200, &file.content_type, &file.bytes, method != "HEAD")
        }
        Err(error) => static_error_response(error),
    }
}

pub fn static_error_response(error: StaticError) -> Vec<u8> {
    match error {
        StaticError::BadRequest => {
            response_bytes_with_body(400, "text/plain; charset=utf-8", b"bad request", true)
        }
        StaticError::Forbidden => {
            response_bytes_with_body(403, "text/plain; charset=utf-8", b"forbidden", true)
        }
        StaticError::MethodNotAllowed => response_bytes_with_body(
            405,
            "text/plain; charset=utf-8",
            b"method not allowed",
            true,
        ),
        StaticError::NotFound => {
            response_bytes_with_body(404, "text/plain; charset=utf-8", b"not found", true)
        }
        StaticError::TooLarge => {
            response_bytes_with_body(413, "text/plain; charset=utf-8", b"file too large", true)
        }
        StaticError::RangeNotSatisfiable => response_bytes_with_body(
            416,
            "text/plain; charset=utf-8",
            b"range not satisfiable",
            true,
        ),
        StaticError::Io(_) => response_bytes_with_body(
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

    let (candidate, metadata) = resolve_static_path(root, raw_target)?;
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

fn resolve_static_path(
    root: &Path,
    raw_target: &str,
) -> Result<(PathBuf, fs::Metadata), StaticError> {
    let target = parse_request_target(raw_target)?;
    let root = fs::canonicalize(root).map_err(map_io_error)?;
    let relative = relative_path(&target.path)?;
    let mut candidate = fs::canonicalize(root.join(relative)).map_err(map_io_error)?;
    if !candidate.starts_with(&root) {
        return Err(StaticError::Forbidden);
    }
    let mut metadata = fs::metadata(&candidate).map_err(map_io_error)?;
    if metadata.is_dir() {
        candidate = fs::canonicalize(candidate.join("index.html")).map_err(map_io_error)?;
        if !candidate.starts_with(&root) {
            return Err(StaticError::Forbidden);
        }
        metadata = fs::metadata(&candidate).map_err(map_io_error)?;
    }
    if !metadata.is_file() {
        return Err(StaticError::NotFound);
    }
    Ok((candidate, metadata))
}

pub struct StaticFile {
    pub bytes: Vec<u8>,
    pub content_type: String,
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

fn content_type(path: &Path) -> String {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    if mime.type_() == mime_guess::mime::TEXT
        || matches!(mime.subtype().as_str(), "javascript" | "json" | "xml")
    {
        format!("{}; charset=utf-8", mime.essence_str())
    } else {
        mime.essence_str().to_owned()
    }
}
