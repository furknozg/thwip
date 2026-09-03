use crate::RequestHead;
use proxy_common::{Action, Location, PathMatcher, Server};

pub fn route<'a>(server: &'a Server, target: &str) -> Option<&'a Action> {
    server
        .locations
        .iter()
        .find_map(|location| matches_exact(location, target).then_some(&location.action))
        .or_else(|| {
            server
                .locations
                .iter()
                .filter_map(|location| match &location.matcher {
                    PathMatcher::Prefix { path } if target.starts_with(path) => {
                        Some((path.len(), &location.action))
                    }
                    _ => None,
                })
                .max_by_key(|(length, _)| *length)
                .map(|(_, action)| action)
        })
}

/// Chooses a virtual host within one already-bound listener group. A missing
/// or unknown Host header intentionally falls back to that listener's first
/// configured server.
pub fn select_server(
    server_indices: &[usize],
    default_server: usize,
    request: &RequestHead,
    servers: &[Server],
) -> usize {
    let host = request
        .headers
        .iter()
        .find(|header| header.name == "host")
        .map(|header| hostname(&header.value));

    host.and_then(|host| {
        server_indices.iter().copied().find(|&server_index| {
            servers.get(server_index).is_some_and(|server| {
                server
                    .server_name
                    .as_deref()
                    .is_some_and(|server_name| server_name.eq_ignore_ascii_case(host))
            })
        })
    })
    .unwrap_or(default_server)
}

fn hostname(host: &str) -> &str {
    let host = host.trim();

    if let Some(bracketed) = host.strip_prefix('[') {
        return bracketed
            .split_once(']')
            .map_or(bracketed, |(address, _)| address);
    }

    host.split_once(':').map_or(host, |(name, _)| name)
}

pub fn response_bytes(status: u16, body: &str) -> Vec<u8> {
    response_bytes_with_body(status, "text/plain; charset=utf-8", body.as_bytes(), true)
}

pub fn response_bytes_with_body(
    status: u16,
    content_type: &str,
    body: &[u8],
    include_body: bool,
) -> Vec<u8> {
    let reason = reason_phrase(status);
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ).into_bytes();
    if include_body {
        response.extend_from_slice(body);
    }
    response
}

fn matches_exact(location: &Location, target: &str) -> bool {
    matches!(&location.matcher, PathMatcher::Exact { path } if path == target)
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}
