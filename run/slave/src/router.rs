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
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> Server {
        Server {
            server_name: None,
            listen: "127.0.0.1:0".parse().unwrap(),
            locations: vec![
                Location {
                    matcher: PathMatcher::Prefix { path: "/".into() },
                    action: Action::Response {
                        status: 200,
                        body: "root".into(),
                    },
                },
                Location {
                    matcher: PathMatcher::Prefix {
                        path: "/api".into(),
                    },
                    action: Action::Response {
                        status: 200,
                        body: "api".into(),
                    },
                },
                Location {
                    matcher: PathMatcher::Exact {
                        path: "/health".into(),
                    },
                    action: Action::Response {
                        status: 200,
                        body: "healthy".into(),
                    },
                },
            ],
        }
    }

    #[test]
    fn exact_matches_win_and_prefix_matches_are_longest() {
        let server = server();

        assert!(matches!(
            route(&server, "/health"),
            Some(Action::Response { body, .. }) if body == "healthy"
        ));
        assert!(matches!(
            route(&server, "/api/users"),
            Some(Action::Response { body, .. }) if body == "api"
        ));
    }

    #[test]
    fn response_contains_a_valid_status_and_length() {
        let response = String::from_utf8(response_bytes(200, "OK")).unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Length: 2\r\n"));
        assert!(response.ends_with("\r\n\r\nOK"));
    }

    #[test]
    fn virtual_host_selection_matches_host_ignoring_case_and_port() {
        let mut default = server();
        default.server_name = Some("default.test".into());
        let mut named = server();
        named.server_name = Some("api.example.test".into());
        let servers = vec![default, named];
        let request = RequestHead {
            method: "GET".into(),
            target: "/".into(),
            version: crate::HttpVersion::Http11,
            headers: vec![crate::Header {
                name: "host".into(),
                value: "API.EXAMPLE.TEST:8080".into(),
            }],
        };

        assert_eq!(select_server(&[0, 1], 0, &request, &servers), 1);
    }

    #[test]
    fn virtual_host_selection_falls_back_to_the_group_default() {
        let servers = vec![server(), server()];
        let request = RequestHead {
            method: "GET".into(),
            target: "/".into(),
            version: crate::HttpVersion::Http11,
            headers: Vec::new(),
        };

        assert_eq!(select_server(&[0, 1], 0, &request, &servers), 0);
    }
}
