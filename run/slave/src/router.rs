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

pub fn response_bytes(status: u16, body: &str) -> Vec<u8> {
    let reason = reason_phrase(status);
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
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
}
