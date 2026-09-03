use proxy_common::{Action, Location, PathMatcher, Server};
use slave::{response_bytes, route, select_server, Header, HttpVersion, RequestHead};

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
    assert!(
        matches!(route(&server, "/health"), Some(Action::Response { body, .. }) if body == "healthy")
    );
    assert!(
        matches!(route(&server, "/api/users"), Some(Action::Response { body, .. }) if body == "api")
    );
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
        version: HttpVersion::Http11,
        headers: vec![Header {
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
        version: HttpVersion::Http11,
        headers: Vec::new(),
    };
    assert_eq!(select_server(&[0, 1], 0, &request, &servers), 0);
}
