use proxy_common::{Action, BalancePolicy, Config};

#[test]
fn parses_weighted_upstream_group() {
    let config = Config::from_toml(
        r#"
[runtime]
type = "epoll"

[[http.servers]]
listen = "127.0.0.1:8080"
[[http.servers.locations]]
matcher = { type = "prefix", path = "/" }
action = { type = "proxy", policy = "weighted_round_robin", upstreams = [
  { url = "http://127.0.0.1:9001", weight = 2 },
  { url = "http://127.0.0.1:9002", weight = 1 }
] }
"#,
    )
    .unwrap();

    let Action::Proxy {
        upstream,
        upstreams,
        policy,
    } = &config.http.servers[0].locations[0].action
    else {
        panic!("expected proxy action");
    };
    assert!(upstream.is_none());
    assert_eq!(*policy, BalancePolicy::WeightedRoundRobin);
    assert_eq!(upstreams[0].weight, 2);
}

#[test]
fn rejects_missing_ambiguous_and_zero_weight_upstreams() {
    for action in [
        "action = { type = \"proxy\" }",
        "action = { type = \"proxy\", upstream = \"http://a\", upstreams = [{ url = \"http://b\" }] }",
        "action = { type = \"proxy\", upstreams = [{ url = \"http://a\", weight = 0 }] }",
    ] {
        let source = format!(
            "[[http.servers]]\nlisten = \"127.0.0.1:8080\"\n[[http.servers.locations]]\nmatcher = {{ type = \"prefix\", path = \"/\" }}\n{action}"
        );
        assert!(Config::from_toml(&source).is_err(), "accepted: {action}");
    }
}
