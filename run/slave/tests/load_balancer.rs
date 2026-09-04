use proxy_common::{Action, BalancePolicy, UpstreamEndpoint, UpstreamGroup};
use slave::UpstreamBalancer;
use std::collections::HashMap;

fn group(policy: BalancePolicy, weights: &[u32]) -> Action {
    Action::Proxy {
        upstream: None,
        upstream_group: None,
        upstreams: weights
            .iter()
            .enumerate()
            .map(|(index, weight)| UpstreamEndpoint {
                url: format!("http://upstream-{index}:8080"),
                weight: *weight,
            })
            .collect(),
        policy,
    }
}

#[test]
fn round_robin_cycles_through_endpoints() {
    let action = group(BalancePolicy::RoundRobin, &[1, 1, 1]);
    let mut balancer = UpstreamBalancer::default();
    let selected: Vec<String> = (0..4).map(|_| balancer.select(&action).unwrap()).collect();
    assert_eq!(
        selected,
        [
            "http://upstream-0:8080",
            "http://upstream-1:8080",
            "http://upstream-2:8080",
            "http://upstream-0:8080",
        ]
    );
}

#[test]
fn weighted_round_robin_obeys_configured_share() {
    let action = group(BalancePolicy::WeightedRoundRobin, &[2, 1]);
    let mut balancer = UpstreamBalancer::default();
    let selected: Vec<String> = (0..6).map(|_| balancer.select(&action).unwrap()).collect();
    assert_eq!(
        selected,
        [
            "http://upstream-0:8080",
            "http://upstream-0:8080",
            "http://upstream-1:8080",
            "http://upstream-0:8080",
            "http://upstream-0:8080",
            "http://upstream-1:8080",
        ]
    );
}

#[test]
fn legacy_single_upstream_remains_supported() {
    let action = Action::Proxy {
        upstream: Some("http://legacy:8080".into()),
        upstream_group: None,
        upstreams: Vec::new(),
        policy: BalancePolicy::default(),
    };
    assert_eq!(
        UpstreamBalancer::default().select(&action).unwrap(),
        "http://legacy:8080"
    );
}

#[test]
fn named_routes_share_the_group_cursor() {
    let mut groups = HashMap::new();
    groups.insert(
        "api".to_owned(),
        UpstreamGroup {
            policy: BalancePolicy::RoundRobin,
            servers: vec![
                UpstreamEndpoint {
                    url: "http://a".into(),
                    weight: 1,
                },
                UpstreamEndpoint {
                    url: "http://b".into(),
                    weight: 1,
                },
            ],
        },
    );
    let action = Action::Proxy {
        upstream: None,
        upstream_group: Some("api".into()),
        upstreams: Vec::new(),
        policy: BalancePolicy::default(),
    };
    let mut balancer = UpstreamBalancer::with_groups(groups);
    assert_eq!(balancer.select(&action).unwrap(), "http://a");
    assert_eq!(balancer.select(&action).unwrap(), "http://b");
}
