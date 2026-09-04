use proxy_common::{Action, BalancePolicy, UpstreamEndpoint};
use slave::UpstreamBalancer;

fn group(policy: BalancePolicy, weights: &[u32]) -> Action {
    Action::Proxy {
        upstream: None,
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
        upstreams: Vec::new(),
        policy: BalancePolicy::default(),
    };
    assert_eq!(
        UpstreamBalancer::default().select(&action).unwrap(),
        "http://legacy:8080"
    );
}
