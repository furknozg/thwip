use proxy_common::{Action, BalancePolicy, UpstreamEndpoint};
use std::{collections::HashMap, fmt};

#[derive(Default)]
pub struct UpstreamBalancer {
    cursors: HashMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BalanceError {
    NotProxy,
    MissingUpstream,
    AmbiguousUpstream,
    InvalidWeight,
}

impl fmt::Display for BalanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotProxy => formatter.write_str("action is not a proxy"),
            Self::MissingUpstream => formatter.write_str("proxy action has no upstream"),
            Self::AmbiguousUpstream => formatter.write_str(
                "proxy action must configure either upstream or upstreams, but not both",
            ),
            Self::InvalidWeight => formatter.write_str("upstream weights exceed supported range"),
        }
    }
}

impl UpstreamBalancer {
    pub fn select(&mut self, action: &Action) -> Result<String, BalanceError> {
        let Action::Proxy {
            upstream,
            upstreams,
            policy,
        } = action
        else {
            return Err(BalanceError::NotProxy);
        };
        if upstream.is_some() && !upstreams.is_empty() {
            return Err(BalanceError::AmbiguousUpstream);
        }
        if let Some(upstream) = upstream {
            return Ok(upstream.clone());
        }
        if upstreams.is_empty() {
            return Err(BalanceError::MissingUpstream);
        }

        let key = group_key(policy, upstreams);
        let cursor = self.cursors.entry(key).or_default();
        let selected = match policy {
            BalancePolicy::RoundRobin => (*cursor as usize) % upstreams.len(),
            BalancePolicy::WeightedRoundRobin => {
                let total = upstreams
                    .iter()
                    .try_fold(0_u64, |total, endpoint| {
                        total.checked_add(u64::from(endpoint.weight))
                    })
                    .ok_or(BalanceError::InvalidWeight)?;
                let position = *cursor % total;
                let mut boundary = 0_u64;
                upstreams
                    .iter()
                    .position(|endpoint| {
                        boundary += u64::from(endpoint.weight);
                        position < boundary
                    })
                    .unwrap()
            }
        };
        *cursor = cursor.wrapping_add(1);
        Ok(upstreams[selected].url.clone())
    }
}

fn group_key(policy: &BalancePolicy, upstreams: &[UpstreamEndpoint]) -> String {
    let mut key = format!("{policy:?}:");
    for endpoint in upstreams {
        key.push_str(&endpoint.url);
        key.push('#');
        key.push_str(&endpoint.weight.to_string());
        key.push('|');
    }
    key
}
