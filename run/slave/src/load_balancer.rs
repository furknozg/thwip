use proxy_common::{Action, BalancePolicy, UpstreamEndpoint, UpstreamGroup};
use std::{collections::HashMap, fmt};

#[derive(Default)]
pub struct UpstreamBalancer {
    groups: HashMap<String, UpstreamGroup>,
    cursors: HashMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BalanceError {
    NotProxy,
    MissingUpstream,
    AmbiguousUpstream,
    InvalidWeight,
    UnknownGroup(String),
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
            Self::UnknownGroup(name) => write!(formatter, "unknown upstream group {name:?}"),
        }
    }
}

impl UpstreamBalancer {
    pub fn with_groups(groups: HashMap<String, UpstreamGroup>) -> Self {
        Self {
            groups,
            cursors: HashMap::new(),
        }
    }

    pub fn select(&mut self, action: &Action) -> Result<String, BalanceError> {
        let Action::Proxy {
            upstream,
            upstream_group,
            upstreams,
            policy,
        } = action
        else {
            return Err(BalanceError::NotProxy);
        };
        let configured = usize::from(upstream.is_some())
            + usize::from(upstream_group.is_some())
            + usize::from(!upstreams.is_empty());
        if configured > 1 {
            return Err(BalanceError::AmbiguousUpstream);
        }
        if let Some(upstream) = upstream {
            return Ok(upstream.clone());
        }
        let (key, policy, upstreams) = if let Some(name) = upstream_group {
            let group = self
                .groups
                .get(name)
                .ok_or_else(|| BalanceError::UnknownGroup(name.clone()))?;
            (
                format!("named:{name}"),
                &group.policy,
                group.servers.as_slice(),
            )
        } else {
            (group_key(policy, upstreams), policy, upstreams.as_slice())
        };
        if upstreams.is_empty() {
            return Err(BalanceError::MissingUpstream);
        }

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
