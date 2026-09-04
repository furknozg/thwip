use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub http: HttpConfig,

    #[serde(default)]
    pub runtime: AsyncRuntimeConfig,

    /// Defaults to one worker per logical CPU available to this process.
    #[serde(default = "default_worker_count")]
    pub worker_count: usize,

    #[serde(default)]
    pub worker: WorkerConfig,

    #[serde(default)]
    pub proxy: ProxyTimeoutConfig,

    #[serde(default)]
    pub dns: DnsConfig,
}

/// Limits shared by every worker runtime. Durations are expressed in
/// milliseconds in TOML to keep the configuration format unambiguous.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    #[serde(
        default = "default_max_connections",
        deserialize_with = "deserialize_positive_usize"
    )]
    pub max_connections: usize,

    #[serde(
        default = "default_max_read_buffer_size",
        deserialize_with = "deserialize_positive_usize"
    )]
    pub max_read_buffer_size: usize,

    #[serde(
        default = "default_max_write_buffer_size",
        deserialize_with = "deserialize_positive_usize"
    )]
    pub max_write_buffer_size: usize,

    #[serde(
        default = "default_idle_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub idle_timeout_ms: u64,

    #[serde(
        default = "default_drain_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub drain_timeout_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_read_buffer_size: default_max_read_buffer_size(),
            max_write_buffer_size: default_max_write_buffer_size(),
            idle_timeout_ms: default_idle_timeout_ms(),
            drain_timeout_ms: default_drain_timeout_ms(),
        }
    }
}

/// Deadlines for each stage of an upstream proxy exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyTimeoutConfig {
    #[serde(
        default = "default_proxy_connect_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub connect_timeout_ms: u64,

    #[serde(
        default = "default_proxy_write_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub write_timeout_ms: u64,

    #[serde(
        default = "default_proxy_read_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub read_timeout_ms: u64,
}

impl Default for ProxyTimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_proxy_connect_timeout_ms(),
            write_timeout_ms: default_proxy_write_timeout_ms(),
            read_timeout_ms: default_proxy_read_timeout_ms(),
        }
    }
}

/// Settings for resolving upstream hostnames outside the readiness loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsConfig {
    #[serde(
        default = "default_dns_resolver_threads",
        deserialize_with = "deserialize_positive_usize"
    )]
    pub resolver_threads: usize,

    #[serde(
        default = "default_dns_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub timeout_ms: u64,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            resolver_threads: default_dns_resolver_threads(),
            timeout_ms: default_dns_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub servers: Vec<Server>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AsyncRuntimeConfig {
    Auto {
        #[serde(
            default = "default_epoll_max_events",
            deserialize_with = "deserialize_runtime_positive_usize"
        )]
        max_events: usize,
        #[serde(
            default = "default_sq_entries",
            deserialize_with = "deserialize_runtime_positive_u32"
        )]
        sq_entries: u32,
        #[serde(
            default = "default_cq_entries",
            deserialize_with = "deserialize_runtime_positive_u32"
        )]
        cq_entries: u32,
        #[serde(
            default = "default_buf_ring_size",
            deserialize_with = "deserialize_buf_ring_size"
        )]
        buf_ring_size: u32,
        #[serde(
            default = "default_buf_size",
            deserialize_with = "deserialize_runtime_positive_usize"
        )]
        buf_size: usize,
    },

    Epoll {
        #[serde(
            default = "default_epoll_max_events",
            deserialize_with = "deserialize_runtime_positive_usize"
        )]
        max_events: usize,
    },

    Kqueue {
        #[serde(
            default = "default_epoll_max_events",
            deserialize_with = "deserialize_runtime_positive_usize"
        )]
        max_events: usize,
    },

    IoUring {
        #[serde(
            default = "default_sq_entries",
            deserialize_with = "deserialize_runtime_positive_u32"
        )]
        sq_entries: u32,
        #[serde(
            default = "default_cq_entries",
            deserialize_with = "deserialize_runtime_positive_u32"
        )]
        cq_entries: u32,
        #[serde(
            default = "default_buf_ring_size",
            deserialize_with = "deserialize_buf_ring_size"
        )]
        buf_ring_size: u32,
        #[serde(
            default = "default_buf_size",
            deserialize_with = "deserialize_runtime_positive_usize"
        )]
        buf_size: usize,
    },
}

impl Default for AsyncRuntimeConfig {
    fn default() -> Self {
        Self::Epoll {
            max_events: default_epoll_max_events(),
        }
    }
}

const fn default_epoll_max_events() -> usize {
    1024
}

const fn default_sq_entries() -> u32 {
    4096
}

const fn default_cq_entries() -> u32 {
    8192
}

const fn default_buf_ring_size() -> u32 {
    16_384
}

const fn default_buf_size() -> usize {
    8192
}

const fn default_max_connections() -> usize {
    1_024
}

const fn default_max_read_buffer_size() -> usize {
    64 * 1024
}

const fn default_max_write_buffer_size() -> usize {
    8 * 1024 * 1024
}

const fn default_idle_timeout_ms() -> u64 {
    30_000
}

const fn default_drain_timeout_ms() -> u64 {
    10_000
}

const fn default_proxy_connect_timeout_ms() -> u64 {
    3_000
}

const fn default_proxy_write_timeout_ms() -> u64 {
    30_000
}

const fn default_proxy_read_timeout_ms() -> u64 {
    30_000
}

const fn default_dns_resolver_threads() -> usize {
    2
}

const fn default_dns_timeout_ms() -> u64 {
    3_000
}

fn deserialize_positive_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom("worker limit must be greater than zero"));
    }
    Ok(value)
}

fn deserialize_positive_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom("worker timeout must be greater than zero"));
    }
    Ok(value)
}

fn deserialize_runtime_positive_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = usize::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom(
            "runtime queue and buffer sizes must be greater than zero",
        ));
    }
    Ok(value)
}

fn deserialize_runtime_positive_u32<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u32::deserialize(deserializer)?;
    if value == 0 {
        return Err(D::Error::custom(
            "io_uring SQ/CQ entry counts must be greater than zero",
        ));
    }
    Ok(value)
}

fn deserialize_buf_ring_size<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u32::deserialize(deserializer)?;
    if value == 0 || value > 32_768 || !value.is_power_of_two() {
        return Err(D::Error::custom(
            "io_uring buf_ring_size must be a power of two between 1 and 32768",
        ));
    }
    Ok(value)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    #[serde(default)]
    pub server_name: Option<String>,
    pub locations: Vec<Location>,
    pub listen: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Location {
    pub matcher: PathMatcher,
    pub action: Action,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Action {
    Proxy {
        #[serde(default)]
        upstream: Option<String>,
        #[serde(default)]
        upstreams: Vec<UpstreamEndpoint>,
        #[serde(default)]
        policy: BalancePolicy,
    },
    Static {
        directory: String,
    },
    Response {
        status: u16,
        body: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BalancePolicy {
    RoundRobin,
    #[default]
    WeightedRoundRobin,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpstreamEndpoint {
    pub url: String,
    #[serde(
        default = "default_upstream_weight",
        deserialize_with = "deserialize_upstream_weight"
    )]
    pub weight: u32,
}

const fn default_upstream_weight() -> u32 {
    1
}

fn deserialize_upstream_weight<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let weight = u32::deserialize(deserializer)?;
    if weight == 0 {
        Err(D::Error::custom(
            "upstream weight must be greater than zero",
        ))
    } else {
        Ok(weight)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PathMatcher {
    Exact { path: String },
    Prefix { path: String },
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http: HttpConfig {
                servers: vec![Server {
                    server_name: None,
                    locations: Vec::new(),
                    listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8089),
                }],
            },
            runtime: AsyncRuntimeConfig::default(),
            worker_count: default_worker_count(),
            worker: WorkerConfig::default(),
            proxy: ProxyTimeoutConfig::default(),
            dns: DnsConfig::default(),
        }
    }
}

impl Config {
    pub fn from_toml(contents: &str) -> Result<Self, toml::de::Error> {
        let config: Self = toml::from_str(contents)?;
        config.validate().map_err(toml::de::Error::custom)?;
        Ok(config)
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    fn validate(&self) -> Result<(), String> {
        for server in &self.http.servers {
            for location in &server.locations {
                if let Action::Proxy {
                    upstream,
                    upstreams,
                    ..
                } = &location.action
                {
                    if upstream.is_some() == !upstreams.is_empty() {
                        return Err(
                            "proxy action must configure exactly one of upstream or upstreams"
                                .to_owned(),
                        );
                    }
                    if upstream.as_ref().is_some_and(|url| url.trim().is_empty())
                        || upstreams
                            .iter()
                            .any(|endpoint| endpoint.url.trim().is_empty())
                    {
                        return Err("proxy upstream URL must not be empty".to_owned());
                    }
                    upstreams.iter().try_fold(0_u64, |total, endpoint| {
                        total
                            .checked_add(u64::from(endpoint.weight))
                            .ok_or_else(|| "proxy upstream weights are too large".to_owned())
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file")]
    Io(#[from] std::io::Error),

    #[error("failed to parse config file")]
    Parse(#[from] toml::de::Error),
}

pub fn read_config(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let contents = fs::read_to_string(path)?;
    Ok(Config::from_toml(&contents)?)
}

/// Compatibility alias for code using the previous configuration name.
pub type ProxyConfig = Config;
