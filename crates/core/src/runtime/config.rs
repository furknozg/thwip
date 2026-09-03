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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub servers: Vec<Server>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AsyncRuntimeConfig {
    Epoll {
        #[serde(default = "default_epoll_max_events")]
        max_events: usize,
    },

    Kqueue {
        #[serde(default = "default_epoll_max_events")]
        max_events: usize,
    },

    IoUring {
        #[serde(default = "default_sq_entries")]
        sq_entries: u32,
        #[serde(default = "default_cq_entries")]
        cq_entries: u32,
        #[serde(default = "default_buf_ring_size")]
        buf_ring_size: u32,
        #[serde(default = "default_buf_size")]
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
    Proxy { upstream: String },
    Static { directory: String },
    Response { status: u16, body: String },
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
        }
    }
}

impl Config {
    pub fn from_toml(contents: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(contents)
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
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
    Ok(toml::from_str(&contents)?)
}

/// Compatibility alias for code using the previous configuration name.
pub type ProxyConfig = Config;
