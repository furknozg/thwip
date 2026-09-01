use serde::{Deserialize, Serialize};
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
