use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub http: HttpConfig,

    #[serde(default)]
    pub io_uring: IoUringConfig,

    /// Defaults to one worker per logical CPU available to this process.
    #[serde(default = "default_worker_count")]
    pub worker_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub servers: Vec<Server>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    #[serde(default)]
    pub server_name: Option<String>,
    pub locations: Vec<Location>,
    pub listen: u16,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoUringConfig {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub buf_ring_size: u32,
    pub buf_size: usize,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            sq_entries: 4096,
            cq_entries: 8192,
            buf_ring_size: 16384,
            buf_size: 8192,
        }
    }
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
                    listen: 8080,
                }],
            },
            io_uring: IoUringConfig::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_and_io_uring_config() {
        let config = Config::from_toml(
            r#"
worker_count = 4

[io_uring]
sq_entries = 1024
cq_entries = 2048
buf_ring_size = 4096
buf_size = 8192

[[http.servers]]
server_name = "example.com"
listen = 8080

[[http.servers.locations]]
matcher = { type = "prefix", path = "/api" }
action = { type = "proxy", upstream = "http://127.0.0.1:3000" }
"#,
        )
        .expect("configuration should parse");

        assert_eq!(config.worker_count, 4);
        assert_eq!(config.io_uring.sq_entries, 1024);
        assert_eq!(config.http.servers[0].listen, 8080);
        assert!(matches!(
            config.http.servers[0].locations[0].matcher,
            PathMatcher::Prefix { ref path } if path == "/api"
        ));
    }

    #[test]
    fn io_uring_defaults_when_section_is_omitted() {
        let config = Config::from_toml(
            r#"
[http]
servers = []
"#,
        )
        .expect("configuration should parse");

        assert_eq!(config.worker_count, default_worker_count());
        assert_eq!(config.io_uring.sq_entries, 4096);
        assert_eq!(config.io_uring.cq_entries, 8192);
        assert_eq!(config.io_uring.buf_ring_size, 16384);
        assert_eq!(config.io_uring.buf_size, 8192);
    }

    #[test]
    fn repository_config_parses() {
        let config = Config::from_toml(include_str!("../../../../rginx.conf"))
            .expect("rginx.conf should parse");

        assert_eq!(config.worker_count, 12);
        assert_eq!(config.http.servers.len(), 2);
        assert_eq!(config.http.servers[0].listen, 8089);
        assert_eq!(config.http.servers[0].locations.len(), 3);
        assert_eq!(config.http.servers[1].locations.len(), 3);
        assert_eq!(config.io_uring.sq_entries, 4096);
    }
}
