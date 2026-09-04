use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{
    collections::HashMap,
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

    #[serde(default)]
    pub upstreams: HashMap<String, UpstreamGroup>,
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
    #[serde(default)]
    pub ssl: Option<SslServerConfig>,
}

/// SSL/TLS termination settings for one client-facing server.
///
/// The public configuration name uses the familiar `ssl` term. The wire
/// protocol is TLS; SSLv2 and SSLv3 are never supported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SslServerConfig {
    pub certificate_path: String,
    pub private_key_path: String,
    #[serde(
        default = "default_tls_handshake_timeout_ms",
        deserialize_with = "deserialize_positive_u64"
    )]
    pub handshake_timeout_ms: u64,
    #[serde(default = "default_ssl_protocols")]
    pub protocols: Vec<SslProtocol>,
    #[serde(default = "default_ssl_ciphers")]
    pub ciphers: Vec<SslCipher>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SslProtocol {
    Tlsv1_2,
    Tlsv1_3,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SslCipher {
    #[serde(rename = "tls13_aes_256_gcm_sha384")]
    Tls13Aes256GcmSha384,
    #[serde(rename = "tls13_aes_128_gcm_sha256")]
    Tls13Aes128GcmSha256,
    #[serde(rename = "tls13_chacha20_poly1305_sha256")]
    Tls13Chacha20Poly1305Sha256,
    #[serde(rename = "tls_ecdhe_ecdsa_with_aes_256_gcm_sha384")]
    TlsEcdheEcdsaWithAes256GcmSha384,
    #[serde(rename = "tls_ecdhe_ecdsa_with_aes_128_gcm_sha256")]
    TlsEcdheEcdsaWithAes128GcmSha256,
    #[serde(rename = "tls_ecdhe_ecdsa_with_chacha20_poly1305_sha256")]
    TlsEcdheEcdsaWithChacha20Poly1305Sha256,
    #[serde(rename = "tls_ecdhe_rsa_with_aes_256_gcm_sha384")]
    TlsEcdheRsaWithAes256GcmSha384,
    #[serde(rename = "tls_ecdhe_rsa_with_aes_128_gcm_sha256")]
    TlsEcdheRsaWithAes128GcmSha256,
    #[serde(rename = "tls_ecdhe_rsa_with_chacha20_poly1305_sha256")]
    TlsEcdheRsaWithChacha20Poly1305Sha256,
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
        upstream_group: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpstreamGroup {
    #[serde(default)]
    pub policy: BalancePolicy,
    pub servers: Vec<UpstreamEndpoint>,
}

const fn default_upstream_weight() -> u32 {
    1
}

const fn default_tls_handshake_timeout_ms() -> u64 {
    10_000
}

fn default_ssl_protocols() -> Vec<SslProtocol> {
    vec![SslProtocol::Tlsv1_2, SslProtocol::Tlsv1_3]
}

fn default_ssl_ciphers() -> Vec<SslCipher> {
    vec![
        SslCipher::Tls13Aes256GcmSha384,
        SslCipher::Tls13Aes128GcmSha256,
        SslCipher::Tls13Chacha20Poly1305Sha256,
        SslCipher::TlsEcdheEcdsaWithAes256GcmSha384,
        SslCipher::TlsEcdheEcdsaWithAes128GcmSha256,
        SslCipher::TlsEcdheEcdsaWithChacha20Poly1305Sha256,
        SslCipher::TlsEcdheRsaWithAes256GcmSha384,
        SslCipher::TlsEcdheRsaWithAes128GcmSha256,
        SslCipher::TlsEcdheRsaWithChacha20Poly1305Sha256,
    ]
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
                    ssl: None,
                }],
            },
            runtime: AsyncRuntimeConfig::default(),
            worker_count: default_worker_count(),
            worker: WorkerConfig::default(),
            proxy: ProxyTimeoutConfig::default(),
            dns: DnsConfig::default(),
            upstreams: HashMap::new(),
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
        for (name, group) in &self.upstreams {
            if name.trim().is_empty() {
                return Err("upstream group name must not be empty".to_owned());
            }
            if group.servers.is_empty() {
                return Err(format!(
                    "upstream group {name:?} must contain at least one server"
                ));
            }
            validate_endpoints(&group.servers)?;
        }
        for server in &self.http.servers {
            if let Some(ssl) = &server.ssl {
                if ssl.certificate_path.trim().is_empty() {
                    return Err("SSL certificate_path must not be empty".to_owned());
                }
                if ssl.private_key_path.trim().is_empty() {
                    return Err("SSL private_key_path must not be empty".to_owned());
                }
                if ssl.protocols.is_empty() {
                    return Err("SSL protocols must not be empty".to_owned());
                }
                if ssl.ciphers.is_empty() {
                    return Err("SSL ciphers must not be empty".to_owned());
                }
            }
            for location in &server.locations {
                if let Action::Proxy {
                    upstream,
                    upstream_group,
                    upstreams,
                    ..
                } = &location.action
                {
                    let configured = usize::from(upstream.is_some())
                        + usize::from(upstream_group.is_some())
                        + usize::from(!upstreams.is_empty());
                    if configured != 1 {
                        return Err(
                            "proxy action must configure exactly one of upstream, upstream_group, or upstreams".to_owned(),
                        );
                    }
                    if upstream.as_ref().is_some_and(|url| url.trim().is_empty())
                        || upstreams
                            .iter()
                            .any(|endpoint| endpoint.url.trim().is_empty())
                    {
                        return Err("proxy upstream URL must not be empty".to_owned());
                    }
                    validate_endpoints(upstreams)?;
                    if let Some(name) = upstream_group
                        && !self.upstreams.contains_key(name)
                    {
                        return Err(format!("proxy references unknown upstream group {name:?}"));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_endpoints(endpoints: &[UpstreamEndpoint]) -> Result<(), String> {
    if endpoints
        .iter()
        .any(|endpoint| endpoint.url.trim().is_empty())
    {
        return Err("proxy upstream URL must not be empty".to_owned());
    }
    endpoints.iter().try_fold(0_u64, |total, endpoint| {
        total
            .checked_add(u64::from(endpoint.weight))
            .ok_or_else(|| "proxy upstream weights are too large".to_owned())
    })?;
    Ok(())
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
