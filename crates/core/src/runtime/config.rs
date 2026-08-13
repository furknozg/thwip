use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProxyConfig {
    pub server: ServerConfig,
    pub buffer: IoUringConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub worker_count: Option<usize>, // None ise sistemdeki CPU sayısı otomatik alınır
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IoUringConfig {
    pub sq_entries: u32,  // Örn: 1024 (Güçlü bir ikili olmalı)
    pub cq_entries: u32,  // Örn: 2048 (SQ'nun en az 2 katı)
    pub buf_ring_size: u32, // Provided buffer sayısı (Örn: 4096)
    pub buf_size: usize,   // Her bir buffer'ın byte boyutu (Örn: 4096 veya 8192)
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                worker_count: None, // Tüm CPU çekirdeklerini kullan
            },
            buffer: IoUringConfig {
                sq_entries: 2048,
                cq_entries: 4096, // SQ * 2 kuralı
                buf_ring_size: 8192,
                buf_size: 4096, // 4KB standart TCP paket boyutu/sayfası
            },
        }
    }
}

impl ProxyConfig {
    /// TOML formatındaki bir metinden konfigürasyonu deserialize eder
    pub fn from_toml(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    /// Mevcut konfigürasyonu TOML metnine dönüştürür (Serialize)
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}
