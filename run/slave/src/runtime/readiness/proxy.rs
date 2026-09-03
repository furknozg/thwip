#![cfg(unix)]

use mio::net::TcpStream;
use std::time::{Duration, Instant};

use super::super::ProxyLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProxyPhase {
    Connecting,
    WritingRequest,
    ReadingResponse,
}

impl ProxyPhase {
    pub(super) fn timeout(self, limits: ProxyLimits) -> Duration {
        match self {
            Self::Connecting => limits.connect_timeout,
            Self::WritingRequest => limits.write_timeout,
            Self::ReadingResponse => limits.read_timeout,
        }
    }

    pub(super) const fn timeout_message(self) -> &'static str {
        match self {
            Self::Connecting => "upstream connect timed out",
            Self::WritingRequest => "upstream request write timed out",
            Self::ReadingResponse => "upstream response timed out",
        }
    }
}

/// Owns the upstream half of one proxied client connection. Request and
/// response offsets stay here because they advance independently of the client.
pub(super) struct ProxyState {
    pub(super) upstream: TcpStream,
    pub(super) request_buffer: Vec<u8>,
    pub(super) request_offset: usize,
    pub(super) upstream_eof: bool,
    pub(super) response_started: bool,
    pub(super) phase: ProxyPhase,
    pub(super) phase_progress_at: Instant,
}

impl ProxyState {
    pub(super) fn new(upstream: TcpStream, request_buffer: Vec<u8>) -> Self {
        Self {
            upstream,
            request_buffer,
            request_offset: 0,
            upstream_eof: false,
            response_started: false,
            phase: ProxyPhase::Connecting,
            phase_progress_at: Instant::now(),
        }
    }

    /// Phase deadlines measure lack of progress, so every successful connect,
    /// send, or receive restarts the current stage's clock.
    pub(super) fn transition(&mut self, phase: ProxyPhase) {
        self.phase = phase;
        self.phase_progress_at = Instant::now();
    }

    pub(super) fn record_progress(&mut self) {
        self.phase_progress_at = Instant::now();
    }
}
