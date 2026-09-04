use crate::{LoadedSslConfig, RequestHead, StaticStream};
use socket2::SockAddr;
use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    os::fd::OwnedFd,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct UringSslSession {
    pub(super) connection: rustls::ServerConnection,
    pub(super) started_at: Instant,
    pub(super) handshake_timeout: Duration,
    pub(super) closing: bool,
}

pub(super) struct UringConnection {
    pub(super) socket: OwnedFd,
    pub(super) generation: u16,
    pub(super) listener_index: usize,
    pub(super) read_pending: bool,
    pub(super) request_buffer: Vec<u8>,
    pub(super) pending_request: Option<PendingRequest>,
    pub(super) request: Option<CompletedRequest>,
    pub(super) write_buffer: Vec<u8>,
    pub(super) write_offset: usize,
    pub(super) write_pending: bool,
    pub(super) ssl_write_buffer: Vec<u8>,
    pub(super) ssl_write_offset: usize,
    pub(super) proxy: Option<UringProxy>,
    pub(super) static_stream: Option<StaticStream>,
    pub(super) ssl: Option<UringSslSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProxyPhase {
    Resolving,
    Connecting,
    WritingRequest,
    ReadingResponse,
}

impl ProxyPhase {
    pub(super) const fn timeout_message(self) -> &'static str {
        match self {
            Self::Resolving => "upstream DNS resolution timed out",
            Self::Connecting => "upstream connect timed out",
            Self::WritingRequest => "upstream request write timed out",
            Self::ReadingResponse => "upstream response timed out",
        }
    }
}

pub(super) struct UringProxy {
    pub(super) upstream: Option<OwnedFd>,
    pub(super) address: Option<Box<SockAddr>>,
    pub(super) addresses: VecDeque<SocketAddr>,
    pub(super) request_buffer: Vec<u8>,
    pub(super) request_offset: usize,
    pub(super) response_started: bool,
    pub(super) phase: ProxyPhase,
    pub(super) progress_at: Instant,
    pub(super) operation_pending: bool,
    pub(super) timed_out: bool,
}

impl UringProxy {
    pub(super) fn resolving(request_buffer: Vec<u8>) -> Self {
        Self {
            upstream: None,
            address: None,
            addresses: VecDeque::new(),
            request_buffer,
            request_offset: 0,
            response_started: false,
            phase: ProxyPhase::Resolving,
            progress_at: Instant::now(),
            operation_pending: false,
            timed_out: false,
        }
    }

    pub(super) fn transition(&mut self, phase: ProxyPhase) {
        self.phase = phase;
        self.progress_at = Instant::now();
        self.timed_out = false;
    }

    pub(super) fn record_progress(&mut self) {
        self.progress_at = Instant::now();
    }
}

pub(super) struct PendingRequest {
    pub(super) head: RequestHead,
    pub(super) body_start: usize,
    pub(super) body_end: usize,
}

pub(super) struct CompletedRequest {
    pub(super) head: RequestHead,
    pub(super) body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConnectionId {
    pub(super) slot: u32,
    pub(super) generation: u16,
}

impl UringConnection {
    pub(super) fn new(
        socket: OwnedFd,
        generation: u16,
        listener_index: usize,
        buffer_size: usize,
        ssl: Option<&LoadedSslConfig>,
    ) -> io::Result<Self> {
        let ssl = ssl
            .map(|config| {
                rustls::ServerConnection::new(Arc::clone(&config.server_config)).map(|connection| {
                    UringSslSession {
                        connection,
                        started_at: Instant::now(),
                        handshake_timeout: config.handshake_timeout,
                        closing: false,
                    }
                })
            })
            .transpose()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("failed to create SSL session: {error}"),
                )
            })?;
        Ok(Self {
            socket,
            generation,
            listener_index,
            read_pending: false,
            request_buffer: Vec::with_capacity(buffer_size),
            pending_request: None,
            request: None,
            write_buffer: Vec::new(),
            write_offset: 0,
            write_pending: false,
            ssl_write_buffer: Vec::new(),
            ssl_write_offset: 0,
            proxy: None,
            static_stream: None,
            ssl,
        })
    }

    pub(super) fn id(&self, slot: u32) -> ConnectionId {
        ConnectionId {
            slot,
            generation: self.generation,
        }
    }

    pub(super) fn matches_generation(&self, generation: u16) -> bool {
        self.generation == generation
    }

    pub(super) fn mark_read_submitted(&mut self) {
        self.read_pending = true;
    }

    pub(super) fn mark_read_completed(&mut self) {
        self.read_pending = false;
    }

    pub(super) fn queue_response(&mut self, response: Vec<u8>) {
        self.write_buffer = response;
        self.write_offset = 0;
    }

    pub(super) fn mark_write_submitted(&mut self) {
        self.write_pending = true;
    }

    pub(super) fn mark_write_completed(&mut self) {
        self.write_pending = false;
    }
}

pub(super) fn next_generation(previous: u16) -> u16 {
    let next = previous.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}
