#![cfg(unix)]

use super::proxy::ProxyState;
use crate::RequestHead;
use mio::net::TcpStream;
use std::time::Instant;

/// A connection is always in exactly one protocol phase. Keeping the upstream
/// state inside `Proxying` prevents contradictory combinations of flags.
pub(super) enum ConnectionPhase {
    Reading {
        pending_request: Option<PendingRequest>,
    },
    WritingResponse,
    Resolving(ResolvingState),
    Proxying(ProxyState),
}

pub(super) struct ResolvingState {
    pub(super) request_buffer: Vec<u8>,
    pub(super) started_at: Instant,
}

pub(super) struct PendingRequest {
    pub(super) head: RequestHead,
    pub(super) body_start: usize,
    pub(super) body_end: usize,
}

pub(super) struct Connection {
    pub(super) socket: TcpStream,
    pub(super) read_buffer: Vec<u8>,
    pub(super) write_buffer: Vec<u8>,
    pub(super) write_offset: usize,
    pub(super) listener_group: usize,
    pub(super) last_progress: Instant,
    pub(super) generation: usize,
    phase: ConnectionPhase,
}

impl Connection {
    pub(super) fn new(socket: TcpStream, listener_group: usize, generation: usize) -> Self {
        Self {
            socket,
            read_buffer: Vec::with_capacity(8 * 1024),
            write_buffer: Vec::new(),
            write_offset: 0,
            listener_group,
            last_progress: Instant::now(),
            generation,
            phase: ConnectionPhase::Reading {
                pending_request: None,
            },
        }
    }

    pub(super) fn is_proxying(&self) -> bool {
        matches!(self.phase, ConnectionPhase::Proxying(_))
    }

    pub(super) fn is_resolving(&self) -> bool {
        matches!(self.phase, ConnectionPhase::Resolving(_))
    }

    pub(super) fn is_handling_request(&self) -> bool {
        matches!(
            self.phase,
            ConnectionPhase::Resolving(_) | ConnectionPhase::Proxying(_)
        )
    }

    pub(super) fn pending_request(&self) -> Option<&PendingRequest> {
        match &self.phase {
            ConnectionPhase::Reading { pending_request } => pending_request.as_ref(),
            _ => None,
        }
    }

    pub(super) fn set_pending_request(&mut self, request: PendingRequest) {
        if let ConnectionPhase::Reading { pending_request } = &mut self.phase {
            *pending_request = Some(request);
        }
    }

    pub(super) fn take_pending_request(&mut self) -> Option<PendingRequest> {
        match &mut self.phase {
            ConnectionPhase::Reading { pending_request } => pending_request.take(),
            _ => None,
        }
    }

    pub(super) fn begin_response(&mut self) {
        self.phase = ConnectionPhase::WritingResponse;
    }

    pub(super) fn begin_resolving(&mut self, request_buffer: Vec<u8>) {
        self.phase = ConnectionPhase::Resolving(ResolvingState {
            request_buffer,
            started_at: Instant::now(),
        });
    }

    pub(super) fn resolution(&self) -> Option<&ResolvingState> {
        match &self.phase {
            ConnectionPhase::Resolving(resolution) => Some(resolution),
            _ => None,
        }
    }

    pub(super) fn take_resolution(&mut self) -> Option<ResolvingState> {
        match std::mem::replace(&mut self.phase, ConnectionPhase::WritingResponse) {
            ConnectionPhase::Resolving(resolution) => Some(resolution),
            previous => {
                self.phase = previous;
                None
            }
        }
    }

    pub(super) fn begin_proxy(&mut self, proxy: ProxyState) {
        self.phase = ConnectionPhase::Proxying(proxy);
    }

    pub(super) fn proxy(&self) -> Option<&ProxyState> {
        match &self.phase {
            ConnectionPhase::Proxying(proxy) => Some(proxy),
            _ => None,
        }
    }

    pub(super) fn proxy_mut(&mut self) -> Option<&mut ProxyState> {
        match &mut self.phase {
            ConnectionPhase::Proxying(proxy) => Some(proxy),
            _ => None,
        }
    }

    pub(super) fn detach_proxy(&mut self) -> Option<ProxyState> {
        match std::mem::replace(&mut self.phase, ConnectionPhase::WritingResponse) {
            ConnectionPhase::Proxying(proxy) => Some(proxy),
            previous => {
                self.phase = previous;
                None
            }
        }
    }
}
