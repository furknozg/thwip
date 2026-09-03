#![cfg(unix)]

use mio::Token;
use std::io;

pub(super) const CONTROL_TOKEN: Token = Token(usize::MAX);
const CONNECTION_TAG: usize = 1 << (usize::BITS - 1);
const UPSTREAM_TAG: usize = 1 << (usize::BITS - 2);
const SLOT_BITS: u32 = usize::BITS / 2;
pub(super) const SLOT_MASK: usize = (1usize << SLOT_BITS) - 1;
const GENERATION_BITS: u32 = usize::BITS - SLOT_BITS - 2;
const GENERATION_MASK: usize = (1usize << GENERATION_BITS) - 1;

/// Stable identity for one occupation of a slab slot. The generation prevents
/// a delayed OS event from targeting a newer socket that reused the same slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ConnectionId {
    pub(super) slot: usize,
    pub(super) generation: usize,
}

/// One logical connection owns two independently registered sockets while it
/// is proxying, so the token must also identify which side became ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SocketRole {
    Client,
    Upstream,
}

impl ConnectionId {
    pub(super) fn token(self, role: SocketRole) -> io::Result<Token> {
        if self.slot > SLOT_MASK || self.generation == 0 || self.generation > GENERATION_MASK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection identifier cannot be encoded as a mio token",
            ));
        }
        let role_tag = match role {
            SocketRole::Client => 0,
            SocketRole::Upstream => UPSTREAM_TAG,
        };
        let token = Token(CONNECTION_TAG | role_tag | (self.generation << SLOT_BITS) | self.slot);
        if token == CONTROL_TOKEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection identifier collides with the control token",
            ));
        }
        Ok(token)
    }

    pub(super) fn from_token(token: Token) -> Option<(Self, SocketRole)> {
        if token == CONTROL_TOKEN || token.0 & CONNECTION_TAG == 0 {
            return None;
        }
        let generation = (token.0 >> SLOT_BITS) & GENERATION_MASK;
        let role = if token.0 & UPSTREAM_TAG == 0 {
            SocketRole::Client
        } else {
            SocketRole::Upstream
        };
        (generation != 0).then_some((
            Self {
                slot: token.0 & SLOT_MASK,
                generation,
            },
            role,
        ))
    }
}

pub(super) fn next_generation(previous: usize) -> usize {
    let next = previous.wrapping_add(1) & GENERATION_MASK;
    if next == 0 {
        1
    } else {
        next
    }
}
