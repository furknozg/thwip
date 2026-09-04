#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperationKind {
    Accept = 1,
    Read = 2,
    Write = 3,
    ProxyConnect = 4,
    ProxyWrite = 5,
    ProxyRead = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OperationId {
    pub(super) slot: u32,
    pub(super) generation: u16,
    pub(super) kind: OperationKind,
}

pub const CONTROL_USER_DATA: u64 = 0;
pub const CANCEL_USER_DATA: u64 = u64::MAX;
pub const DNS_USER_DATA: u64 = u64::MAX - 1;
pub const TIMER_USER_DATA: u64 = u64::MAX - 2;
pub const PROXY_CANCEL_USER_DATA: u64 = u64::MAX - 3;

impl OperationId {
    /// Encode this operation into the `user_data` field carried by an SQE/CQE.
    pub fn encode(self) -> u64 {
        ((self.kind as u64) << 48) | ((self.generation as u64) << 32) | self.slot as u64
    }

    /// Decode completion `user_data`, rejecting reserved or unknown values.
    pub fn decode(value: u64) -> Option<Self> {
        let kind = match (value >> 48) as u16 {
            1 => OperationKind::Accept,
            2 => OperationKind::Read,
            3 => OperationKind::Write,
            4 => OperationKind::ProxyConnect,
            5 => OperationKind::ProxyWrite,
            6 => OperationKind::ProxyRead,
            _ => return None,
        };

        let generation = ((value >> 32) & 0xffff) as u16;
        if generation == 0 {
            return None;
        }

        Some(Self {
            slot: value as u32,
            generation,
            kind,
        })
    }

    pub const fn accept(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::Accept,
        }
    }

    pub const fn read(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::Read,
        }
    }

    pub const fn write(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::Write,
        }
    }

    pub const fn proxy_connect(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::ProxyConnect,
        }
    }

    pub const fn proxy_write(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::ProxyWrite,
        }
    }

    pub const fn proxy_read(slot: u32, generation: u16) -> Self {
        Self {
            slot,
            generation,
            kind: OperationKind::ProxyRead,
        }
    }
}
