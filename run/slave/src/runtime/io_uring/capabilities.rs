use io_uring::{cqueue, opcode, squeue, IoUring, Probe};
use std::io;

pub(super) struct Capabilities {
    accept: bool,
    connect: bool,
    recv: bool,
    send: bool,
    read: bool,
    async_cancel: bool,
}

impl Capabilities {
    pub(super) fn probe(ring: &IoUring<squeue::Entry, cqueue::Entry>) -> io::Result<Self> {
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        Ok(Self {
            accept: probe.is_supported(opcode::Accept::CODE),
            connect: probe.is_supported(opcode::Connect::CODE),
            recv: probe.is_supported(opcode::Recv::CODE),
            send: probe.is_supported(opcode::Send::CODE),
            read: probe.is_supported(opcode::Read::CODE),
            async_cancel: probe.is_supported(opcode::AsyncCancel::CODE),
        })
    }

    pub(super) fn validate_required(&self) -> io::Result<()> {
        let required = [
            ("accept", self.accept),
            ("connect", self.connect),
            ("recv", self.recv),
            ("send", self.send),
            ("read", self.read),
            ("async_cancel", self.async_cancel),
        ];
        let missing: Vec<&str> = required
            .into_iter()
            .filter_map(|(name, supported)| (!supported).then_some(name))
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "io_uring is missing required operation(s): {}",
                    missing.join(", ")
                ),
            ))
        }
    }
}
