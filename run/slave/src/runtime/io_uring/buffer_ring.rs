use io_uring::{cqueue, squeue, types::BufRingEntry, IoUring};
use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    io,
    ptr::NonNull,
    sync::atomic::{fence, Ordering},
};

pub(super) const BUFFER_GROUP: u16 = 0;

/// Owns the shared receive buffers and the kernel-visible descriptor ring.
/// A CQE lends one buffer to the worker; `copy_and_release` copies its bytes
/// into connection-owned state and publishes the descriptor back to the ring.
pub(super) struct ProvidedBufferRing {
    entries: NonNull<BufRingEntry>,
    layout: Layout,
    buffers: Vec<Box<[u8]>>,
    mask: u16,
    tail: u16,
    buffer_len: u32,
}

impl ProvidedBufferRing {
    pub(super) fn register(
        ring: &IoUring<squeue::Entry, cqueue::Entry>,
        entry_count: u32,
        buffer_size: usize,
    ) -> io::Result<Self> {
        if entry_count == 0 || !entry_count.is_power_of_two() || entry_count > 32_768 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring buf_ring_size must be a power of two between 1 and 32768",
            ));
        }
        let entry_count = u16::try_from(entry_count).unwrap();
        let buffer_len = u32::try_from(buffer_size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring buf_size is too large",
            )
        })?;
        if buffer_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "io_uring buf_size must be greater than zero",
            ));
        }

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::last_os_error());
        }
        let bytes = usize::from(entry_count)
            .checked_mul(std::mem::size_of::<BufRingEntry>())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "buffer ring is too large")
            })?;
        let layout = Layout::from_size_align(bytes, page_size as usize).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid buffer ring layout")
        })?;
        let entries = NonNull::new(unsafe { alloc_zeroed(layout) }.cast::<BufRingEntry>())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "buffer ring allocation failed")
            })?;

        let buffers: Vec<Box<[u8]>> = (0..entry_count)
            .map(|_| vec![0; buffer_size].into_boxed_slice())
            .collect();
        let mut pool = Self {
            entries,
            layout,
            buffers,
            mask: entry_count - 1,
            tail: 0,
            buffer_len,
        };
        for id in 0..entry_count {
            pool.publish(id);
        }
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                pool.entries.as_ptr() as u64,
                entry_count,
                BUFFER_GROUP,
                0,
            )?;
        }
        Ok(pool)
    }

    pub(super) fn copy_and_release(&mut self, id: u16, length: usize) -> io::Result<Vec<u8>> {
        let buffer = self.buffers.get(usize::from(id)).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "kernel selected an invalid buffer ID",
            )
        })?;
        if length > buffer.len() {
            self.publish(id);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "receive completion exceeds its selected buffer",
            ));
        }
        let bytes = buffer[..length].to_vec();
        self.publish(id);
        Ok(bytes)
    }

    fn publish(&mut self, id: u16) {
        let index = self.tail & self.mask;
        let entry = unsafe { &mut *self.entries.as_ptr().add(usize::from(index)) };
        entry.set_addr(self.buffers[usize::from(id)].as_ptr() as u64);
        entry.set_len(self.buffer_len);
        entry.set_bid(id);
        self.tail = self.tail.wrapping_add(1);
        fence(Ordering::Release);
        unsafe {
            std::ptr::write_volatile(
                BufRingEntry::tail(self.entries.as_ptr()).cast_mut(),
                self.tail,
            );
        }
    }
}

impl Drop for ProvidedBufferRing {
    fn drop(&mut self) {
        unsafe { dealloc(self.entries.as_ptr().cast(), self.layout) };
    }
}
