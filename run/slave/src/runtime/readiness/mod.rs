//! Shared readiness runtime used by epoll on Linux and kqueue on macOS/BSD.

mod connection;
mod proxy;
mod token;
mod worker;

pub(crate) use worker::run;
