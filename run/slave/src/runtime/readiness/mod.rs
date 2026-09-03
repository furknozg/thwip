//! Shared readiness runtime used by epoll on Linux and kqueue on macOS/BSD.

mod connection;
mod proxy;
mod resolver;
mod token;
mod worker;

#[cfg(test)]
mod tests;

pub(crate) use worker::run;
