use std::{io, net::SocketAddr};

pub trait Listener {
    fn bind(address: SocketAddr) -> io::Result<Self>
    where
        Self: Sized;

    fn local_addr(&self) -> io::Result<SocketAddr>;

    async fn accept(&mut self) -> std::io::Result<(Self::Connection, std::net::SocketAddr)>;
}
