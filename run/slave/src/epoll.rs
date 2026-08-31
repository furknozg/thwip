#[cfg(target_os = "linux")]
use crate::{parse_request_head, RequestHeadParse};
#[cfg(target_os = "linux")]
use mio::{net::TcpListener, net::TcpStream, Events, Interest, Poll, Token};
#[cfg(target_os = "linux")]
use slab::Slab;
use std::{io, net::TcpListener as StdTcpListener};

#[cfg(target_os = "linux")]
struct Connection {
    socket: TcpStream,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    request_head_complete: bool,
}

#[cfg(target_os = "linux")]
struct EpollWorker {
    poll: Poll,
    listeners: Vec<TcpListener>,
    connections: Slab<Connection>,
}

#[cfg(target_os = "linux")]
impl EpollWorker {
    fn new(listeners: Vec<StdTcpListener>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let mut listeners: Vec<TcpListener> =
            listeners.into_iter().map(TcpListener::from_std).collect();

        for (index, listener) in listeners.iter_mut().enumerate() {
            poll.registry()
                .register(listener, Token(index), Interest::READABLE)?;
        }

        Ok(Self {
            poll,
            listeners,
            connections: Slab::new(),
        })
    }

    fn run(mut self, max_events: usize) -> io::Result<()> {
        let mut events = Events::with_capacity(max_events.max(1));

        loop {
            self.poll.poll(&mut events, None)?;
            let ready: Vec<(Token, bool)> = events
                .iter()
                .map(|event| (event.token(), event.is_readable()))
                .collect();

            for (token, readable) in ready {
                if !readable {
                    continue;
                }

                if token.0 < self.listeners.len() {
                    self.accept_ready(token.0)?;
                } else {
                    self.connection_ready(token.0 - self.listeners.len())?;
                }
            }
        }
    }

    fn accept_ready(&mut self, listener_index: usize) -> io::Result<()> {
        loop {
            match self.listeners[listener_index].accept() {
                Ok((stream, peer_address)) => {
                    println!("accepted connection from {peer_address}");
                    self.register_connection(stream)?;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    eprintln!("accept failed on listener {listener_index}: {error}");
                    return Ok(());
                }
            }
        }
    }

    fn register_connection(&mut self, stream: TcpStream) -> io::Result<()> {
        let connection_id = self.connections.insert(Connection {
            socket: stream,
            read_buffer: Vec::with_capacity(8 * 1024),
            write_buffer: Vec::new(),
            request_head_complete: false,
        });
        let token = Token(self.listeners.len() + connection_id);

        self.poll.registry().register(
            &mut self.connections[connection_id].socket,
            token,
            Interest::READABLE,
        )
    }

    fn connection_ready(&mut self, connection_id: usize) -> io::Result<()> {
        if !self.connections.contains(connection_id) {
            return Ok(());
        }

        let mut close_connection = false;
        {
            use std::io::Read;

            let connection = &mut self.connections[connection_id];
            let mut buffer = [0_u8; 8 * 1024];

            loop {
                match connection.socket.read(&mut buffer) {
                    Ok(0) => {
                        close_connection = true;
                        break;
                    }
                    Ok(read) => {
                        connection.read_buffer.extend_from_slice(&buffer[..read]);

                        if !connection.request_head_complete {
                            match parse_request_head(&connection.read_buffer) {
                                Ok(RequestHeadParse::Incomplete) => {}
                                Ok(RequestHeadParse::Complete { request, .. }) => {
                                    connection.request_head_complete = true;
                                    println!("{} {}", request.method, request.target);
                                }
                                Err(error) => {
                                    eprintln!("invalid HTTP request: {error}");
                                    close_connection = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        eprintln!("connection read failed: {error}");
                        close_connection = true;
                        break;
                    }
                }
            }
        }

        if close_connection {
            let mut connection = self.connections.remove(connection_id);
            self.poll.registry().deregister(&mut connection.socket)?;
        }

        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn run_epoll(listeners: Vec<StdTcpListener>, max_events: usize) -> io::Result<()> {
    EpollWorker::new(listeners)?.run(max_events)
}

#[cfg(not(target_os = "linux"))]
pub fn run_epoll(_listeners: Vec<StdTcpListener>, _max_events: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "epoll is only supported on Linux",
    ))
}
