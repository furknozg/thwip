#![cfg(unix)]

use proxy_common::{Action, Location, PathMatcher, Server, SslServerConfig};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, RootCertStore, StreamOwned};
#[cfg(target_os = "linux")]
use slave::EpollRuntime;
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
use slave::KqueueRuntime;
use slave::{
    load_ssl_configs, BoundListenerGroup, DnsLimits, ProxyLimits, Runtime, ShutdownHandle,
    WorkerContext, WorkerLimits, WorkerMetrics,
};
use std::{
    fs::File,
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/config/ssl-test")
        .join(name)
}

#[test]
fn serves_https_requests_over_the_readiness_runtime() {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    let certificate_path = fixture_path("cert.pem");
    let private_key_path = fixture_path("key.pem");
    let server = Server {
        server_name: Some("localhost".to_owned()),
        listen: address,
        ssl: Some(SslServerConfig {
            certificate_path: certificate_path.to_string_lossy().into_owned(),
            private_key_path: private_key_path.to_string_lossy().into_owned(),
            handshake_timeout_ms: 2_000,
            protocols: vec![proxy_common::SslProtocol::Tlsv1_3],
            ciphers: vec![proxy_common::SslCipher::Tls13Aes256GcmSha384],
        }),
        locations: vec![Location {
            matcher: PathMatcher::Exact {
                path: "/health".to_owned(),
            },
            action: Action::Response {
                status: 200,
                body: "OK".to_owned(),
            },
        }],
    };
    let shutdown = ShutdownHandle::new();
    let worker_shutdown = shutdown.clone();
    let worker = thread::spawn(move || {
        let servers = vec![server];
        let context = WorkerContext {
            listener_groups: vec![BoundListenerGroup {
                socket,
                address,
                default_server: 0,
                server_indices: vec![0],
            }],
            ssl_configs: load_ssl_configs(&servers).unwrap(),
            servers,
            shutdown: worker_shutdown,
            limits: WorkerLimits::default(),
            proxy_limits: ProxyLimits::default(),
            dns_limits: DnsLimits::default(),
            metrics: WorkerMetrics::default(),
            upstream_groups: Default::default(),
        };
        #[cfg(target_os = "linux")]
        return EpollRuntime { max_events: 64 }.run(context);
        #[cfg(any(
            target_os = "macos",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "openbsd",
            target_os = "dragonfly"
        ))]
        return KqueueRuntime { max_events: 64 }.run(context);
        #[allow(unreachable_code)]
        Err(std::io::Error::other("unsupported test platform"))
    });

    let mut roots = RootCertStore::empty();
    let mut certificate = BufReader::new(File::open(certificate_path).unwrap());
    for certificate in rustls_pemfile::certs(&mut certificate) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connection = ClientConnection::new(
        Arc::new(config),
        ServerName::try_from("localhost").unwrap().to_owned(),
    )
    .unwrap();
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut stream = StreamOwned::new(connection, stream);
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.ends_with("\r\n\r\nOK"), "{response}");

    shutdown.request();
    worker.join().unwrap().unwrap();
}
