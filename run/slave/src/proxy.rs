use crate::RequestHead;
use std::{collections::HashSet, fmt, io, net::SocketAddr, net::ToSocketAddrs};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Upstream {
    authority: String,
    connect_address: String,
    base_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpstreamError {
    UnsupportedScheme,
    MissingHost,
    InvalidAuthority,
}

impl fmt::Display for UpstreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedScheme => formatter.write_str("only http upstreams are supported"),
            Self::MissingHost => formatter.write_str("upstream URL has no host"),
            Self::InvalidAuthority => formatter.write_str("upstream URL has an invalid authority"),
        }
    }
}

impl Upstream {
    pub(crate) fn parse(url: &str) -> Result<Self, UpstreamError> {
        let remainder = url
            .strip_prefix("http://")
            .ok_or(UpstreamError::UnsupportedScheme)?;
        let (authority, base_path) = remainder
            .split_once('/')
            .map_or((remainder, String::new()), |(authority, path)| {
                (authority, format!("/{path}"))
            });
        if authority.is_empty() {
            return Err(UpstreamError::MissingHost);
        }
        if authority.contains('@') || authority.contains(['?', '#']) {
            return Err(UpstreamError::InvalidAuthority);
        }

        let connect_address = if authority.starts_with('[') {
            let closing = authority.find(']').ok_or(UpstreamError::InvalidAuthority)?;
            if closing + 1 == authority.len() {
                format!("{authority}:80")
            } else if authority.as_bytes().get(closing + 1) == Some(&b':') {
                authority.to_owned()
            } else {
                return Err(UpstreamError::InvalidAuthority);
            }
        } else if authority.rsplit_once(':').is_some() {
            authority.to_owned()
        } else {
            format!("{authority}:80")
        };

        Ok(Self {
            authority: authority.to_owned(),
            connect_address,
            base_path,
        })
    }

    pub(crate) fn resolve(&self) -> io::Result<SocketAddr> {
        self.connect_address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::AddrNotAvailable, "upstream has no address")
            })
    }

    pub(crate) fn request_bytes(&self, request: &RequestHead, body: &[u8]) -> Vec<u8> {
        let connection_headers: HashSet<String> = request
            .headers
            .iter()
            .filter(|header| header.name == "connection")
            .flat_map(|header| header.value.split(','))
            .map(|name| name.trim().to_ascii_lowercase())
            .collect();
        let target = if self.base_path.is_empty() {
            request.target.clone()
        } else {
            format!(
                "{}{}",
                self.base_path.trim_end_matches('/'),
                request.target.as_str()
            )
        };
        let mut bytes = format!(
            "{} {target} HTTP/1.1\r\nHost: {}\r\n",
            request.method, self.authority
        )
        .into_bytes();

        for header in &request.headers {
            if matches!(
                header.name.as_str(),
                "host"
                    | "connection"
                    | "proxy-connection"
                    | "keep-alive"
                    | "transfer-encoding"
                    | "upgrade"
            ) || connection_headers.contains(&header.name)
            {
                continue;
            }
            bytes.extend_from_slice(header.name.as_bytes());
            bytes.extend_from_slice(b": ");
            bytes.extend_from_slice(header.value.as_bytes());
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(b"Connection: close\r\n\r\n");
        bytes.extend_from_slice(body);
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Header, HttpVersion};

    #[test]
    fn builds_close_delimited_upstream_request_and_removes_hop_by_hop_headers() {
        let upstream = Upstream::parse("http://127.0.0.1:8080/base").unwrap();
        let request = RequestHead {
            method: "POST".into(),
            target: "/items".into(),
            version: HttpVersion::Http11,
            headers: vec![
                Header {
                    name: "host".into(),
                    value: "public.test".into(),
                },
                Header {
                    name: "content-length".into(),
                    value: "2".into(),
                },
                Header {
                    name: "connection".into(),
                    value: "x-remove".into(),
                },
                Header {
                    name: "x-remove".into(),
                    value: "secret".into(),
                },
            ],
        };

        let bytes = String::from_utf8(upstream.request_bytes(&request, b"{}")).unwrap();
        assert!(bytes.starts_with("POST /base/items HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n"));
        assert!(bytes.contains("content-length: 2\r\n"));
        assert!(!bytes.contains("x-remove"));
        assert!(bytes.ends_with("Connection: close\r\n\r\n{}"));
    }
}
