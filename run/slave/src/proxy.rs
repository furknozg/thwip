use crate::RequestHead;
use std::{collections::HashSet, fmt};

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

    pub(crate) fn connect_address(&self) -> &str {
        &self.connect_address
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
