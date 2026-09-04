---
name: thwip-config-migration
description: Migrate a supported reverse-proxy or web-server configuration to Thwip TOML, reporting every directive that cannot be represented safely.
---

# Thwip Configuration Migration

Convert a user-provided proxy configuration into a candidate Thwip TOML file.
Treat the source as the specification: preserve its routing intent where Thwip has an equivalent feature, and never silently ignore unsupported behavior.

## Output

Provide these three parts:

1. A candidate Thwip TOML configuration.
2. A short mapping summary describing source blocks and their Thwip destinations.
3. A **Migration warnings** section. Include every source directive that is not migrated exactly, why it cannot be migrated, and the safest next step.

Do not claim that the candidate is production-ready until warnings are resolved and the user has validated paths, addresses, certificates, and upstream reachability.

## Supported mappings

| Source intent | Thwip form |
| --- | --- |
| Listen address | `[[http.servers]]` with `listen = "IP:port"` |
| Host/server name | `server_name = "example.test"` |
| Exact route | `matcher = { type = "exact", path = "/health" }` |
| Prefix route | `matcher = { type = "prefix", path = "/api" }` |
| Fixed response | `action = { type = "response", status = 200, body = "..." }` |
| Static document root | `action = { type = "static", directory = "./public" }` |
| One HTTP upstream | `action = { type = "proxy", upstream = "http://127.0.0.1:9000" }` |
| Reusable upstream pool | `[upstreams.name]` plus `upstream_group = "name"` |
| Round robin | `policy = "round_robin"` |
| Weighted round robin | `policy = "weighted_round_robin"` with positive endpoint weights |

Use the longest-prefix and exact-match semantics correctly: exact routes win, then the longest matching prefix wins. When source order would change behavior, explain the difference in Migration warnings.

## Current Thwip configuration schema

Use this schema when producing the candidate file. Do not invent keys outside it.

| Root key | Required | Current purpose |
| --- | --- | --- |
| `http` | Yes | Defines `http.servers`. |
| `runtime` | No | Selects the socket runtime; omission defaults to `epoll`. |
| `worker_count` | No | Worker process count; defaults to logical CPU count. |
| `worker` | No | Per-worker connection, buffer, and shutdown limits. |
| `proxy` | No | Upstream connect, write, and read timeouts. |
| `dns` | No | Background hostname-resolution settings. |
| `upstreams` | No | Reusable named upstream groups. |

### Runtime choice

For a portable migrated configuration, prefer:

```toml
[runtime]
type = "auto"
```

`auto` selects io_uring on Linux only after the required capabilities and provided buffers are successfully probed. It falls back to epoll on unsupported Linux hosts and selects kqueue on macOS/BSD.

Use an explicit runtime only when the deployment target is known:

| Operating system / goal | Configuration | Fields |
| --- | --- | --- |
| Portable default | `type = "auto"` | `max_events = 1024`, `sq_entries = 4096`, `cq_entries = 8192`, `buf_ring_size = 16384`, `buf_size = 8192` |
| Linux readiness runtime | `type = "epoll"` | `max_events = 1024` |
| macOS/BSD readiness runtime | `type = "kqueue"` | `max_events = 1024` |
| Verified Linux io_uring host | `type = "io_uring"` | `sq_entries = 4096`, `cq_entries = 8192`, `buf_ring_size = 16384`, `buf_size = 8192` |

All runtime event, queue, and buffer sizes must be greater than zero. `buf_ring_size` must be a power of two from 1 through 32768. Explicit `io_uring` fails startup if required Linux support is absent; it does not fall back.

### Workers, proxy deadlines, and DNS

```toml
worker_count = 4

[worker]
max_connections = 1024
max_read_buffer_size = 65536
max_write_buffer_size = 8388608
idle_timeout_ms = 30000
drain_timeout_ms = 10000

[proxy]
connect_timeout_ms = 3000
write_timeout_ms = 30000
read_timeout_ms = 30000

[dns]
resolver_threads = 2
timeout_ms = 3000
```

All worker limits and timeout values must be greater than zero. Limits are per worker. DNS addresses are connection fallbacks for one selected endpoint; they are not an upstream balancing policy.

### HTTP servers, virtual hosts, and locations

```toml
[[http.servers]]
listen = "0.0.0.0:8080"
server_name = "example.test"

[[http.servers.locations]]
matcher = { type = "exact", path = "/health" }
action = { type = "response", status = 200, body = "OK" }

[[http.servers.locations]]
matcher = { type = "prefix", path = "/" }
action = { type = "static", directory = "./public" }
```

`listen` is a required literal socket address. `server_name` is optional. Servers sharing an address form one listener group; the first server is the fallback and an exact case-insensitive `Host` match selects a named server.

Supported matchers are `exact` and `prefix`. Exact wins before prefix; otherwise the longest prefix wins. Regex matchers are not available yet.

### SSL / TLS ingress

For a source configuration that terminates TLS with one certificate, add `ssl`
to the target server rather than an action:

```toml
[[http.servers]]
listen = "0.0.0.0:443"
server_name = "example.test"
ssl = { certificate_path = "/etc/thwip/fullchain.pem", private_key_path = "/etc/thwip/privkey.pem", handshake_timeout_ms = 10000, protocols = ["tlsv1_2", "tlsv1_3"] }
```

`certificate_path` and `private_key_path` are required PEM paths. The timeout
defaults to `10000` milliseconds. TLS 1.2 and TLS 1.3 are the only protocol
choices; omit `ciphers` to retain the secure default suite set. SSL is optional:
a server without this block remains plaintext. Warn instead of converting when
the source needs SNI certificate selection, multiple certificates on one
listener, client-certificate authentication, or HTTPS upstreams.

### Actions

| Action | Required fields | Notes |
| --- | --- | --- |
| `response` | `status`, `body` | Returns a fixed text response. |
| `static` | `directory` | Serves `GET`/`HEAD`, protects the root from traversal, and selects `index.html` for directory URLs. |
| `proxy` | Exactly one upstream form | Supports plaintext HTTP upstreams only. |
| `ssl` | Per-server TLS termination | TLS 1.2/1.3 with PEM certificate/key paths; no SNI certificate selection. |

Proxy has exactly one of these forms:

```toml
# Direct endpoint
action = { type = "proxy", upstream = "http://127.0.0.1:9000" }

# Named root-level group
action = { type = "proxy", upstream_group = "api" }

# Route-local pool
[http.servers.locations.action]
type = "proxy"
policy = "round_robin"
upstreams = [
  { url = "http://127.0.0.1:9001" },
  { url = "http://127.0.0.1:9002", weight = 2 },
]
```

### Named upstream groups

```toml
[upstreams.api]
policy = "weighted_round_robin"
servers = [
  { url = "http://127.0.0.1:9001", weight = 2 },
  { url = "http://127.0.0.1:9002", weight = 1 },
]
```

The supported policies are `round_robin` and `weighted_round_robin`; weighted round robin is the default. Every group must have a non-empty name and at least one non-empty endpoint URL. Endpoint weight defaults to `1` and must be positive. Named-group selection state is worker-local.

## Required warnings

Warn rather than approximate when the source needs any of the following:

- SNI certificate selection or HTTPS upstreams;
- regex or case-insensitive regex routes;
- rewrites, redirects, `try_files`, named locations, `if`, `map`, variables, or included configuration files;
- PHP/FastCGI, uWSGI, SCGI, gRPC, WebSockets, UDP, QUIC, or HTTP/3;
- upstream pooling, retries, health checks, `least_conn`, `ip_hash`, failover controls, or advanced balancing;
- proxy header customization, request/response buffering controls, caching, compression, rate limits, authentication, or access rules;
- chunked request bodies or keep-alive-dependent behavior;
- automatic `index.php`, directory listings, symlink policies, or static-file behavior beyond Thwip's `index.html` handling.

State whether the correct action is to keep the existing proxy in front of Thwip, simplify the source behavior, or wait for the corresponding Thwip roadmap feature.

## Validation checklist

Before handing over the configuration, verify:

- every `listen` value is a literal socket address;
- every `ssl` block has protected, readable PEM certificate/key paths; TLS 1.2/1.3 and the configured cipher suites are intentional;
- every proxy action configures exactly one of `upstream`, `upstream_group`, or `upstreams`;
- named upstream groups exist and every URL is non-empty;
- weights are positive and the selected policy is supported;
- the selected runtime matches the deployment platform, or `type = "auto"` is intentional;
- static directories are explicit and appropriate for the process working directory.
