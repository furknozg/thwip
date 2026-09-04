# Thwip

![Thwip logo](public/logo.png)

Thwip is an experimental Unix HTTP server and reverse proxy written in Rust.
It keeps worker, socket, and runtime ownership explicit while supporting the
best native event mechanism available on the host: epoll on Linux, kqueue on
macOS/BSD, and io_uring where the Linux kernel supports it.

> Thwip is currently suited to development and experimentation. It serves
> client-side TLS termination is available with an explicit per-server `ssl`
> block. Upstreams remain plaintext `http://`; keep-alive, HTTP/2, HTTP/3,
> SNI certificate selection, and HTTPS upstreams are not available yet.

## Get started

### Check our Docs

For further documentation of configuring and setting up thwip, check out the [Thwip Documentation](https://thwip.tech/).

### Prerequisites

- A Unix-like host: Linux, macOS, or a supported BSD.
- A current stable [Rust toolchain](https://rustup.rs/).

### Run a minimal server

```sh
git clone https://github.com/furknozg/thwip.git
cd thwip
cp examples/config/minimal-response.toml rginx.toml
cargo run --release -p master --bin thwip-main
```

In a second terminal, verify the configured health route:

```sh
curl -i http://127.0.0.1:8080/health
```

It should return `HTTP/1.1 200 OK` with an `OK` body. Press `Ctrl-C` in the
server terminal to request graceful shutdown.

Thwip currently reads `rginx.toml` from its working directory. The
`minimal-response` example uses `runtime = "auto"`: Linux attempts io_uring
when its required features are available and otherwise selects epoll; macOS and
BSD select kqueue. Use an explicit runtime while comparing behavior or
performance.

## Configure it

The executable configuration is TOML. Start with one of the validated
examples in [`examples/config`](examples/config/):

- [`static-site.toml`](examples/config/static-site.toml) for static files.
- [`direct-proxy.toml`](examples/config/direct-proxy.toml) for one upstream.
- [`load_balanced.toml`](examples/config/load_balanced.toml) for named,
  weighted upstream groups.
- [`virtual-hosts.toml`](examples/config/virtual-hosts.toml) for multiple
  hosts sharing a listener.

The rendered configuration guide is available from the included static site at
[`public/docs/index.html`](public/docs/index.html), or serve `public/` with any
static web server to browse it locally.

## Test

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

GitHub Actions runs the readiness implementation on Linux/epoll and
macOS/kqueue, and validates the Linux io_uring build and configuration. Native
io_uring integration coverage remains a roadmap item.

## Project links

- [GitHub repository](https://github.com/furknozg/thwip)
- [Roadmap](ROADMAP.md)
- [Migration guide and configuration skill](public/docs/migration/SKILL.md)

## License and contribution

Thwip is licensed under the [Apache License, Version 2.0](LICENSE). Please open
an issue before relying on it in production or proposing a large change. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.
