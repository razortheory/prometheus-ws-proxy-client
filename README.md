# Prometheus WebSocket Proxy Client

[![CI](https://github.com/roman-karpovich/prometheus-ws-proxy-client/actions/workflows/ci.yml/badge.svg)](https://github.com/roman-karpovich/prometheus-ws-proxy-client/actions/workflows/ci.yml)

`proxy-client` lets Prometheus scrape exporters in private networks without opening inbound exporter ports. The client keeps outbound WebSocket connections to the proxy server, receives allow-listed scrape requests, calls local exporters, and returns their status and body.

This is product version 3. It preserves the configuration, CLI, routes, and historical wire protocols used by the long-running Python proxy. The matching server is [prometheus-ws-proxy-server](https://github.com/roman-karpovich/prometheus-ws-proxy-server).

## How it works

```text
Prometheus
    |  GET /proxy/request/<instance>/<resource>/
    v
proxy server  <==== WebSocket ====  proxy-client  ---- HTTP ----> exporter
    ^                                      |
    +--------- WebSocket or HTTP ----------+
```

Each worker connection handles one scrape at a time. `--parallel=3` opens three independent worker connections, so a slow exporter cannot create an unbounded queue inside one worker.

## Compatibility

Product version and wire version are separate concepts. Product v3 supports all historical wire modes:

| `--protocol` | Selection handshake | Response transport | Intended compatibility |
| --- | --- | --- | --- |
| `1` | Direct request | WebSocket | Oldest clients and servers |
| `2` | `ready` / selected worker | WebSocket | Existing Rust v2 deployments |
| `3` | `ready` / selected worker | HTTP form POST | Existing Python deployments and the v3 default |

The Rust v3 server accepts old Python clients and Rust wire v1/v2 clients. This client can connect to the old Python server when configured with its supported wire version. That allows a server-first rollout without changing scrape target syntax.

Wire v3 uses an HTTP response POST and falls back to a WebSocket response when that POST fails or times out. Delivery is therefore transport-level at-least-once: if the POST was applied but its reply was lost, the same UID can be sent again. The Rust server consumes pending UIDs once, and the legacy Python server safely overwrites the same UID result.

## Install a release

Releases contain one stripped, static Linux amd64 binary and its checksum. Pin a version in automation:

```bash
VERSION=v3.0.0
BASE="https://github.com/roman-karpovich/prometheus-ws-proxy-client/releases/download/${VERSION}"
curl -fLO "${BASE}/proxy-client-linux-amd64"
curl -fLO "${BASE}/proxy-client-linux-amd64.sha256"
sha256sum --check proxy-client-linux-amd64.sha256
sudo install -m 0755 proxy-client-linux-amd64 /usr/local/bin/proxy-client
proxy-client --version
```

Verify the canonical filename before installing it under the shorter `proxy-client` name; the checksum file intentionally contains the release asset basename.

The binary is built with musl and rustls and has no runtime dependency on glibc or OpenSSL. The host still needs a current `ca-certificates` package for TLS certificate validation.

## Configuration

The positional argument is a JSON file:

```json
{
  "instance": "host-a",
  "target": "https://prometheus.example.com/proxy/",
  "resources": {
    "node": "http://127.0.0.1:9100/metrics",
    "application": "http://127.0.0.1:9200/metrics"
  },
  "cf_access_enabled": false,
  "cf_access_key": "",
  "cf_access_secret": ""
}
```

- `instance` is the name used in the Prometheus request route.
- `target` accepts either an HTTP(S) base such as `https://host/proxy/` or an exact WebSocket endpoint such as `wss://host/proxy/ws/`. HTTP maps to WS and HTTPS maps to WSS automatically.
- `resources` is an allow-list from public resource name to exporter URL. An unknown name returns exactly `404` with `No such resource` and is never treated as a URL.
- `cf_access_enabled`, `cf_access_key`, and `cf_access_secret` enable Cloudflare Access service-token headers on the WebSocket and wire-v3 response POST. Secret values are redacted from debug output.
- `ec2_meta_domain` is optional and defaults to `http://169.254.169.254`.

Set `instance` to `ec2` to resolve the EC2 instance ID once at startup. The client tries IMDSv2 first and falls back to IMDSv1 for compatibility.

The JSON can contain Cloudflare credentials. Keep it owned by the service account and mode `0600`; do not commit production values or pass them through logs.

## Command line

```text
proxy-client [CONFIG] [--parallel <N>] [--protocol <1|2|3>] [-v...] [--sentry_dsn <DSN>]
```

Example:

```bash
proxy-client /etc/prometheus-proxy/client.json \
  --parallel=3 \
  --protocol=3 \
  -v
```

Defaults are `client_config.json`, three workers, and wire protocol 3. `RUST_LOG` overrides the verbosity-derived log filter. Application debug and trace logs remain available, but debug and trace from HTTP/WebSocket transport dependencies are hard-disabled so request headers cannot be exposed by `-vvv` or `RUST_LOG=trace`. The underscore spelling of `--sentry_dsn` is retained for existing service definitions.

## systemd

```ini
[Unit]
Description=Prometheus WebSocket proxy client
After=network-online.target
Wants=network-online.target

[Service]
User=prometheus
Group=prometheus
ExecStart=/usr/local/bin/proxy-client /etc/prometheus-proxy/client.json --parallel=3 --protocol=3
Restart=always
RestartSec=2
TimeoutStopSec=20
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

The client reconnects internally after connection loss and attempts graceful shutdown within 15 seconds.

## Resource and failure bounds

- One active exporter scrape per worker connection.
- 64 MiB maximum exporter body and WebSocket message.
- Exporter connect timeout: 5 seconds; total request timeout: 60 seconds.
- Proxy WebSocket connect timeout: 10 seconds; reconnect delay: 1 second.
- Heartbeat every 20 seconds; stale connection timeout: 45 seconds.
- WebSocket writes and fallback response queues are bounded.

Choose `--parallel` from expected scrape concurrency. Adding workers increases concurrency and open sockets; it does not create an unbounded task pool.

## Build and test from source

Rust 1.96 is pinned for normal development. The declared minimum supported Rust version is 1.88.

```bash
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 test --locked --all-targets --all-features
cargo +1.96.0 clippy --locked --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --all-targets --all-features
```

Build the release artifact exactly as CI does:

```bash
docker buildx build \
  --platform linux/amd64 \
  --target artifact \
  --output type=local,dest=dist \
  .
```

## Releases and Ubuntu support

Pushing a tag such as `v3.0.0` starts the release workflow. The tag must exactly match the Cargo package version. CI builds one static `proxy-client-linux-amd64`, verifies that it has no ELF interpreter or dynamic dependencies, creates a SHA-256 file, and runs that same artifact in Ubuntu 16.04, 18.04, 20.04, 22.04, 24.04, and 26.04 containers before publishing it.

Container smoke tests verify each Ubuntu userspace, not its historical kernel. In particular, Ubuntu 16.04 normally runs kernel 4.4 while GitHub and Docker hosts use a newer kernel. Treat Ubuntu 16 support as provisional until the artifact has also run on a real Ubuntu 16 host or VM.

## Safe rollout and rollback

1. Run the Rust v3 server on a shadow port or hostname and test it with existing Python clients.
2. Point the existing Nginx `/proxy` upstream at Rust while leaving client configuration and Prometheus targets unchanged.
3. Keep the Python server running on its previous rollback port, but remove it from the production upstream.
4. Replace Python clients with this binary in small batches.
5. Remove the Python server only after the observation window.

To roll back the server, restore the Nginx upstream to Python. Rust clients continue to work with the old Python server, so client batches do not have to be rolled back at the same time.

The current Rust server is intentionally single-process and keeps connection state in memory. Do not place multiple active server processes behind one route: there is no shared connection registry.
