FROM rust:1.96.0-alpine@sha256:f87aa870663e2b57ec8c69de82c7eedf7383bee987eef7612c0359635eaadb41 AS build

ARG TARGETARCH

SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

RUN case "$TARGETARCH" in \
      amd64) \
        sccache_target=x86_64-unknown-linux-musl; \
        sccache_sha256=aec995a83ad3dff3d14b6314e08858b7b73d35ca85a5bcf3d3a9ec07dee35588 ;; \
      arm64) \
        sccache_target=aarch64-unknown-linux-musl; \
        sccache_sha256=f73a5c39f96bb6ebb89cc7915cf182260d4cbf30765322c5e793d0fe8bd80784 ;; \
      *) \
        echo "unsupported TARGETARCH '${TARGETARCH}', expected amd64 or arm64" >&2; \
        exit 1 ;; \
    esac \
    && sccache_release="sccache-v0.16.0-${sccache_target}" \
    && wget -q -O /tmp/sccache.tar.gz \
      "https://github.com/mozilla/sccache/releases/download/v0.16.0/${sccache_release}.tar.gz" \
    && echo "${sccache_sha256}  /tmp/sccache.tar.gz" \
      | sha256sum -c - \
    && tar -xzf /tmp/sccache.tar.gz -C /tmp \
    && install -m 0755 \
      "/tmp/${sccache_release}/sccache" \
      /usr/local/bin/sccache \
    && rm -rf /tmp/sccache.tar.gz \
      "/tmp/${sccache_release}"

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo ./.cargo
COPY src ./src
RUN --mount=type=cache,id=rust-sccache-v0.16.0-${TARGETARCH},target=/root/.cache/sccache,sharing=locked \
    --mount=type=secret,id=actions_results_url,required=false \
    --mount=type=secret,id=actions_runtime_token,required=false \
    set -eu; \
    export SCCACHE_DIR=/root/.cache/sccache; \
    if [ -s /run/secrets/actions_results_url ] \
      && [ -s /run/secrets/actions_runtime_token ]; then \
      export SCCACHE_GHA_ENABLED=true; \
      ACTIONS_RESULTS_URL="$(cat /run/secrets/actions_results_url)"; \
      ACTIONS_RUNTIME_TOKEN="$(cat /run/secrets/actions_runtime_token)"; \
      export ACTIONS_RESULTS_URL ACTIONS_RUNTIME_TOKEN; \
    fi; \
    cargo build --release --locked; \
    sccache --show-stats; \
    sccache --stop-server

FROM scratch AS artifact
ARG TARGETARCH
COPY --from=build /src/target/release/proxy-client /proxy-client-linux-${TARGETARCH}
