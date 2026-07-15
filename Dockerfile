FROM rust:1.96.0-alpine@sha256:f87aa870663e2b57ec8c69de82c7eedf7383bee987eef7612c0359635eaadb41 AS build

SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

RUN wget -q -O /tmp/sccache.tar.gz \
      https://github.com/mozilla/sccache/releases/download/v0.16.0/sccache-v0.16.0-x86_64-unknown-linux-musl.tar.gz \
    && echo "aec995a83ad3dff3d14b6314e08858b7b73d35ca85a5bcf3d3a9ec07dee35588  /tmp/sccache.tar.gz" \
      | sha256sum -c - \
    && tar -xzf /tmp/sccache.tar.gz -C /tmp \
    && install -m 0755 \
      /tmp/sccache-v0.16.0-x86_64-unknown-linux-musl/sccache \
      /usr/local/bin/sccache \
    && rm -rf /tmp/sccache.tar.gz \
      /tmp/sccache-v0.16.0-x86_64-unknown-linux-musl

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo ./.cargo
COPY src ./src
RUN --mount=type=cache,id=rust-sccache-v0.16.0,target=/root/.cache/sccache,sharing=locked \
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
COPY --from=build /src/target/release/proxy-client /proxy-client-linux-amd64
