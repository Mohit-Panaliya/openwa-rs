# syntax=docker/dockerfile:1

# ---- builder: compile the release gateway binary ----
FROM rust:1-slim-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p openwa-gateway

# ---- runtime: debian slim + node 22 + sidecar ----
FROM debian:bookworm-slim AS runtime

# Node.js 22 is required by the Baileys sidecar; libssl3 backs reqwest's
# native-tls (OpenSSL) builds on Linux.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg libssl3 \
 && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Sidecar sources and dependencies. Falls back to `npm install` when no lockfile
# is present.
COPY sidecar/ /app/sidecar/
RUN cd /app/sidecar && (npm ci --omit=dev || npm install --omit=dev) && npm cache clean --force

# Static dashboard (served at /), no build step required.
COPY dashboard/ /app/dashboard/

COPY --from=builder /build/target/release/openwa-gateway /usr/local/bin/openwa-gateway

# Non-root runtime user. /app/data holds the API key, the SQLite database, media
# blobs and sidecar auth state, and is expected to be a volume.
RUN useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin openwa \
 && mkdir -p /app/data \
 && chown -R openwa:openwa /app

USER openwa

ENV SIDECAR_DIR=/app/sidecar \
    DATA_DIR=/app/data \
    DASHBOARD_DIR=/app/dashboard \
    NODE_BIN=node \
    PORT=2785 \
    HOST=0.0.0.0 \
    RUST_LOG=info

EXPOSE 2785

ENTRYPOINT ["/usr/local/bin/openwa-gateway"]
