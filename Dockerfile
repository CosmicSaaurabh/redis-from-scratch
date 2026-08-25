# syntax=docker/dockerfile:1

# Two binaries from two toolchains, assembled into one small runtime image.
#
# The Go server and the Rust storage engine are built in separate stages so
# that a change to one does not invalidate the other's layer cache, and neither
# toolchain ends up in the image that actually ships.

# ---------------------------------------------------------------------------
# Stage 1: the Rust storage engine.
# ---------------------------------------------------------------------------
FROM rust:1.91-slim-bookworm AS engine-build

RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Manifests first, with a dummy source tree, so that the dependency build is
# cached separately from the engine's own code. Without this every source edit
# recompiles the whole dependency graph.
COPY engine/Cargo.toml engine/Cargo.lock ./engine/
# The toolchain file lives at the repository root and applies to the engine
# crate, so it has to be copied before any cargo invocation or the container
# would silently build with a different compiler than local development uses.
COPY rust-toolchain.toml ./
RUN mkdir -p engine/src/bin \
 && echo 'fn main() {}' > engine/src/bin/rfs-engine.rs \
 && echo '' > engine/src/lib.rs \
 && cd engine \
 && cargo build --release --locked 2>/dev/null || true

COPY proto/ ./proto/
COPY engine/ ./engine/
# Touch the real sources so cargo does not trust the dummy build's timestamps.
RUN cd engine && touch src/lib.rs src/bin/rfs-engine.rs \
 && cargo build --release --locked --bins

# ---------------------------------------------------------------------------
# Stage 2: the Go server.
# ---------------------------------------------------------------------------
FROM golang:1.26-bookworm AS go-build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

ARG VERSION=dev
ARG COMMIT=unknown
# CGO is off so the result is a static binary that runs on a distroless base
# with no libc to match.
RUN CGO_ENABLED=0 go build \
      -ldflags "-s -w \
        -X github.com/CosmicSaaurabh/redis-from-scratch/internal/version.Version=${VERSION} \
        -X github.com/CosmicSaaurabh/redis-from-scratch/internal/version.Commit=${COMMIT}" \
      -o /out/rfs-server ./cmd/server \
 && CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/rfs-bench ./cmd/rfs-bench

# ---------------------------------------------------------------------------
# Stage 3: the runtime.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 rfs \
 && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /data --shell /usr/sbin/nologin rfs

COPY --from=go-build     /out/rfs-server              /usr/local/bin/rfs-server
COPY --from=go-build     /out/rfs-bench               /usr/local/bin/rfs-bench
COPY --from=engine-build /src/engine/target/release/rfs-engine /usr/local/bin/rfs-engine

# The data directory is a volume so that a container restart is not a data
# loss event. Everything this server promises about durability depends on the
# directory outliving the process.
RUN mkdir -p /data && chown -R rfs:rfs /data
VOLUME ["/data"]

USER rfs
WORKDIR /data

EXPOSE 6379 9121 50051

# The health check speaks the actual protocol rather than probing the port,
# because a socket that accepts and then hangs is exactly the failure a port
# probe cannot see. It is an inline PING, so no redis-cli is needed in the
# image.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD ["/bin/sh", "-c", "exec 3<>/dev/tcp/127.0.0.1/6379 && printf 'PING\\r\\n' >&3 && head -c 7 <&3 | grep -q PONG"]

ENTRYPOINT ["/usr/local/bin/rfs-server"]
CMD ["-dir", "/data", "-addr", ":6379", "-metrics-addr", ":9121"]
