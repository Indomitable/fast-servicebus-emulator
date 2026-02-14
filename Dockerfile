# ── Builder ───────────────────────────────────────────────────────
FROM rust:1-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/* && \
    rustup target add x86_64-unknown-linux-musl

WORKDIR /build

# Copy manifests and vendor source first for better layer caching.
# Changes to src/ won't invalidate the dependency build layer.
COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/

# Create a dummy main.rs so `cargo build` compiles dependencies only.
RUN mkdir src && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'pub mod config; pub mod server; pub mod router; pub mod sasl; pub mod cbs;' > src/lib.rs && \
    touch src/config.rs src/server.rs src/router.rs src/sasl.rs src/cbs.rs && \
    cargo build --release --target x86_64-unknown-linux-musl || true && \
    rm -rf src

# Copy the real source and rebuild. The `touch` ensures cargo sees
# the real sources as newer than the dummy-built artefacts.
COPY src/ src/
RUN touch src/*.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    strip /build/target/x86_64-unknown-linux-musl/release/azure-servicebus-emulator

# ── Runtime ───────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/azure-servicebus-emulator /emulator

# Ship the default topology so the image works out of the box.
COPY config.yaml /config/config.yaml

ENV CONFIG_PATH=/config/config.yaml

EXPOSE 5672

ENTRYPOINT ["/emulator"]
