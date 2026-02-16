# ── Builder ───────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM rust:1-slim AS builder

# Install cross-compilation helpers
COPY --from=tonistiigi/xx / /

ARG TARGETPLATFORM

# Install dependencies for cross-compilation
# clang and lld are recommended for cross-compiling to musl with xx
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang \
        lld \
        musl-tools \
        git \
        file && \
    xx-apt-get install -y musl-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Install the target architecture for Rust
RUN rustup target add $(xx-info march)-unknown-linux-musl

WORKDIR /build

# Copy manifests and vendor source first for better layer caching.
COPY Cargo.toml Cargo.lock ./
COPY vendor/fe2o3-amqp/ vendor/fe2o3-amqp/

# Create a dummy main.rs so `cargo build` compiles dependencies only.
RUN mkdir src && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'pub mod config; pub mod server; pub mod router; pub mod sasl; pub mod cbs;' > src/lib.rs && \
    touch src/config.rs src/server.rs src/router.rs src/sasl.rs src/cbs.rs && \
    # Configure linker to use clang with the correct target architecture
    mkdir -p .cargo && \
    echo "[target.$(xx-info march)-unknown-linux-musl]" > .cargo/config.toml && \
    echo "linker = \"clang\"" >> .cargo/config.toml && \
    echo "rustflags = [\"-C\", \"link-arg=-fuse-ld=lld\", \"-C\", \"link-arg=--target=$(xx-info triple)\", \"-C\", \"target-feature=+crt-static\"]" >> .cargo/config.toml && \
    # Build for the target architecture
    xx-cargo build --release --target $(xx-info march)-unknown-linux-musl || true && \
    rm -rf src

# Copy the real source and rebuild.
COPY src/ src/
RUN touch src/*.rs && \
    xx-cargo build --release --target $(xx-info march)-unknown-linux-musl && \
    # Verify the binary architecture
    xx-verify target/$(xx-info march)-unknown-linux-musl/release/fast-servicebus-emulator && \
    # Move binary to a predictable location for the next stage
    cp target/$(xx-info march)-unknown-linux-musl/release/fast-servicebus-emulator /emulator

# ── Runtime ───────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /emulator /emulator
COPY config-sample.yaml /config/config.yaml

ENV CONFIG_PATH=/config/config.yaml

EXPOSE 5672

ENTRYPOINT ["/emulator"]
