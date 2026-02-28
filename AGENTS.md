# AGENTS.md

This file provides guidance for AI agents working on this codebase.

## Project Overview

A high-performance, lightweight Azure Service Bus emulator written in Rust. It implements AMQP 1.0 over plain TCP (port 5672) and is designed to work with official Azure SDKs (especially .NET). The emulator uses static topology defined in YAML, supports in-memory message storage only, and provides mock authentication (SASL + CBS). It also provides a testing REST API for messages management working on port 45672.

## Architecture

```
TCP (port 5672)
  -> SASL handshake (accepts any mechanism)
  -> AMQP 1.0 connection
  -> Sessions (one or more)
    -> Links (sender/receiver)
      -> CBS links ($cbs) for mock token auth
      -> Queue/Topic links for message flow
HTTP (port 45672)
  -> REST API for messages management
  - POST /testing/messages/queue/{queue_name} - Create a message and send it to the specified queue.
  - POST /testing/messages/topic/{topic_name} - Create a message and send it to the specified topic.
  - DELETE /testing/messages - Delete all messages from all queues and topics.
  - DELETE /testing/messages/queue/{queue_name} - Delete all messages from the specified queue.
  - DELETE /testing/messages/topic/{topic_name} - Delete all messages from the specified topic.
  - DELETE /testing/messages/topic/{topic_name}/subscription/{subscription_name} - Delete all messages from the specified subscription.
  - GET /testing/messages - Retrieve all messages from all queues and topics.
  - GET /testing/messages/queue/{queue_name} - Retrieve all messages from the specified queue.
  - GET /testing/messages/topic/{topic_name} - Retrieve all messages from the specified topic.
  - GET /testing/messages/topic/{topic_name}/subscription/{subscription_name} - Retrieve all messages from the specified subscription.
```

### Key Design Decisions

- **Static topology**: All queues, topics, and subscriptions are defined in `config.yaml` at startup. No management API.
- **In-memory only**: No persistence. Messages are lost on restart.
- **Competing consumers**: Within a queue or subscription, each message goes to exactly one receiver. Topics fan out to all subscriptions.
- **Case-insensitive** address matching for subscription paths.
- **Correlation filters only**: SQL filters are not implemented (log a warning, match all messages).
- **Mock auth**: SASL accepts any mechanism/credentials. CBS accepts any token and returns 200 OK.

## Directory Structure

```
src/                    # Emulator Rust source code
tests/                  # Rust integration tests
integration/            # .NET integration tests and Aspire nuget library.
vendor/fe2o3-amqp/      # Git submodule: patched fe2o3-amqp AMQP 1.0 library
vendor/azure-sdk-for-net/  # Reference: Azure .NET SDK source
vendor/azure-amqp/      # Reference: Microsoft.Azure.Amqp source
config.yaml             # Static topology configuration
Dockerfile              # Scratch-based Docker image (musl static binary)
PLAN.md                 # Full project plan and requirements
```

## fe2o3-amqp Submodule (vendor/fe2o3-amqp/)

This is a **patched fork** at `https://github.com/Indomitable/fe2o3-amqp.git`, branch `patch/pipelined-flow-credit`. It contains fixes for:
1. Session relay not registered for pipelined frames
2. Flow/Transfer frames crashing sessions for not-yet-accepted links
3. Pipelined Flow credit lost (sender links block forever)

**Additional modifications made in this project:**
- `fe2o3-amqp-types/src/messaging/format/header.rs` -- Manual `Serialize` impl to always serialize `delivery_count` (even when 0)
- `fe2o3-amqp/src/session/mod.rs` -- Tail chunk fix in `on_incoming_disposition` + echo state changed to `Accepted`
- `fe2o3-amqp/src/link/delivery.rs` -- `Sendable` supports custom `delivery_tag` (for lock tokens)

## Configuration

`config.yaml` defines the static topology. Path is set via `CONFIG_PATH` env var (default: `config.yaml` locally, `/config/topology.yaml` in Docker).

Key config options per queue/subscription:
- `lock_duration_secs` (default: 30)
- `max_delivery_count` (default: 10) -- auto-dead-letter after this many deliveries
- `default_message_ttl_secs` (default: 0 = no TTL)
- `dead_lettering_on_message_expiration` (default: false)
- `max_size` (default: none) -- backpressure limit; rejects with `amqp:resource-limit-exceeded` when full

## Testing

### Rust Tests
```bash
cargo test --lib           
cargo test                 # All tests including integration (requires emulator NOT running on 5672)
cargo test --test queue_test   # Individual integration test
```

### .NET Integration Tests
Dotnet tests can be run directly using `dotnet test` . Most of the tests are located in `FastServiceBusEmulator.IntegrationTests` project.
It is using an Aspire which starts automatically the emulator on port 5672, so no need to start it manually.
```bash
# Run all tests in a class
dotnet test --filter-class FULL_CLASS_NAME
# example:
dotnet test --filter-class FastServiceBusEmulator.IntegrationTests.CorrelationFilterTests
# Run a single test
dotnet test --filter-method FULL_METHOD_NAME
# example:
dotnet test --filter-method FastServiceBusEmulator.IntegrationTests.CorrelationFilterTests.Correlation_Filter_Routes_By_Subject
```

Connection string: `Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`

## Build and Deploy

```bash
# Local development
cargo build
RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/fast-servicebus-emulator

# Docker (uses podman)
podman build -t docker.io/indomitable/fast-servicebus-emulator .
podman run -p 5672:5672 -p 45672:45672 docker.io/indomitable/fast-servicebus-emulator

# Kill running emulator
kill $(pgrep -f fast-servicebus-emulator) 2>/dev/null
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_PATH` | `config.yaml` | Path to topology YAML file |
| `RUST_LOG` | `info` | Tracing log level filter |

## Known Workarounds

### Azure SDK Bug: Missing `initial-delivery-count`
The `Microsoft.Azure.Amqp` library omits the mandatory `initial-delivery-count` field on sender Attach frames in some code paths. Our workaround: `patch_attach_if_needed()` in `server.rs` patches incoming sender attaches with `initial_delivery_count = Some(0)`. See `BUG_REPORT_INITIAL_DELIVERY_COUNT.md` for details.

### Lock Token in Delivery Tag
The Azure .NET SDK reads lock tokens from the AMQP delivery tag (`new Guid(amqpMessage.DeliveryTag.Array)`), not from `x-opt-lock-token`. We set custom delivery tags via the modified `Sendable::builder().delivery_tag()` API in fe2o3-amqp.

### Header delivery_count Must Always Be Serialized
The `serde_amqp` derive macro skips trailing default fields. Since `delivery_count` (default 0) is the last field in `Header`, it was omitted when 0. The .NET SDK throws `Nullable object must have a value`. Fixed with a manual `Serialize` impl using `DESCRIBED_LIST` encoding.

## Common Debugging

```bash
# Check emulator logs
tail -f /tmp/emulator.log

# Verify port is listening
ss -tlnp | grep 5672

# Check if emulator is running
pgrep -f fast-servicebus-emulator
```
