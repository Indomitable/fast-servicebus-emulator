# AGENTS.md

This file provides guidance for AI agents working on this codebase.

## Project Overview

A high-performance, lightweight Azure Service Bus emulator written in Rust. It implements AMQP 1.0 over plain TCP (port 5672) and is designed to work with official Azure SDKs (especially .NET). The emulator uses static topology defined in YAML, supports in-memory message storage only, and provides mock authentication (SASL + CBS).

## Architecture

```
TCP (port 5672)
  -> SASL handshake (accepts any mechanism)
  -> AMQP 1.0 connection
  -> Sessions (one or more)
    -> Links (sender/receiver)
      -> CBS links ($cbs) for mock token auth
      -> Queue/Topic links for message flow
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
src/                    # Emulator Rust source code (8 files)
tests/                  # Rust integration tests (4 files)
integration/            # .NET xUnit integration tests (9 test files)
vendor/fe2o3-amqp/      # Git submodule: patched fe2o3-amqp AMQP 1.0 library
vendor/azure-sdk-for-net/  # Reference: Azure .NET SDK source (read-only)
vendor/azure-amqp/      # Reference: Microsoft.Azure.Amqp source (read-only)
config.yaml             # Static topology configuration
Dockerfile              # Scratch-based Docker image (musl static binary)
PLAN.md                 # Full project plan and requirements
```

## Source Files

### `src/main.rs` (31 lines)
Entry point. Sets up tracing, reads `CONFIG_PATH` env var, loads config, starts server.

### `src/lib.rs` (7 lines)
Module declarations.

### `src/config.rs` (394 lines)
YAML topology configuration parsing. Key types:
- `Config` / `Topology` / `QueueConfig` / `TopicConfig` / `SubscriptionConfig`
- `SubscriptionFilter` enum (`Correlation` variant only; `Sql` is parsed but not evaluated)
- `SubscriptionEntry` (untagged: string name or full config object)

### `src/server.rs` (362 lines)
AMQP connection handling and link lifecycle. Key functions:
- `Server::run()` / `Server::run_on()` -- TCP listener on 0.0.0.0:5672
- `handle_connection()` -- SASL handshake, AMQP connection setup
- `handle_session()` -- link accept loop, CBS state per session
- `handle_link()` -- routes sender/receiver endpoints to CBS or queue/topic handlers
- `patch_attach_if_needed()` -- workaround for Azure SDK bug (missing `initial_delivery_count` on sender Attach frames)

### `src/router.rs` (1754 lines)
Message routing and delivery. The largest source file. Key types and functions:
- `Router` -- owns all message stores, DLQ stores, topic-subscription mappings
- `Router::publish()` -- enqueues to queue or fans out to topic subscriptions
- `handle_incoming_messages()` -- client-to-server (sender link handler)
- `handle_outgoing_messages()` -- server-to-client, ReceiveAndDelete mode
- `handle_outgoing_messages_peek_lock()` -- server-to-client, PeekLock mode with disposition settlement
- `handle_outgoing_dlq_messages()` / `handle_outgoing_dlq_messages_peek_lock()` -- DLQ variants
- `stamp_broker_properties()` -- stamps sequence number, enqueued time, lock token, delivery count on outgoing messages
- `matches_filter()` -- evaluates correlation filters against message properties

### `src/store.rs` (1158 lines)
In-memory message store. Key types:
- `MessageStore` -- main queue/subscription store with enqueue, receive, lock, complete, abandon, dead-letter, TTL expiry, backpressure (`logical_count` / `max_size`)
- `DlqStore` -- dead-letter queue (simpler, no TTL, no nested DLQ)
- `Envelope` -- wraps a message with metadata (sequence number, delivery count, lock state, TTL)
- `EntityConfig` -- per-queue/subscription settings (lock duration, max delivery count, TTL, max size)

### `src/sasl.rs` (115 lines)
Mock SASL acceptor. Accepts ANONYMOUS, PLAIN, MSSBCBS, EXTERNAL. Always returns `SaslCode::Ok`.

### `src/cbs.rs` (160 lines)
Mock CBS (Claims-Based Security) handler. Accepts any put-token request, responds with status 200 OK.

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
cargo test --lib           # ~57 unit tests (inline #[cfg(test)] modules)
cargo test                 # All tests including integration (requires emulator NOT running on 5672)
cargo test --test queue_test   # Individual integration test
```

### .NET Integration Tests (20 tests, 9 files)
Require the emulator running on port 5672.
```bash
# Start emulator
RUST_LOG=debug CONFIG_PATH=config.yaml cargo run > /tmp/emulator.log 2>&1 &

# Build and run tests
cd integration/AzureServiceBusEmulator.IntegrationTests
dotnet build
dotnet test --no-build -v n

# Single test
dotnet test --no-build -v n --filter "Send_To_Full_Queue_Is_Rejected"
```

Connection string: `Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`

### Test Categories
| File | Tests | What it covers |
|------|-------|----------------|
| `EmulatorTests.cs` | Queue send/receive, topic fanout | Basic AMQP messaging |
| `SingleReceiverTests.cs` | Single receiver semantics | Competing consumer correctness |
| `LinkCreditTests.cs` | Flow credit / prefetch | AMQP flow control |
| `PeekLockTests.cs` | Complete, abandon, delivery count, broker properties | Lock-based settlement |
| `DeadLetterTests.cs` | Explicit DLQ, auto DLQ on max delivery | Dead-letter queues |
| `MessageTtlTests.cs` | TTL discard, TTL with dead-lettering | Message expiration |
| `BackpressureTests.cs` | Reject when queue full | Backpressure / max_size |
| `CorrelationFilterTests.cs` | Subject filter, app property filter | Subscription filters |

## Build and Deploy

```bash
# Local development
cargo build
RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/fast-servicebus-emulator

# Docker (uses podman)
podman build -t localhost/servicebus-emulator .
podman run -p 5672:5672 localhost/servicebus-emulator

# Kill running emulator
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null
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
pgrep -f azure-servicebus-emulator
```
