---
name: test-runner
description: Run complete test suite for Azure Service Bus emulator (Rust unit, Rust integration, .NET integration) with automatic emulator lifecycle management
license: MIT OR Apache-2.0
compatibility: opencode
metadata:
  audience: developers
  workflow: testing
---

## What I do

I orchestrate the full test workflow for the Azure Service Bus emulator:
- Kill any running emulator instances on port 5672
- Build the emulator binary
- Start the emulator in background with debug logging
- Run all Rust unit tests (`cargo test --lib`)
- Run all Rust integration tests (`cargo test`)
- Run all .NET integration tests (`dotnet test`)
- Capture failures with full context and logs
- Clean up emulator process when done

## When to use me

Use me when you need to verify tests pass after making changes:
- After fixing bugs or adding features
- Before committing changes
- After modifying AMQP protocol handling, message routing, or store logic
- Before creating a pull request

## Step-by-step workflow

### Phase 1: Cleanup and build
```bash
# Kill any running emulator
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null; sleep 1

# Verify port is free
ss -tlnp | grep 5672

# Build the emulator
cargo build
```

### Phase 2: Rust unit tests (emulator must NOT be running)
```bash
cargo test --lib
```
Expected: ~57 tests pass. These test config parsing, message store operations, router logic, correlation filters, TTL, backpressure, broker property stamping.

### Phase 3: Rust integration tests (emulator must NOT be running)
```bash
cargo test --test queue_test
cargo test --test topic_test
cargo test --test cbs_test
cargo test --test stress_test
```
These start their own emulator instances on port 5672. The emulator must NOT be running already.

Alternatively, run all at once:
```bash
cargo test
```

### Phase 4: .NET integration tests (emulator MUST be running)
```bash
# Start emulator in background
RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &

# Wait for startup
sleep 2

# Verify emulator is listening
ss -tlnp | grep 5672

# Build and run .NET tests
cd integration/AzureServiceBusEmulator.IntegrationTests
dotnet build
dotnet test --no-build -v n
```

To run a single .NET test:
```bash
dotnet test --no-build -v n --filter "TestMethodName"
```

### Phase 5: Cleanup
```bash
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null
```

## Test categories

### Rust Unit Tests (~57 tests, inline `#[cfg(test)]` modules)
| File | What it tests |
|------|---------------|
| `src/config.rs` | YAML parsing, defaults, filters, edge cases |
| `src/store.rs` | Enqueue/receive, PeekLock lifecycle, settlement, auto-DLQ, lock expiry, TTL, backpressure |
| `src/router.rs` | Queue/topic routing, fan-out, competing consumers, address resolution, broker properties, correlation filters |
| `src/server.rs` | patch_attach_if_needed scenarios |
| `src/sasl.rs` | Mechanism listing, accept any init/response |
| `src/cbs.rs` | Response building, correlation, state init |

### Rust Integration Tests (4 files in `tests/`)
| File | What it tests |
|------|---------------|
| `queue_test.rs` | AMQP queue send/receive over TCP |
| `topic_test.rs` | AMQP topic fanout over TCP |
| `cbs_test.rs` | CBS put-token handshake over TCP |
| `stress_test.rs` | Sequential and concurrent connections |

### .NET Integration Tests (20 tests, 9 files)
| File | What it tests |
|------|---------------|
| `EmulatorTests.cs` | Queue send/receive, topic fanout |
| `SingleReceiverTests.cs` | Competing consumer correctness |
| `LinkCreditTests.cs` | Flow credit / prefetch |
| `PeekLockTests.cs` | Complete, abandon, delivery count, broker properties |
| `DeadLetterTests.cs` | Explicit DLQ, auto DLQ on max delivery |
| `MessageTtlTests.cs` | TTL discard, TTL with dead-lettering |
| `BackpressureTests.cs` | Reject when queue full |
| `CorrelationFilterTests.cs` | Subject filter, app property filter |

Connection string: `Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`

## Common test failures and fixes

### Port already in use
- Symptom: `Address already in use (os error 98)`
- Fix: Kill existing emulator: `kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null`

### .NET tests timeout
- Symptom: Tests hang or timeout after 30s
- Debug: `tail -50 /tmp/emulator.log` for AMQP protocol errors
- Common causes: Missing broker properties, disposition echo issues
- Use the `amqp-debug` skill for detailed diagnosis

### Delivery count assertion fails
- Symptom: `DeliveryCount_Increments_On_Abandon` reports count=2 instead of 1
- Fix: `stamp_broker_properties()` in `src/router.rs` must use `delivery_count.saturating_sub(1)`

### "Nullable object must have a value"
- Symptom: .NET SDK throws on first message receive
- Fix: `header.delivery_count` must always be serialized (manual Serialize impl in `vendor/fe2o3-amqp/fe2o3-amqp-types/src/messaging/format/header.rs`)

## Files involved

- `Cargo.toml` - Rust project definition
- `config.yaml` - Static topology (12 queues, 4 topics)
- `src/**/*.rs` - Emulator source (8 files)
- `tests/*.rs` - Rust integration tests (4 files)
- `integration/AzureServiceBusEmulator.IntegrationTests/*.cs` - .NET tests (9 files)
- `/tmp/emulator.log` - Emulator runtime logs
