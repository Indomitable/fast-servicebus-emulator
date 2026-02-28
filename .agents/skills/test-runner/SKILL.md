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
kill $(pgrep -f fast-servicebus-emulator) 2>/dev/null; sleep 1

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

To run a single .NET test:
```bash
dotnet test --no-build -v n --filter "TestMethodName"
```

### Phase 5: Cleanup
```bash
kill $(pgrep -f fast-servicebus-emulator) 2>/dev/null
```
