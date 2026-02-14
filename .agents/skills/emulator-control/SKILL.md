---
name: emulator-control
description: Manage Azure Service Bus emulator lifecycle (start, stop, restart, status, logs)
license: MIT OR Apache-2.0
compatibility: opencode
metadata:
  audience: developers
  workflow: local-development
---

## What I do

I provide full lifecycle management for the Azure Service Bus emulator:
- Start the emulator with custom configuration and log levels
- Stop running instances (gracefully or force)
- Restart the emulator with new settings
- Check if the emulator is running and healthy
- Tail and search emulator logs
- Verify AMQP port 5672 is listening

## When to use me

Use me whenever you need to control the emulator:
- "Start the emulator"
- "Stop the emulator"
- "Restart the emulator with debug logging"
- "Is the emulator running?"
- "Show me the emulator logs"
- "Start emulator with a custom config"

## Commands

### Start emulator

Default (debug logging):
```bash
RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
```

If not yet built:
```bash
cargo build && RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
```

With custom config:
```bash
RUST_LOG=debug CONFIG_PATH=/path/to/custom.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
```

With custom log level (trace, debug, info, warn, error):
```bash
RUST_LOG=trace CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
```

After starting, always verify:
```bash
sleep 2 && ss -tlnp | grep 5672
```

### Stop emulator

Graceful:
```bash
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null
```

Force (if graceful fails):
```bash
kill -9 $(pgrep -f azure-servicebus-emulator) 2>/dev/null
```

### Restart emulator

```bash
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null
sleep 1
RUST_LOG=debug CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
sleep 2
ss -tlnp | grep 5672
```

### Check status

Process running:
```bash
pgrep -f azure-servicebus-emulator
```

Port listening:
```bash
ss -tlnp | grep 5672
```

Full health check (both process AND port):
```bash
pgrep -f azure-servicebus-emulator && ss -tlnp | grep 5672 && echo "Emulator is healthy" || echo "Emulator is NOT running"
```

### View logs

Last 50 lines:
```bash
tail -50 /tmp/emulator.log
```

Follow live:
```bash
tail -f /tmp/emulator.log
```

Search for errors:
```bash
grep -i "error\|warn\|panic" /tmp/emulator.log
```

Search for specific address/queue:
```bash
grep "input-queue" /tmp/emulator.log
```

## Configuration

### Default topology (`config.yaml`)

12 queues:
- `input-queue`, `processing-queue`, `prefetch-queue` -- basic queues
- `peeklock-complete-queue`, `peeklock-abandon-queue`, `peeklock-deliverycount-queue`, `peeklock-brokerprops-queue` -- PeekLock test queues
- `dlq-explicit-queue`, `dlq-auto-queue` (max_delivery_count: 2) -- dead-letter test queues
- `ttl-discard-queue`, `ttl-dlq-queue` (default_message_ttl_secs: 2) -- TTL test queues
- `backpressure-queue` (max_size: 10) -- backpressure test queue

4 topics:
- `events-topic` -- subscriptions: `sub-1`, `sub-2`
- `competing-topic` -- subscription: `shared-sub`
- `filter-topic` -- subscriptions: `orders-sub` (subject filter), `all-sub`
- `filter-appprop-topic` -- subscriptions: `region-sub` (property filter), `catch-all-sub`

### Config options per queue/subscription

| Option | Default | Description |
|--------|---------|-------------|
| `lock_duration_secs` | 30 | PeekLock lock duration |
| `max_delivery_count` | 10 | Auto-dead-letter threshold |
| `default_message_ttl_secs` | 0 (no TTL) | Message expiration |
| `dead_lettering_on_message_expiration` | false | Move expired to DLQ |
| `max_size` | none | Backpressure limit |

### Subscription filters

Only correlation filters are supported:
```yaml
subscriptions:
  - name: orders-sub
    filter:
      type: Correlation
      subject: "order-created"
  - name: region-sub
    filter:
      type: Correlation
      properties:
        region: "us-east"
```

SQL filters are parsed but not evaluated (log warning, match all messages).

## Port and process details

- **Port**: 5672 (AMQP 1.0 over plain TCP, no TLS)
- **Bind address**: 0.0.0.0 (all interfaces)
- **Process name**: `azure-servicebus-emulator`
- **Default log file**: `/tmp/emulator.log`
- **Binary location**: `target/debug/azure-servicebus-emulator` (debug) or `target/release/azure-servicebus-emulator` (release)

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_PATH` | `config.yaml` | Path to topology YAML file |
| `RUST_LOG` | `info` | Tracing log level filter |

## Common issues

### Port already in use
- Symptom: `Address already in use (os error 98)`
- Fix: `kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null; sleep 1`

### Emulator crashes on startup
- Symptom: Process exits immediately
- Debug: `cat /tmp/emulator.log` -- check for config parse errors
- Fix: Validate `config.yaml` syntax

### No output in log file
- Symptom: `/tmp/emulator.log` is empty
- Fix: Ensure `RUST_LOG` is set (default `info` may suppress startup messages, use `debug`)

## Docker

Build image (uses podman):
```bash
podman build -t localhost/servicebus-emulator .
```

Run container:
```bash
podman run -p 5672:5672 localhost/servicebus-emulator
```

The Docker image is scratch-based (~4.58 MB) with a statically-linked musl binary. Config path inside container: `/config/config.yaml`.

## Integration with testing

- **Rust integration tests** bind to port 5672 themselves. Stop the emulator before running `cargo test`.
- **.NET integration tests** connect to a running emulator on port 5672. Start the emulator before running `dotnet test`.
- Use the `test-runner` skill to automatically manage the emulator lifecycle during the full test suite.
