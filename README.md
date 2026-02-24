# Fast Azure Service Bus Emulator

A high-performance, lightweight Azure Service Bus emulator for integration testing.
It implements the AMQP 1.0 protocol and mocks the Azure Service Bus behavior required by official SDKs.

## Features
- **Protocol**: AMQP 1.0 over plain TCP (port 5672).
- **Topology**: Static YAML topology (`config.yaml` / `CONFIG_PATH`) with queues, topics, and subscriptions.
- **Mock auth**: SASL accepts any mechanism/credentials; CBS accepts any token (200 OK).
- **Delivery modes**: ReceiveAndDelete and PeekLock.
- **Settlement**: Complete/Abandon/Dead-letter; auto-dead-letter on `max_delivery_count`.
- **DLQ**: `<entity>/$deadletterqueue` (also accepts `$DeadLetterQueue`).
- **TTL**: Per-message TTL + per-entity default TTL; optional dead-letter-on-expiration.
- **Backpressure**: Per-entity `max_size` rejects sends with `amqp:resource-limit-exceeded` when full.
- **Subscription filters**: Correlation filters (system properties + application properties). SQL filters are parsed but not evaluated (warning, match-all).
- **Compatibility workarounds**: Patches common Azure SDK quirks (e.g. missing `initial-delivery-count`, lock-token in delivery tag, always serialize header `delivery_count`).
- **Operational**: Ctrl+C / SIGTERM stops accepting new connections.
- **Testing Admin API**: HTTP API on port `45672` for resetting, inspecting, and injecting messages during tests.

## Configuration
Edit `config.yaml` to define queues, topics and other configurations:
```yaml
topology:
    queues:
      - name: "input-queue"
      - name: "processing-queue"
    
    topics:
      - name: "events-topic"
        subscriptions:
          - "sub-1"
          - "sub-2"
```

## Usage
Run the emulator:
```bash
cargo run
```

AMQP listens on `5672` and the testing admin API listens on `45672`.

## Testing Admin API

Base URL: `http://localhost:45672`

### Inspect and reset messages
- `GET /testing/messages`
- `DELETE /testing/messages`
- `GET /testing/messages/queues/:queue`
- `DELETE /testing/messages/queues/:queue`
- `GET /testing/messages/topics/:topic`
- `DELETE /testing/messages/topics/:topic`
- `GET /testing/messages/topics/:topic/subscriptions/:subscription`
- `DELETE /testing/messages/topics/:topic/subscriptions/:subscription`

### Inject messages via REST
- `POST /testing/messages/queues/:queue`
- `POST /testing/messages/topics/:topic`

Request body is the message payload.

Supported message-property headers:
- `X-MESSAGE-SUBJECT`
- `X-MESSAGE-MESSAGE-ID`
- `X-MESSAGE-USER-ID`
- `X-MESSAGE-TO`
- `X-MESSAGE-REPLY-TO`
- `X-MESSAGE-CORRELATION-ID`
- `X-MESSAGE-CONTENT-TYPE`
- `X-MESSAGE-GROUP-ID`
- `X-MESSAGE-REPLY-TO-GROUP-ID`
- `X-MESSAGE-ABSOLUTE-EXPIRY-TIME` (epoch millis or RFC3339)

Application properties can be added with repeatable headers (preserves key casing):
- `X-MESSAGE-PROPERTY: <name>=<value>`

Example:
```bash
curl -X POST "http://localhost:45672/testing/messages/topics/filter-appprop-topic" \
  -H "X-MESSAGE-SUBJECT: order-created" \
  -H "X-MESSAGE-PROPERTY: Region=us-east" \
  -H "X-MESSAGE-ABSOLUTE-EXPIRY-TIME: 2026-12-31T23:59:59Z" \
  --data "hello from admin api"
```

## Integration Testing
Configure your Azure Service Bus SDK to connect to `amqp://localhost:5672`.
Use any SAS Key (the emulator accepts anything).
Ensure your client uses `TransportType.Amqp` (Plain TCP) if possible, or trust the connection.

## Development
Run tests:
```bash
cargo test
```

# Docker Image
Run either using:
```bash
podman kube play podman-pod.yaml
```
and kill with 
```bash
podman kube down podman-pod.yaml
```

or using compose
```bash
podman compose up fast-servicebus-emulator 
```
and kill with
```bash
podman compose down fast-servicebus-emulator
```
