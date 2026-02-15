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
