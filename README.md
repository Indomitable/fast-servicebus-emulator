# Custom Azure Service Bus Emulator (Rust)

A high-performance, lightweight Azure Service Bus emulator for integration testing.
It implements the AMQP 1.0 protocol and mocks the Azure Service Bus behavior required by official SDKs.

## Features
- **Protocol**: AMQP 1.0 over Plain TCP (Port 5672).
- **Topology**: Static configuration via `topology.yaml`.
- **Authentication**: Mocks CBS handshake (accepts any token).
- **Message Delivery**: Simple "Fire and Forget" (ReceiveAndDelete) model using broadcast channels. No locking, no persistence.

## Configuration
Edit `topology.yaml` to define queues and topics:
```yaml
queues:
  - name: "input-queue"
  - name: "processing-queue"

topics:
  - name: "events-topic"
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
