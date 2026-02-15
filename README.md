# Fast Azure Service Bus Emulator

A high-performance, lightweight Azure Service Bus emulator for integration testing.
It implements the AMQP 1.0 protocol and mocks the Azure Service Bus behavior required by official SDKs.

## Features
- **Protocol**: AMQP 1.0 over Plain TCP (Port 5672).
- **Configuration**: Static configuration via `config.yaml`.
- **Authentication**: Mocks CBS handshake (accepts any token).
- **Message Delivery**: Simple "Fire and Forget" (ReceiveAndDelete) model using broadcast channels. No locking, no persistence.

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
