# Azure Service Bus Emulator — Project Plan

High-performance, lightweight Azure Service Bus emulator in Rust implementing AMQP 1.0 over plain TCP. Works with official Azure SDKs (especially .NET). Uses static YAML topology, mock SASL/CBS auth, and competing-consumer message delivery.

## Design Decisions

- **Protocol**: AMQP 1.0 over plain TCP (port 5672)
- **Topology**: Static config via YAML (`CONFIG_PATH` env var, defaults to `config.yaml` / `/config/topology.yaml` in Docker)
- **Auth**: Mock SASL + mock CBS (accepts any token, returns 200 OK)
- **Delivery**: Competing consumers — each message to exactly one receiver per queue/subscription. Topics fan out to all subscriptions.
- **No Management API**: Static topology only
- **Persistence**: In-memory only
- **Case-insensitive** subscription path matching (`Subscriptions` vs `subscriptions`)
- **fe2o3-amqp**: Consumed as git submodule (`vendor/fe2o3-amqp/`, branch `patch/pipelined-flow-credit`)
- **Docker image**: `localhost/servicebus-emulator` — ~4.58 MB scratch-based, statically-linked musl binary
- **Connection string**: `Endpoint=sb://{host}:{port};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`

## Architecture

```
src/
  main.rs      — Entry point, CONFIG_PATH env var
  lib.rs       — Module declarations
  config.rs    — YAML topology config (QueueConfig, SubscriptionEntry, SubscriptionFilter)
  router.rs    — Message routing, topic fan-out, competing consumers (uses MessageStore)
  server.rs    — AMQP connection/session/link lifecycle
  store.rs     — MessageStore (PeekLock, settlement, TTL) + DlqStore (dead-letter queues)
  sasl.rs      — Mock SASL acceptor
  cbs.rs       — Mock CBS token handler
```

## P1 — Critical Features

Transform the emulator from a simple ReceiveAndDelete pass-through into a production-realistic emulator.

| #  | Feature | Status | Description |
|----|---------|--------|-------------|
| 1  | Topology config enhancement | DONE | Extended YAML schema: lockDuration, maxDeliveryCount, defaultMessageTTL, deadLetteringOnExpiration, subscription filter rules |
| 2  | Message store module | DONE | `MessageStore` with Envelope, SequenceNumber, EnqueuedTimeUtc, DeliveryCount, LockToken, message state machine. `DlqStore` as separate type (no recursive Arc, no unsafe). Integrated with router — replaced `async_channel` with `MessageStore`. |
| 3  | Broker properties stamping | TODO | Stamp SequenceNumber, EnqueuedTimeUtc, DeliveryCount as AMQP message-annotations on outgoing messages |
| 4  | PeekLock mode | TODO | Detect `ReceiverSettleMode::Second` on link attach. Use `receive_and_lock()` instead of `receive_and_delete()`. Send locked messages with lock token in delivery-annotations. |
| 5  | Message settlement | TODO | Handle AMQP disposition frames: Accepted -> complete, Released -> abandon, Rejected/Modified -> dead-letter. Wire up to `MessageStore::complete/abandon/dead_letter`. |
| 6  | Delivery count tracking + auto-DLQ | TODO | Auto-dead-letter when delivery count exceeds `maxDeliveryCount` (logic exists in store, needs wiring) |
| 7  | Dead-letter queue | TODO | `$deadletterqueue` sub-entity per queue/subscription. DLQ receiver links (address `<entity>/$deadletterqueue`). |
| 8  | Message TTL | TODO | Per-message `TimeToLive` + per-entity `DefaultMessageTimeToLive`. Background expiry task to avoid relying on receive-triggered cleanup. |
| 9  | Subscription filters | TODO | SQL filters and correlation filters evaluated during topic fan-out |
| 10 | Backpressure | TODO | AMQP link credit flow control when entity is at capacity |
| 11 | Reject unknown addresses | TODO | Return `amqp:not-found` error instead of silent accept |
| 12 | Verify no regressions | TODO | Run all Rust + .NET tests after all P1 features are implemented |

### Implementation Notes

**P1.3 — Broker properties**: Use `MessageAnnotations::builder()` to set:
- `x-opt-sequence-number` (i64)
- `x-opt-enqueued-time` (Timestamp)
- `x-opt-delivery-count` (i32, for PeekLock — header.delivery_count is also set)

**P1.4 — PeekLock**: On link attach, check `rcv_settle_mode`:
- `ReceiverSettleMode::First` (0) = ReceiveAndDelete (current behavior)
- `ReceiverSettleMode::Second` (1) = PeekLock
- Configure `LinkAcceptor` to support both modes
- Use `sender.send_unsettled()` for PeekLock (returns `Settlement::Unsettled` with outcome receiver)

**P1.5 — Settlement**: Handle disposition from client:
- `DeliveryState::Accepted` -> `store.complete(lock_token)` -> remove message
- `DeliveryState::Released` -> `store.abandon(lock_token)` -> unlock, re-deliver
- `DeliveryState::Rejected` -> `store.dead_letter(lock_token)` -> move to DLQ
- `DeliveryState::Modified { delivery_failed: true }` -> `store.abandon(lock_token)`

**P1.7 — DLQ addresses**: Azure SDK uses `<entity>/$deadletterqueue` or `<entity>/$DeadLetterQueue`. Router already has `get_dlq_store()` and `is_dlq_address()` methods.

**P1.9 — Subscription filters**: During topic fan-out in `router.publish()`, evaluate filter for each subscription before enqueuing. SQL filter needs a simple expression evaluator. Correlation filter matches on message properties.

## P2 — Enhanced Features

| Feature | Description |
|---------|-------------|
| Filter actions | SQL SET/REMOVE on message properties during routing |
| Batch receive | Multiple messages in single receive call |
| Lock renewal | AMQP-level lock renewal via management operations |
| Peek operation | Read without consuming |
| Scheduled messages | `ScheduledEnqueueTimeUtc` — message becomes visible at scheduled time |
| Message sessions | `SessionId`, session lock, session state, session-aware receivers |
| Auto-forwarding | Forward messages from one entity to another |
| Duplicate detection | `MessageId`-based deduplication within configurable window |

## P3 — Polish

| Feature | Description |
|---------|-------------|
| Integration test expansion | .NET tests for PeekLock, DLQ, settlement, TTL, filters |
| Fix test port conflicts | Rust integration tests share port 5672 — use dynamic ports |
| Aspire integration completion | Verify Aspire hosting works end-to-end |
| Docker image optimization | Review multi-stage build, minimize layers |

## Test Infrastructure

- **Rust unit tests**: `cargo test --lib` (57 tests)
- **Rust integration tests**: `cargo test --test <name>` (5 tests: queue, topic, CBS, 2x stress)
- **.NET integration tests**: 8 tests (EmulatorTests, SingleReceiverTests, LinkCreditTests)
- **Test isolation**: .NET tests share emulator on port 5672, use separate queues/topics per test

## Key Files

| File | Purpose |
|------|---------|
| `src/store.rs` | MessageStore + DlqStore — PeekLock, settlement, TTL, delivery tracking |
| `src/router.rs` | Message routing with MessageStore, topic fan-out, address resolution |
| `src/server.rs` | AMQP server — connection/session/link lifecycle |
| `src/config.rs` | YAML topology config with queue/subscription properties and filters |
| `src/sasl.rs` | Mock SASL (accepts PLAIN, ANONYMOUS, EXTERNAL, MSSBCBS) |
| `src/cbs.rs` | Mock CBS token handler |
| `config.yaml` | Default topology (3 queues, 2 topics) |
| `Cargo.toml` | Dependencies: tokio, fe2o3-amqp, serde, uuid |

## fe2o3-amqp Patches

Three race conditions fixed in the vendored submodule (branch `patch/pipelined-flow-credit`):

1. **Session relay not registered for pipelined frames** — `acceptor/connection.rs`
2. **Flow/Transfer frames crash session for not-yet-accepted links** — `acceptor/session.rs`
3. **Pipelined Flow credit lost, causing sender links to block forever** — `acceptor/session.rs`
