---
name: amqp-debug
description: Troubleshoot AMQP 1.0 protocol issues and Azure SDK compatibility problems with the Service Bus emulator
license: MIT OR Apache-2.0
compatibility: opencode
metadata:
  audience: developers
  workflow: debugging
---

## What I do

I help diagnose and fix AMQP 1.0 protocol issues between the emulator and Azure SDKs:
- Parse AMQP frames from emulator logs
- Identify protocol violations (missing required fields, invalid states)
- Check for known Azure SDK bugs and their workarounds
- Suggest fixes based on 5 documented bug fixes in this project
- Validate message broker properties
- Debug disposition settlement issues (complete, abandon, reject, dead-letter)
- Diagnose flow credit exhaustion

## When to use me

Use me when you encounter:
- .NET SDK timeout errors
- "Nullable object must have a value" exceptions
- Messages stuck in PeekLock state
- Disposition (complete/abandon) failures
- Delivery count mismatches
- Lock tokens not recognized by the SDK
- Subscription filter not matching
- Flow credit exhaustion (sender blocked forever)
- Any AMQP protocol error in emulator logs

## Debugging workflow

### Step 1: Identify the symptom
- SDK exception message or test failure
- Timeout location: send vs receive vs settlement
- Which test file and method fails

### Step 2: Check emulator logs
```bash
tail -100 /tmp/emulator.log | grep -i "error\|warn\|panic"
```

Key patterns to look for:
- `InitialDeliveryCountIsNone` -- Bug #1 (Azure SDK missing field)
- Disposition not sent back -- Bug #2 (tail chunk)
- Dead-letter timeout -- Bug #3 (echo state)
- Wrong delivery count -- Bug #4 (off-by-one)
- "Nullable object" -- Bug #5 (header serialization)

### Step 3: Enable trace logging
```bash
kill $(pgrep -f azure-servicebus-emulator) 2>/dev/null; sleep 1
RUST_LOG=trace CONFIG_PATH=config.yaml ./target/debug/azure-servicebus-emulator > /tmp/emulator.log 2>&1 &
```

### Step 4: Inspect AMQP frames in logs
- **Attach frames**: Check `initial-delivery-count`, `role` (sender=true vs receiver=false)
- **Transfer frames**: Check `header.delivery_count`, delivery tag, message annotations
- **Disposition frames**: Check `state` (Accepted, Rejected, Released, Modified), `settled` flag
- **Flow frames**: Check `link-credit`, `available`, `drain` mode

### Step 5: Cross-reference with Azure SDK source
- .NET SDK: `vendor/azure-sdk-for-net/sdk/servicebus/Azure.Messaging.ServiceBus/src/`
- AMQP library: `vendor/azure-amqp/src/`

Key SDK files:
- `AmqpMessageConverter.cs` -- message property mapping
- `AmqpSender.cs` / `AmqpReceiver.cs` -- link operations
- `Framing/Attach.cs`, `Framing/Transfer.cs`, `Framing/Disposition.cs` -- frame definitions

## Known bugs and workarounds

### Bug #1: Missing initial-delivery-count (Azure SDK bug)

**Symptom**: `ReceiverAttachError::InitialDeliveryCountIsNone` in emulator logs when SDK opens a sender link.

**Root cause**: `Microsoft.Azure.Amqp` omits the mandatory AMQP `initial-delivery-count` field on sender Attach frames in several code paths:
- `RequestResponseAmqpLink` constructor (CBS links) -- `vendor/azure-amqp/src/RequestResponseAmqpLink.cs:70-77`
- `AmqpLinkSettings.Create()` -- `vendor/azure-amqp/src/AmqpLinkSettings.cs:175-195`
- Validation in `Attach.EnsureRequired()` is commented out -- `vendor/azure-amqp/src/Framing/Attach.cs:134-137`

**Workaround**: `patch_attach_if_needed()` in `src/server.rs` patches incoming sender attaches with `initial_delivery_count = Some(0)`.

**Log signature**:
```
WARN patch_attach_if_needed: Added missing initial_delivery_count=0
```

**Files**: `src/server.rs`, `BUG_REPORT_INITIAL_DELIVERY_COUNT.md`

---

### Bug #2: Tail chunk missing in disposition echo (fe2o3-amqp bug)

**Symptom**: Disposition not echoed back to SDK. SDK waits for echo, times out, falls back to unimplemented `$management` link.

**Root cause**: `on_incoming_disposition()` in fe2o3-amqp used `consecutive_chunk_indices()` (`.windows(2)`) to group delivery IDs into ranges for echo Dispositions. The loop body never processed the tail chunk. For a single delivery ID, `windows(2)` returns empty, so zero echo Dispositions were sent.

**Fix**: Added tail chunk handler after the loop at `vendor/fe2o3-amqp/fe2o3-amqp/src/session/mod.rs:735-746`.

**Files**: `vendor/fe2o3-amqp/fe2o3-amqp/src/session/mod.rs`

---

### Bug #3: Echo disposition state must be Accepted (fe2o3-amqp bug)

**Symptom**: Dead-letter operation fails with timeout. SDK sends `Rejected` disposition, receives `Rejected` echo back, falls back to unimplemented `$management` link.

**Root cause**: fe2o3-amqp echoed back the SAME disposition state the SDK sent (e.g., `Rejected`). The Azure SDK checks the echoed state -- if it's not `Accepted`, it falls back to the management link. Real Azure Service Bus always echoes `Accepted` regardless of incoming state.

**Fix**: Changed echo to `DeliveryState::Accepted(Accepted {})` in `vendor/fe2o3-amqp/fe2o3-amqp/src/session/mod.rs`.

**Log signature** (after fix):
```
DEBUG received disposition: first=1, last=1, state=Rejected
DEBUG echoing disposition: first=1, last=1, state=Accepted
```

**Files**: `vendor/fe2o3-amqp/fe2o3-amqp/src/session/mod.rs`

---

### Bug #4: Delivery count off-by-one

**Symptom**: .NET test `DeliveryCount_Increments_On_Abandon` fails. SDK reports delivery count = 2 on first delivery instead of 1.

**Root cause**: AMQP `header.delivery_count` counts *prior delivery attempts* (0-based). The Azure SDK adds 1 to this value:
```csharp
// AmqpMessageConverter.cs
annotatedMessage.Header.DeliveryCount = annotatedMessage.Header.DeliveryCount + 1;
```
Our store used 1-based delivery count internally and passed it directly to the header, so SDK computed `1 + 1 = 2`.

**Fix**: `let amqp_delivery_count = delivery_count.saturating_sub(1)` in `stamp_broker_properties()`.

**Files**: `src/router.rs:524` (`stamp_broker_properties()`)

---

### Bug #5: Header delivery_count omitted when zero

**Symptom**: .NET SDK throws `InvalidOperationException: Nullable object must have a value` on first message receive.

**Root cause**: The `SerializeComposite` derive macro on AMQP `Header` struct uses `#[amqp_contract(default)]` on `delivery_count`. The underlying `buffer_if_eq_default!` macro (`serde_amqp_derive/src/util.rs:285`) skips serializing trailing fields that equal their default. Since `delivery_count` (default 0) is the LAST field in `Header`, when it's 0, it's omitted from the wire format entirely.

The SDK reads `DeliveryCount` via `.Value` on a nullable -- if null (field absent), it throws.

**Fix**: Removed `SerializeComposite` from the derive list and wrote a manual `Serialize` impl that always serializes `delivery_count`. Critical detail: must use `DESCRIBED_LIST` encoding (not `DESCRIBED_BASIC`). The derive macro for `encoding = "list"` uses `serializer.serialize_struct(DESCRIBED_LIST, len + 1)`.

**Files**: `vendor/fe2o3-amqp/fe2o3-amqp-types/src/messaging/format/header.rs`

---

### Workaround: Lock token in delivery tag

**Detail**: Azure .NET SDK reads lock tokens from the AMQP delivery tag, NOT from `x-opt-lock-token`:
```csharp
// AmqpMessageConverter.cs
new Guid(amqpMessage.DeliveryTag.Array)
```

**Implementation**: Modified fe2o3-amqp to support custom delivery tags via `Sendable::builder().delivery_tag(lock_token.as_bytes())`.

**Files**: `src/router.rs` (PeekLock handlers), `vendor/fe2o3-amqp/fe2o3-amqp/src/link/delivery.rs`

## Broker properties reference

Messages sent from emulator to SDK MUST include these properties:

| Property | Location | Type | Notes |
|----------|----------|------|-------|
| `x-opt-sequence-number` | message_annotations | i64 | Monotonic per queue/subscription |
| `x-opt-enqueued-time` | message_annotations | Timestamp (ms) | Unix epoch milliseconds |
| `x-opt-lock-token` | message_annotations | Uuid | PeekLock only (SDK actually reads from delivery tag) |
| `delivery_count` | header | u32 | MUST always serialize, even when 0. Value = store_count - 1 |

Stamped in: `stamp_broker_properties()` at `src/router.rs:524`

## Disposition settlement reference

### Accepted (Complete)
- SDK sends: `Disposition { state: Accepted, settled: true }`
- Emulator action: Remove message from store
- Echo back: `Accepted`
- Implementation: `MessageStore::complete()` in `src/store.rs`

### Rejected (Dead-letter)
- SDK sends: `Disposition { state: Rejected { error }, settled: true }`
- Emulator action: Move message to DLQ store
- Echo back: `Accepted` (NOT `Rejected` -- see Bug #3)
- Implementation: `MessageStore::dead_letter()` in `src/store.rs`

### Released (Abandon)
- SDK sends: `Disposition { state: Released, settled: true }`
- Emulator action: Unlock message, increment delivery count. If count > max_delivery_count, auto-move to DLQ.
- Echo back: `Accepted`
- Implementation: `MessageStore::abandon()` in `src/store.rs`

## Flow credit debugging

### Sender blocked forever
**Symptom**: `send()` never completes, no error returned.

**Possible causes**:
1. Receiver never sent Flow frame granting credit
2. Flow frame lost due to pipelined link attach (fixed in fe2o3-amqp `patch/pipelined-flow-credit` branch)
3. Credit exhausted and receiver closed without draining

**Debug**:
```bash
grep -i "flow\|credit" /tmp/emulator.log
```

Look for:
- `Received Flow: link_credit=X` -- credit granted
- `Sending Transfer` -- message sent (consumes 1 credit)
- `link credit exhausted` -- sender blocked

### Backpressure (queue full)
**Symptom**: Send fails with `ServiceBusException(QuotaExceeded)` on the .NET side.

**Expected behavior**: When `logical_count >= max_size`, emulator rejects with `Disposition { state: Rejected { error: amqp:resource-limit-exceeded } }`.

**Debug**:
```bash
grep "resource-limit-exceeded\|max_size\|logical_count" /tmp/emulator.log
```

**Log signature**:
```
WARN enqueue rejected: queue full (logical_count=10, max_size=10)
```

**Implementation**: `MessageStore::enqueue()` in `src/store.rs` checks `logical_count` against `max_size`.

## Correlation filter debugging

**Symptom**: Messages not delivered to filtered subscription, or delivered when they shouldn't be.

**How filters work**: `matches_filter()` in `src/router.rs` evaluates correlation filters against message properties. A filter matches if ALL specified fields match (AND logic). Empty/unset filter fields are ignored.

Supported filter fields:
- `subject` -- matches `message.properties.subject`
- `message_id` -- matches `message.properties.message_id`
- `correlation_id` -- matches `message.properties.correlation_id`
- `to` -- matches `message.properties.to`
- `reply_to` -- matches `message.properties.reply_to`
- `content_type` -- matches `message.properties.content_type`
- `properties` (map) -- matches `message.application_properties`

**SQL filters**: Parsed but NOT evaluated. Log a warning, match all messages.

**Debug**:
```bash
grep -i "filter\|matches\|subscription" /tmp/emulator.log
```

## Common log patterns

### Successful message flow
```
DEBUG accepting session
DEBUG accepting link: role=Sender, address=input-queue
DEBUG handle_incoming_messages: received message
DEBUG published to input-queue, seq=1
DEBUG accepting link: role=Receiver, address=input-queue
DEBUG sending message seq=1, delivery_count=0
DEBUG received disposition: delivery_id=0, state=Accepted
```

### Backpressure rejection
```
WARN enqueue rejected: queue full (logical_count=10, max_size=10)
DEBUG sending Disposition: state=Rejected(amqp:resource-limit-exceeded)
```

### Topic fan-out
```
DEBUG published to events-topic, fan-out to 2 subscriptions
DEBUG enqueued to events-topic/Subscriptions/sub-1, seq=1
DEBUG enqueued to events-topic/Subscriptions/sub-2, seq=1
```

### Dead-letter (auto, max delivery exceeded)
```
DEBUG abandon: delivery_count=3 exceeds max_delivery_count=2, moving to DLQ
```

## Key source files

| File | What to look at |
|------|-----------------|
| `src/server.rs` | Connection/session/link handling, `patch_attach_if_needed()` |
| `src/router.rs` | Message routing, `stamp_broker_properties()`, disposition handlers, `matches_filter()` |
| `src/store.rs` | MessageStore, DlqStore, settlement operations, backpressure |
| `src/cbs.rs` | Mock CBS token handling |
| `vendor/fe2o3-amqp/fe2o3-amqp/src/session/mod.rs` | Disposition echo (Bugs #2, #3) |
| `vendor/fe2o3-amqp/fe2o3-amqp-types/src/messaging/format/header.rs` | Header serialization (Bug #5) |
| `vendor/fe2o3-amqp/fe2o3-amqp/src/link/delivery.rs` | Custom delivery tag support |
| `vendor/azure-sdk-for-net/` | .NET SDK source (read-only reference) |
| `vendor/azure-amqp/` | Microsoft.Azure.Amqp source (read-only reference) |

## Advanced: Packet capture

For deep protocol analysis, capture AMQP frames with tcpdump:
```bash
sudo tcpdump -i lo -w /tmp/amqp.pcap port 5672
```

Analyze with Wireshark using the `amqp` protocol filter.
