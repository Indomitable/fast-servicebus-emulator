//! Message router with competing-consumer queues and topic fan-out.
//!
//! Each queue and each topic subscription gets a `MessageStore`.
//! When multiple receivers subscribe to the same queue or the same
//! subscription, each message is delivered to exactly **one** receiver
//! (competing consumers).
//!
//! Topics themselves have no store — publishing to a topic sends the
//! message to every subscription's store independently.
//!
//! Subscription paths like `events-topic/subscriptions/sub-1` are
//! resolved case-insensitively.

use anyhow::Result;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::types::messaging::{Body, Header, Message, MessageAnnotations};
use fe2o3_amqp::types::primitives::{Timestamp, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use fe2o3_amqp_types::definitions::{AmqpError, Error as AmqpErrorDef};
use crate::config::{
    QueueConfig, SubscriptionEntry, SubscriptionFilter, Topology, DEFAULT_CHANNEL_CAPACITY,
};
use crate::store::{DlqStore, EntityConfig, LockToken, MessageState, MessageStore};

/// Type alias for a message routed through the stores.
pub type RouterMessage = Message<Body<Value>>;

/// Info about a subscription for filter evaluation during fan-out.
#[derive(Clone, Debug)]
struct SubscriptionInfo {
    /// The lowercased store key (e.g. `events-topic/subscriptions/sub-1`).
    key: String,
    /// Optional filter; `None` means accept all messages (default / TrueFilter).
    filter: Option<SubscriptionFilter>,
}

/// Immutable message router. Created once at startup from the topology
/// configuration and shared across all connections via `Arc`.
#[derive(Clone)]
pub struct Router {
    /// Message stores for queues and subscriptions. Keyed by queue name or
    /// the lowercased subscription path (e.g. `events-topic/subscriptions/sub-1`).
    stores: HashMap<String, Arc<MessageStore>>,

    /// Dead-letter queue stores for each entity.
    dlq_stores: HashMap<String, Arc<DlqStore>>,

    /// Maps topic name → list of subscription info (key + optional filter).
    /// Used by `publish()` to fan out to matching subscriptions.
    topic_subscriptions: HashMap<String, Vec<SubscriptionInfo>>,

    /// Maps lowercased subscription paths to their store key.
    subscription_index: HashMap<String, String>,

    /// Set of topic names, so we can distinguish topics from queues
    /// in `resolve_address` and `has_address`.
    topics: std::collections::HashSet<String>,
}

/// Converts a `QueueConfig` to an `EntityConfig`.
fn queue_entity_config(q: &QueueConfig) -> EntityConfig {
    EntityConfig {
        lock_duration: std::time::Duration::from_secs(q.lock_duration_secs),
        max_delivery_count: q.max_delivery_count,
        default_message_ttl_ms: q.default_message_ttl_secs * 1000,
        dead_lettering_on_expiration: q.dead_lettering_on_message_expiration,
        max_size: if q.max_size > 0 { q.max_size } else { DEFAULT_CHANNEL_CAPACITY },
    }
}

/// Converts a `SubscriptionEntry` to an `EntityConfig`.
fn sub_entity_config(sub: &SubscriptionEntry) -> EntityConfig {
    EntityConfig {
        lock_duration: std::time::Duration::from_secs(sub.lock_duration_secs()),
        max_delivery_count: sub.max_delivery_count(),
        default_message_ttl_ms: sub.default_message_ttl_secs() * 1000,
        dead_lettering_on_expiration: sub.dead_lettering_on_message_expiration(),
        max_size: if sub.max_size() > 0 { sub.max_size() } else { DEFAULT_CHANNEL_CAPACITY },
    }
}

/// Extracts a string representation from a `MessageId` for filter comparison.
fn message_id_to_string(id: &fe2o3_amqp_types::messaging::MessageId) -> String {
    match id {
        fe2o3_amqp_types::messaging::MessageId::String(s) => s.clone(),
        fe2o3_amqp_types::messaging::MessageId::Ulong(n) => n.to_string(),
        fe2o3_amqp_types::messaging::MessageId::Uuid(u) => format!("{:?}", u),
        fe2o3_amqp_types::messaging::MessageId::Binary(b) => {
            // Hex-encode binary IDs
            b.iter().map(|byte| format!("{:02x}", byte)).collect()
        }
    }
}

/// Extracts a string from a `SimpleValue` for correlation filter property matching.
fn simple_value_to_string(
    val: &fe2o3_amqp_types::primitives::SimpleValue,
) -> Option<String> {
    use fe2o3_amqp_types::primitives::SimpleValue;
    match val {
        SimpleValue::String(s) => Some(s.clone()),
        SimpleValue::Bool(b) => Some(b.to_string()),
        SimpleValue::Ubyte(n) => Some(n.to_string()),
        SimpleValue::Ushort(n) => Some(n.to_string()),
        SimpleValue::Uint(n) => Some(n.to_string()),
        SimpleValue::Ulong(n) => Some(n.to_string()),
        SimpleValue::Byte(n) => Some(n.to_string()),
        SimpleValue::Short(n) => Some(n.to_string()),
        SimpleValue::Int(n) => Some(n.to_string()),
        SimpleValue::Long(n) => Some(n.to_string()),
        SimpleValue::Symbol(s) => Some(s.to_string()),
        _ => None, // Float, Decimal, Binary, etc. — not commonly used in correlation filters
    }
}

/// Checks whether a message matches a subscription filter.
///
/// - `None` filter means accept all messages (equivalent to Azure's TrueFilter).
/// - `Correlation` filter matches on exact equality of system properties
///   (correlation_id, message_id, to, reply_to, subject, content_type) and
///   custom application properties. All specified fields must match.
/// - `Sql` filter is not implemented — logs a warning and accepts all messages.
fn matches_filter(message: &RouterMessage, filter: &Option<SubscriptionFilter>) -> bool {
    let filter = match filter {
        None => return true,
        Some(f) => f,
    };

    match filter {
        SubscriptionFilter::Correlation {
            correlation_id,
            message_id,
            to,
            reply_to,
            subject,
            content_type,
            properties,
        } => {
            let msg_props = message.properties.as_ref();

            // Check system properties — each configured field must match exactly.
            if let Some(expected) = correlation_id {
                let actual = msg_props
                    .and_then(|p| p.correlation_id.as_ref())
                    .map(message_id_to_string);
                if actual.as_deref() != Some(expected.as_str()) {
                    return false;
                }
            }

            if let Some(expected) = message_id {
                let actual = msg_props
                    .and_then(|p| p.message_id.as_ref())
                    .map(message_id_to_string);
                if actual.as_deref() != Some(expected.as_str()) {
                    return false;
                }
            }

            if let Some(expected) = to {
                let actual = msg_props.and_then(|p| p.to.as_deref());
                if actual != Some(expected.as_str()) {
                    return false;
                }
            }

            if let Some(expected) = reply_to {
                let actual = msg_props.and_then(|p| p.reply_to.as_deref());
                if actual != Some(expected.as_str()) {
                    return false;
                }
            }

            if let Some(expected) = subject {
                let actual = msg_props.and_then(|p| p.subject.as_deref());
                if actual != Some(expected.as_str()) {
                    return false;
                }
            }

            if let Some(expected) = content_type {
                let actual = msg_props
                    .and_then(|p| p.content_type.as_ref())
                    .map(|s| s.to_string());
                if actual.as_deref() != Some(expected.as_str()) {
                    return false;
                }
            }

            // Check custom application properties.
            if !properties.is_empty() {
                let app_props = message.application_properties.as_ref();
                for (key, expected_val) in properties {
                    let actual = app_props
                        .and_then(|ap| ap.0.get(key))
                        .and_then(simple_value_to_string);
                    if actual.as_deref() != Some(expected_val.as_str()) {
                        return false;
                    }
                }
            }

            true
        }
        SubscriptionFilter::Sql { expression } => {
            warn!(
                expression = expression.as_str(),
                "SQL filters are not implemented; accepting all messages"
            );
            true
        }
    }
}

impl Router {
    /// Creates a new router from the topology configuration.
    ///
    /// - Each queue gets its own `MessageStore` and `DlqStore`.
    /// - Each topic subscription gets its own `MessageStore` and `DlqStore`.
    /// - Topics are recorded for fan-out but have no store of their own.
    pub fn from_topology(topology: &Topology) -> Self {
        let mut stores = HashMap::new();
        let mut dlq_stores = HashMap::new();
        let mut topic_subscriptions = HashMap::new();
        let mut subscription_index = HashMap::new();
        let mut topics = std::collections::HashSet::new();

        for queue in &topology.queues {
            let dlq = Arc::new(DlqStore::new());
            let store = Arc::new(MessageStore::new(queue_entity_config(queue), dlq.clone()));
            stores.insert(queue.name.clone(), store);
            dlq_stores.insert(queue.name.clone(), dlq);
        }

        for topic in &topology.topics {
            topics.insert(topic.name.clone());
            let mut sub_keys = Vec::new();

            for sub in &topic.subscriptions {
                let key = format!("{}/subscriptions/{}", topic.name, sub.name()).to_lowercase();
                let dlq = Arc::new(DlqStore::new());
                let store = Arc::new(MessageStore::new(sub_entity_config(sub), dlq.clone()));
                stores.insert(key.clone(), store);
                dlq_stores.insert(key.clone(), dlq);
                subscription_index.insert(key.clone(), key.clone());
                sub_keys.push(SubscriptionInfo {
                    key,
                    filter: sub.filter().cloned(),
                });
            }

            topic_subscriptions.insert(topic.name.clone(), sub_keys);
        }

        Self {
            stores,
            dlq_stores,
            topic_subscriptions,
            subscription_index,
            topics,
        }
    }

    /// Resolves an address to the store key it maps to.
    ///
    /// - Queue names map directly to their store key.
    /// - Topic names resolve to the topic name (for publishing — fan-out
    ///   is handled by `publish()`).
    /// - Subscription paths like `events-topic/Subscriptions/sub-1` are
    ///   resolved case-insensitively to the subscription's store key.
    /// - Returns `None` if the address is unknown.
    pub fn resolve_address<'a>(&'a self, address: &'a str) -> Option<&'a str> {
        // Direct queue/subscription store match
        if self.stores.contains_key(address) {
            return Some(address);
        }

        // Topic match (topics have no store but are valid addresses for publishing)
        if self.topics.contains(address) {
            return Some(address);
        }

        // Case-insensitive subscription path lookup
        let lower = address.to_lowercase();
        self.subscription_index.get(&lower).map(|s| s.as_str())
    }

    /// Returns true if the given address can be resolved.
    pub fn has_address(&self, address: &str) -> bool {
        self.resolve_address(address).is_some()
    }

    /// Returns a list of all direct store addresses (queues and subscription paths)
    /// plus topic names.
    pub fn addresses(&self) -> Vec<&str> {
        let mut addrs: Vec<&str> = self.stores.keys().map(|s| s.as_str()).collect();
        for t in &self.topics {
            if !self.stores.contains_key(t.as_str()) {
                addrs.push(t.as_str());
            }
        }
        addrs
    }

    /// Returns a list of all registered subscription paths (lowercased).
    pub fn subscription_paths(&self) -> Vec<&str> {
        self.subscription_index.keys().map(|s| s.as_str()).collect()
    }

    /// Publishes a message to the given address.
    ///
    /// - For queues: enqueues directly into the queue's store.
    /// - For topics: fans out to every subscription's store.
    /// - For subscription paths: enqueues directly into that subscription's store.
    ///
    /// Returns `Accepted(count)` on success, `Full` if any target store is at
    /// capacity, or `UnknownAddress` if the address doesn't match any entity.
    pub async fn publish(&self, address: &str, message: RouterMessage) -> PublishResult {
        // Direct queue/subscription store?
        if let Some(store) = self.stores.get(address) {
            if store.enqueue(message).await {
                return PublishResult::Accepted(1);
            } else {
                return PublishResult::Full;
            }
        }

        // Topic fan-out?
        if let Some(subs) = self.topic_subscriptions.get(address) {
            let mut count = 0;
            for sub_info in subs {
                // Check filter — None means accept all (TrueFilter)
                if !matches_filter(&message, &sub_info.filter) {
                    debug!(
                        topic = address,
                        subscription = sub_info.key,
                        "message filtered out by subscription filter"
                    );
                    continue;
                }
                if let Some(store) = self.stores.get(&sub_info.key) {
                    if !store.enqueue(message.clone()).await {
                        return PublishResult::Full;
                    }
                    count += 1;
                }
            }
            return PublishResult::Accepted(count);
        }

        // Try case-insensitive subscription path
        let lower = address.to_lowercase();
        if let Some(key) = self.subscription_index.get(&lower) {
            if let Some(store) = self.stores.get(key) {
                if store.enqueue(message).await {
                    return PublishResult::Accepted(1);
                } else {
                    return PublishResult::Full;
                }
            }
        }

        PublishResult::UnknownAddress
    }

    /// Returns the message store for the given address (competing-consumer).
    ///
    /// Multiple calls to `get_store()` on the same address return clones of
    /// the same `Arc<MessageStore>`. Each message will be delivered to
    /// exactly one consumer that calls `receive_and_delete()` or
    /// `receive_and_lock()`.
    ///
    /// Subscription paths are resolved case-insensitively.
    /// Returns `None` if the address is unknown.
    pub fn get_store(&self, address: &str) -> Option<Arc<MessageStore>> {
        // Direct queue/subscription store?
        if let Some(store) = self.stores.get(address) {
            return Some(store.clone());
        }

        // Case-insensitive subscription path lookup
        let lower = address.to_lowercase();
        if let Some(key) = self.subscription_index.get(&lower) {
            return self.stores.get(key).cloned();
        }

        None
    }

    /// Returns the DLQ store for the given address.
    ///
    /// The DLQ address convention is `<entity>/$deadletterqueue`.
    /// This method accepts both the base entity address and the full DLQ path.
    pub fn get_dlq_store(&self, address: &str) -> Option<Arc<DlqStore>> {
        // Strip /$deadletterqueue suffix if present
        let base = address
            .strip_suffix("/$deadletterqueue")
            .or_else(|| address.strip_suffix("/$DeadLetterQueue"))
            .unwrap_or(address);

        if let Some(dlq) = self.dlq_stores.get(base) {
            return Some(dlq.clone());
        }

        // Case-insensitive subscription path lookup
        let lower = base.to_lowercase();
        if let Some(key) = self.subscription_index.get(&lower) {
            return self.dlq_stores.get(key).cloned();
        }

        None
    }

    /// Returns true if this address is a DLQ address.
    pub fn is_dlq_address(&self, address: &str) -> bool {
        let lower = address.to_lowercase();
        lower.ends_with("/$deadletterqueue")
    }
}

/// Result of publishing a message to the router.
#[derive(Debug, PartialEq, Eq)]
pub enum PublishResult {
    /// Message accepted and enqueued to `count` stores.
    Accepted(usize),
    /// At least one target store is full (backpressure).
    Full,
    /// The address does not match any known queue, topic, or subscription.
    UnknownAddress,
}

/// Shared router type used across connections.
pub type SharedRouter = Arc<Router>;

/// Normalizes an AMQP address by stripping the leading `/`.
///
/// The Azure SDK sends addresses like `/input-queue` but the topology
/// configuration uses `input-queue`.
pub fn normalize_address(addr: &str) -> &str {
    addr.trim_start_matches('/')
}

/// Handles an incoming sender link (client sending messages to a queue/topic).
///
/// Receives messages from the AMQP link and publishes them to the
/// corresponding store. Rejects deliveries if the target address is unknown
/// or the store is at capacity.
pub async fn handle_incoming_messages(
    mut receiver: Receiver,
    address: String,
    router: &Router,
) -> Result<()> {
    if !router.has_address(&address) {
        // Unknown address — reject every delivery with amqp:not-found.
        warn!(address = %address, "Address not found in router, rejecting messages");
        while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
            let _ = receiver
                .reject(
                    &delivery,
                    AmqpErrorDef::new(
                        AmqpError::NotFound,
                        Some(format!("Address '{}' does not exist", address)),
                        None,
                    ),
                )
                .await;
        }
        return Ok(());
    }

    while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
        debug!(address = %address, "Message received");
        let message = delivery.message().clone();
        match router.publish(&address, message).await {
            PublishResult::Accepted(_) => {
                receiver.accept(&delivery).await?;
            }
            PublishResult::Full => {
                warn!(address = %address, "Store at capacity, rejecting message");
                let _ = receiver
                    .reject(
                        &delivery,
                        AmqpErrorDef::new(
                            AmqpError::ResourceLimitExceeded,
                            Some(format!("Queue/subscription '{}' is full", address)),
                            None,
                        ),
                    )
                    .await;
            }
            PublishResult::UnknownAddress => {
                // Shouldn't happen since we checked has_address above, but handle it.
                let _ = receiver
                    .reject(
                        &delivery,
                        AmqpErrorDef::new(
                            AmqpError::NotFound,
                            Some(format!("Address '{}' does not exist", address)),
                            None,
                        ),
                    )
                    .await;
            }
        }
    }

    Ok(())
}

/// Stamps broker-assigned properties onto an outgoing message.
///
/// Sets the following on the message before it is sent to the consumer:
/// - `message_annotations`:
///   - `x-opt-sequence-number` — broker-assigned monotonic sequence number (Long)
///   - `x-opt-enqueued-time` — time the message was enqueued (Timestamp, ms since epoch)
///   - `x-opt-lock-token` — lock token UUID (only for PeekLock mode)
/// - `header.delivery_count` — number of prior delivery attempts (for PeekLock)
///
/// Existing message annotations from the publisher are preserved; broker
/// annotations are merged in (overwriting if keys collide).
fn stamp_broker_properties(
    message: &mut RouterMessage,
    sequence_number: u64,
    enqueued_time_utc: u64,
    delivery_count: u32,
    lock_token: Option<LockToken>,
) {
    // --- Message annotations ---
    let mut builder = MessageAnnotations::builder()
        .insert(
            "x-opt-sequence-number",
            Value::Long(sequence_number as i64),
        )
        .insert(
            "x-opt-enqueued-time",
            Value::Timestamp(Timestamp::from_milliseconds(enqueued_time_utc as i64)),
        );

    if let Some(token) = lock_token {
        // Convert uuid::Uuid → AMQP Uuid via bytes
        let amqp_uuid = fe2o3_amqp_types::primitives::Uuid::from(*token.as_bytes());
        builder = builder.insert("x-opt-lock-token", Value::Uuid(amqp_uuid));
    }

    let broker_annotations = builder.build();

    match &mut message.message_annotations {
        Some(existing) => {
            // Merge broker annotations into existing (broker wins on conflict)
            for (k, v) in broker_annotations.0.into_iter() {
                existing.0.insert(k, v);
            }
        }
        None => {
            message.message_annotations = Some(broker_annotations);
        }
    }

    // --- Header: delivery_count ---
    // AMQP 1.0 header.delivery_count counts *prior* delivery attempts (0-based).
    // Our store tracks total delivery attempts (1-based: first delivery = 1).
    // The Azure SDK adds 1 to the AMQP header value to produce its DeliveryCount
    // property, so we subtract 1 here to keep things consistent:
    //   store=1 → header=0 → SDK=1 (first delivery)
    //   store=2 → header=1 → SDK=2 (first redelivery)
    let amqp_delivery_count = delivery_count.saturating_sub(1);
    match &mut message.header {
        Some(header) => {
            header.delivery_count = amqp_delivery_count;
        }
        None => {
            let mut header = Header::default();
            header.delivery_count = amqp_delivery_count;
            message.header = Some(header);
        }
    }
}

/// Handles an outgoing sender link (server sending messages to a client).
///
/// Receives messages from the message store and forwards them to the
/// client over the AMQP link. Uses ReceiveAndDelete mode.
pub async fn handle_outgoing_messages(
    mut sender: Sender,
    address: String,
    store: Arc<MessageStore>,
) -> Result<()> {
    loop {
        tokio::select! {
            envelope = store.receive_and_delete() => {
                let mut message = envelope.message;
                stamp_broker_properties(
                    &mut message,
                    envelope.sequence_number,
                    envelope.enqueued_time_utc,
                    envelope.delivery_count,
                    None, // No lock token in ReceiveAndDelete mode
                );
                debug!(address = %address, seq = envelope.sequence_number, "Forwarding message to client");
                match sender.send(message).await {
                    Ok(_) => {
                        // Delivery confirmed — decrement logical queue depth
                        store.confirm_removal();
                    }
                    Err(e) => {
                        debug!(address = %address, error = ?e, "Client disconnected");
                        // Message was already removed from the store by receive_and_delete,
                        // but we still need to decrement the logical count since the
                        // message is gone (lost on disconnect).
                        store.confirm_removal();
                        break;
                    }
                }
            }
            detach_err = sender.on_detach() => {
                debug!(address = %address, error = ?detach_err, "Link detached by peer");
                break;
            }
        }
    }

    Ok(())
}

/// Handles an outgoing sender link in PeekLock mode.
///
/// Messages are locked in the store before delivery. After sending, the
/// handler waits for the client's disposition (Accepted/Released/Modified/Rejected)
/// and settles the message accordingly:
/// - **Accepted** → Complete (remove from store)
/// - **Released** → Abandon (unlock, make available again)
/// - **Modified** → Abandon (unlock, make available again)
/// - **Rejected** → Dead-letter (move to DLQ)
pub async fn handle_outgoing_messages_peek_lock(
    mut sender: Sender,
    address: String,
    store: Arc<MessageStore>,
) -> Result<()> {
    loop {
        tokio::select! {
            envelope = store.receive_and_lock() => {
                // Extract lock token from envelope state
                let lock_token = match &envelope.state {
                    MessageState::Locked { lock_token, .. } => *lock_token,
                    _ => {
                        warn!(address = %address, "receive_and_lock returned non-locked envelope");
                        continue;
                    }
                };

                let mut message = envelope.message;
                stamp_broker_properties(
                    &mut message,
                    envelope.sequence_number,
                    envelope.enqueued_time_utc,
                    envelope.delivery_count,
                    Some(lock_token),
                );

                debug!(
                    address = %address,
                    seq = envelope.sequence_number,
                    lock_token = %lock_token,
                    delivery_count = envelope.delivery_count,
                    "Sending message (PeekLock)"
                );

                // send() blocks until the client settles the delivery
                // Set the delivery tag to the lock token UUID bytes so the
                // Azure .NET SDK can extract the lock token from the AMQP
                // delivery tag (it does NOT read x-opt-lock-token).
                let sendable = fe2o3_amqp::link::delivery::Sendable::builder()
                    .message(message)
                    .delivery_tag(lock_token.as_bytes().to_vec())
                    .build();
                match sender.send(sendable).await {
                    Ok(outcome) => {
                        use fe2o3_amqp_types::messaging::Outcome;
                        match outcome {
                            Outcome::Accepted(_) => {
                                debug!(address = %address, seq = envelope.sequence_number, "Complete (Accepted)");
                                store.complete(lock_token).await;
                            }
                            Outcome::Released(_) => {
                                debug!(address = %address, seq = envelope.sequence_number, "Abandon (Released)");
                                store.abandon(lock_token).await;
                            }
                            Outcome::Modified(_) => {
                                debug!(address = %address, seq = envelope.sequence_number, "Abandon (Modified)");
                                store.abandon(lock_token).await;
                            }
                            Outcome::Rejected(rejected) => {
                                debug!(
                                    address = %address,
                                    seq = envelope.sequence_number,
                                    error = ?rejected.error,
                                    "Dead-letter (Rejected)"
                                );
                                store.dead_letter(lock_token).await;
                            }
                            #[allow(unreachable_patterns)]
                            _ => {
                                debug!(address = %address, seq = envelope.sequence_number, "Unknown outcome, abandoning");
                                store.abandon(lock_token).await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(address = %address, error = ?e, "Client disconnected (PeekLock), abandoning message");
                        // Client disconnected — abandon the locked message so
                        // it becomes available for other consumers.
                        store.abandon(lock_token).await;
                        break;
                    }
                }
            }
            detach_err = sender.on_detach() => {
                debug!(address = %address, error = ?detach_err, "Link detached by peer (PeekLock)");
                break;
            }
        }
    }

    Ok(())
}

/// Handles an outgoing sender link for a DLQ store (ReceiveAndDelete mode).
///
/// Similar to `handle_outgoing_messages` but reads from a `DlqStore` instead
/// of a `MessageStore`.
pub async fn handle_outgoing_dlq_messages(
    mut sender: Sender,
    address: String,
    store: Arc<DlqStore>,
) -> Result<()> {
    loop {
        tokio::select! {
            envelope = store.receive() => {
                let mut message = envelope.message;
                stamp_broker_properties(
                    &mut message,
                    envelope.sequence_number,
                    envelope.enqueued_time_utc,
                    envelope.delivery_count,
                    None,
                );
                debug!(address = %address, seq = envelope.sequence_number, "Forwarding DLQ message to client");
                if let Err(e) = sender.send(message).await {
                    debug!(address = %address, error = ?e, "Client disconnected (DLQ)");
                    break;
                }
            }
            detach_err = sender.on_detach() => {
                debug!(address = %address, error = ?detach_err, "Link detached by peer (DLQ)");
                break;
            }
        }
    }

    Ok(())
}

/// Handles an outgoing sender link for a DLQ store in PeekLock mode.
///
/// Similar to `handle_outgoing_messages_peek_lock` but reads from a `DlqStore`.
/// DLQ messages can be completed (removed) or abandoned (unlocked for redelivery)
/// but cannot be dead-lettered again (no nested DLQ).
pub async fn handle_outgoing_dlq_messages_peek_lock(
    mut sender: Sender,
    address: String,
    store: Arc<DlqStore>,
) -> Result<()> {
    loop {
        tokio::select! {
            envelope = store.receive_and_lock() => {
                let lock_token = match &envelope.state {
                    MessageState::Locked { lock_token, .. } => *lock_token,
                    _ => {
                        warn!(address = %address, "DLQ receive_and_lock returned non-locked envelope");
                        continue;
                    }
                };

                let mut message = envelope.message;
                stamp_broker_properties(
                    &mut message,
                    envelope.sequence_number,
                    envelope.enqueued_time_utc,
                    envelope.delivery_count,
                    Some(lock_token),
                );

                debug!(
                    address = %address,
                    seq = envelope.sequence_number,
                    lock_token = %lock_token,
                    "Sending DLQ message (PeekLock)"
                );

                match {
                    let sendable = fe2o3_amqp::link::delivery::Sendable::builder()
                        .message(message)
                        .delivery_tag(lock_token.as_bytes().to_vec())
                        .build();
                    sender.send(sendable).await
                } {
                    Ok(outcome) => {
                        use fe2o3_amqp_types::messaging::Outcome;
                        match outcome {
                            Outcome::Accepted(_) => {
                                debug!(address = %address, seq = envelope.sequence_number, "DLQ Complete (Accepted)");
                                store.complete(lock_token).await;
                            }
                            Outcome::Released(_) | Outcome::Modified(_) => {
                                debug!(address = %address, seq = envelope.sequence_number, "DLQ Abandon (Released/Modified)");
                                store.abandon(lock_token).await;
                            }
                            Outcome::Rejected(_) => {
                                // No nested DLQ — just abandon
                                debug!(address = %address, seq = envelope.sequence_number, "DLQ Rejected — abandoning (no nested DLQ)");
                                store.abandon(lock_token).await;
                            }
                            #[allow(unreachable_patterns)]
                            _ => {
                                debug!(address = %address, seq = envelope.sequence_number, "DLQ Unknown outcome, abandoning");
                                store.abandon(lock_token).await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(address = %address, error = ?e, "Client disconnected (DLQ PeekLock), abandoning");
                        store.abandon(lock_token).await;
                        break;
                    }
                }
            }
            detach_err = sender.on_detach() => {
                debug!(address = %address, error = ?detach_err, "Link detached by peer (DLQ PeekLock)");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{QueueConfig, SubscriptionEntry, SubscriptionFilter, TopicConfig};
    use fe2o3_amqp::types::messaging::AmqpValue;
    use fe2o3_amqp_types::messaging::annotations::OwnedKey;
    use fe2o3_amqp_types::primitives::{OrderedMap, SimpleValue, Symbol};
    use uuid::Uuid;

    fn test_topology() -> Topology {
        Topology {
            queues: vec![
                QueueConfig::new("queue-a"),
                QueueConfig::new("queue-b"),
            ],
            topics: vec![TopicConfig {
                name: "topic-x".to_string(),
                subscriptions: vec![
                    SubscriptionEntry::Name("sub-1".to_string()),
                    SubscriptionEntry::Name("sub-2".to_string()),
                ],
            }],
        }
    }

    fn test_message(body: &str) -> RouterMessage {
        Message::builder()
            .body(Body::Value(AmqpValue(Value::String(body.to_string()))))
            .build()
    }

    #[test]
    fn test_router_from_topology() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert!(router.has_address("queue-a"));
        assert!(router.has_address("queue-b"));
        assert!(router.has_address("topic-x"));
        assert!(!router.has_address("nonexistent"));
    }

    #[test]
    fn test_router_addresses() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let mut addrs = router.addresses();
        addrs.sort();
        assert!(addrs.contains(&"queue-a"));
        assert!(addrs.contains(&"queue-b"));
        assert!(addrs.contains(&"topic-x"));
    }

    #[tokio::test]
    async fn test_router_publish_no_subscribers() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // Publishing to a queue with no active receivers should succeed
        // (message is buffered in the store)
        let count = router.publish("queue-a", test_message("test")).await;
        assert_eq!(count, PublishResult::Accepted(1));
    }

    #[tokio::test]
    async fn test_router_publish_nonexistent_address() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let count = router.publish("nonexistent", test_message("test")).await;
        assert_eq!(count, PublishResult::UnknownAddress);
    }

    #[tokio::test]
    async fn test_router_store_and_receive() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let store = router.get_store("queue-a").unwrap();

        let count = router.publish("queue-a", test_message("hello")).await;
        assert_eq!(count, PublishResult::Accepted(1));

        let envelope = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store.receive_and_delete(),
        )
        .await
        .expect("Should receive message");

        if let Body::Value(AmqpValue(Value::String(s))) = &envelope.message.body {
            assert_eq!(s, "hello");
        } else {
            panic!("Unexpected message body");
        }
    }

    #[test]
    fn test_router_get_store_nonexistent() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert!(router.get_store("nonexistent").is_none());
    }

    #[test]
    fn test_normalize_address() {
        assert_eq!(normalize_address("/input-queue"), "input-queue");
        assert_eq!(normalize_address("input-queue"), "input-queue");
        assert_eq!(normalize_address("///triple"), "triple");
        assert_eq!(normalize_address("$cbs"), "$cbs");
    }

    #[test]
    fn test_router_empty_topology() {
        let topology = Topology {
            queues: vec![],
            topics: vec![],
        };
        let router = Router::from_topology(&topology);

        assert!(!router.has_address("anything"));
    }

    #[tokio::test]
    async fn test_router_competing_consumers_queue() {
        // Two receivers on the same queue — only one should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let store1 = router.get_store("queue-a").unwrap();
        let store2 = router.get_store("queue-a").unwrap();

        router.publish("queue-a", test_message("competing")).await;

        let r1 = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            store1.receive_and_delete(),
        );
        let r2 = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            store2.receive_and_delete(),
        );

        let (r1, r2) = tokio::join!(r1, r2);

        // Exactly one should succeed
        assert!(
            (r1.is_ok() && r2.is_err()) || (r1.is_err() && r2.is_ok()),
            "Expected exactly one receiver to get the message"
        );
    }

    #[tokio::test]
    async fn test_router_topic_fanout_different_subscriptions() {
        // Two receivers on different subscriptions — both should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let store1 = router
            .get_store("topic-x/Subscriptions/sub-1")
            .expect("sub-1 should resolve");
        let store2 = router
            .get_store("topic-x/Subscriptions/sub-2")
            .expect("sub-2 should resolve");

        let count = router.publish("topic-x", test_message("fanout")).await;
        assert_eq!(count, PublishResult::Accepted(2));

        let e1 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store1.receive_and_delete(),
        )
        .await
        .unwrap();
        let e2 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store2.receive_and_delete(),
        )
        .await
        .unwrap();

        if let Body::Value(AmqpValue(Value::String(s))) = &e1.message.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-1");
        }

        if let Body::Value(AmqpValue(Value::String(s))) = &e2.message.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-2");
        }
    }

    #[tokio::test]
    async fn test_router_competing_consumers_same_subscription() {
        // Two receivers on the SAME subscription — only one should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let store1 = router.get_store("topic-x/subscriptions/sub-1").unwrap();
        let store2 = router.get_store("topic-x/subscriptions/sub-1").unwrap();

        router.publish("topic-x", test_message("compete")).await;

        let r1 = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            store1.receive_and_delete(),
        );
        let r2 = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            store2.receive_and_delete(),
        );

        let (r1, r2) = tokio::join!(r1, r2);

        assert!(
            (r1.is_ok() && r2.is_err()) || (r1.is_err() && r2.is_ok()),
            "Expected exactly one receiver to get the message on same subscription"
        );
    }

    // --- Subscription resolution tests ---

    #[test]
    fn test_router_subscription_paths() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let mut paths = router.subscription_paths();
        paths.sort();
        assert_eq!(
            paths,
            vec!["topic-x/subscriptions/sub-1", "topic-x/subscriptions/sub-2"]
        );
    }

    #[test]
    fn test_router_resolve_queue() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(router.resolve_address("queue-a"), Some("queue-a"));
        assert_eq!(router.resolve_address("queue-b"), Some("queue-b"));
    }

    #[test]
    fn test_router_resolve_topic() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(router.resolve_address("topic-x"), Some("topic-x"));
    }

    #[test]
    fn test_router_resolve_subscription_lowercase() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(
            router.resolve_address("topic-x/subscriptions/sub-1"),
            Some("topic-x/subscriptions/sub-1")
        );
    }

    #[test]
    fn test_router_resolve_subscription_capitalized() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // Azure SDK sends "Subscriptions" with capital S
        assert_eq!(
            router.resolve_address("topic-x/Subscriptions/sub-1"),
            Some("topic-x/subscriptions/sub-1")
        );
    }

    #[test]
    fn test_router_resolve_subscription_mixed_case() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(
            router.resolve_address("topic-x/SUBSCRIPTIONS/SUB-1"),
            Some("topic-x/subscriptions/sub-1")
        );
        assert_eq!(
            router.resolve_address("Topic-X/Subscriptions/Sub-1"),
            Some("topic-x/subscriptions/sub-1")
        );
    }

    #[test]
    fn test_router_resolve_nonexistent() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(router.resolve_address("nonexistent"), None);
        assert_eq!(
            router.resolve_address("topic-x/subscriptions/no-such-sub"),
            None
        );
    }

    #[tokio::test]
    async fn test_router_get_store_via_subscription_path() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // Get stores via subscription paths — both resolve to different stores
        let store1 = router
            .get_store("topic-x/Subscriptions/sub-1")
            .expect("sub-1 should resolve");
        let store2 = router
            .get_store("topic-x/Subscriptions/sub-2")
            .expect("sub-2 should resolve");

        // Publish to the topic directly
        let count = router.publish("topic-x", test_message("fanout")).await;
        assert_eq!(count, PublishResult::Accepted(2));

        // Both stores should have the message
        let e1 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store1.receive_and_delete(),
        )
        .await
        .unwrap();
        let e2 = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store2.receive_and_delete(),
        )
        .await
        .unwrap();

        if let Body::Value(AmqpValue(Value::String(s))) = &e1.message.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-1");
        }

        if let Body::Value(AmqpValue(Value::String(s))) = &e2.message.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-2");
        }
    }

    #[test]
    fn test_router_dlq_store() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // DLQ store for a queue
        assert!(router.get_dlq_store("queue-a").is_some());
        assert!(router.get_dlq_store("queue-a/$deadletterqueue").is_some());
        assert!(router.get_dlq_store("queue-a/$DeadLetterQueue").is_some());

        // DLQ store for a subscription
        assert!(router.get_dlq_store("topic-x/subscriptions/sub-1").is_some());
        assert!(router
            .get_dlq_store("topic-x/subscriptions/sub-1/$deadletterqueue")
            .is_some());

        // Nonexistent
        assert!(router.get_dlq_store("nonexistent").is_none());
    }

    #[test]
    fn test_router_is_dlq_address() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert!(router.is_dlq_address("queue-a/$deadletterqueue"));
        assert!(router.is_dlq_address("queue-a/$DeadLetterQueue"));
        assert!(!router.is_dlq_address("queue-a"));
    }

    // --- Broker property stamping tests ---

    #[test]
    fn test_stamp_broker_properties_on_empty_message() {
        let mut msg = test_message("hello");
        assert!(msg.message_annotations.is_none());
        assert!(msg.header.is_none());

        stamp_broker_properties(&mut msg, 42, 1_700_000_000_000, 0, None);

        // Check message annotations
        let annotations = msg.message_annotations.as_ref().expect("annotations should be set");
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        let enq_key = OwnedKey::Symbol(Symbol::from("x-opt-enqueued-time"));

        let seq = annotations.0.get(&seq_key).expect("seq should exist");
        assert_eq!(*seq, Value::Long(42));

        let enq = annotations.0.get(&enq_key).expect("enqueued time should exist");
        assert_eq!(*enq, Value::Timestamp(Timestamp::from_milliseconds(1_700_000_000_000)));

        // No lock token annotation in ReceiveAndDelete mode
        let lock_key = OwnedKey::Symbol(Symbol::from("x-opt-lock-token"));
        assert!(annotations.0.get(&lock_key).is_none());

        // Check header
        let header = msg.header.as_ref().expect("header should be set");
        assert_eq!(header.delivery_count, 0);
    }

    #[test]
    fn test_stamp_broker_properties_preserves_existing_annotations() {
        let mut msg = test_message("hello");

        // Set existing message annotations from the publisher
        let existing = MessageAnnotations::builder()
            .insert("x-custom-key", Value::String("custom-value".to_string()))
            .build();
        msg.message_annotations = Some(existing);

        stamp_broker_properties(&mut msg, 7, 1_600_000_000_000, 3, None);

        let annotations = msg.message_annotations.as_ref().unwrap();

        // Custom annotation preserved
        let custom_key = OwnedKey::Symbol(Symbol::from("x-custom-key"));
        let custom = annotations.0.get(&custom_key).expect("custom key should be preserved");
        assert_eq!(*custom, Value::String("custom-value".to_string()));

        // Broker annotations added
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        let enq_key = OwnedKey::Symbol(Symbol::from("x-opt-enqueued-time"));

        let seq = annotations.0.get(&seq_key).unwrap();
        assert_eq!(*seq, Value::Long(7));

        let enq = annotations.0.get(&enq_key).unwrap();
        assert_eq!(*enq, Value::Timestamp(Timestamp::from_milliseconds(1_600_000_000_000)));
    }

    #[test]
    fn test_stamp_broker_properties_sets_delivery_count() {
        let mut msg = test_message("hello");
        // Store delivery_count=5 → AMQP header=4 (0-based prior attempts)
        stamp_broker_properties(&mut msg, 1, 1_000, 5, None);

        let header = msg.header.as_ref().unwrap();
        assert_eq!(header.delivery_count, 4);
    }

    #[test]
    fn test_stamp_broker_properties_preserves_existing_header() {
        let mut msg = test_message("hello");

        // Set existing header with durable=true and priority
        let mut header = Header::default();
        header.durable = true;
        header.priority = 7.into();
        msg.header = Some(header);

        stamp_broker_properties(&mut msg, 1, 1_000, 2, None);

        let header = msg.header.as_ref().unwrap();
        assert!(header.durable);
        // Store delivery_count=2 → AMQP header=1
        assert_eq!(header.delivery_count, 1);
    }

    #[test]
    fn test_stamp_broker_properties_with_lock_token() {
        let mut msg = test_message("hello");
        let token = Uuid::new_v4();
        stamp_broker_properties(&mut msg, 10, 2_000, 1, Some(token));

        let annotations = msg.message_annotations.as_ref().unwrap();
        let lock_key = OwnedKey::Symbol(Symbol::from("x-opt-lock-token"));
        let lock_val = annotations.0.get(&lock_key).expect("lock token should exist");
        let expected_amqp_uuid = fe2o3_amqp_types::primitives::Uuid::from(*token.as_bytes());
        assert_eq!(*lock_val, Value::Uuid(expected_amqp_uuid));

        // Other annotations should still be present
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        assert!(annotations.0.get(&seq_key).is_some());
    }

    // ── Correlation filter tests ──────────────────────────────────────

    #[test]
    fn test_matches_filter_none_accepts_all() {
        let msg = test_message("anything");
        assert!(matches_filter(&msg, &None));
    }

    #[test]
    fn test_matches_filter_correlation_subject_match() {
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);

        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: HashMap::new(),
        };
        assert!(matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_subject_mismatch() {
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-shipped".to_string());
        msg.properties = Some(props);

        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: HashMap::new(),
        };
        assert!(!matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_subject_missing_in_message() {
        let msg = test_message("hello"); // no properties set
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: HashMap::new(),
        };
        assert!(!matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_id_match() {
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.correlation_id = Some(
            fe2o3_amqp_types::messaging::MessageId::String("session-42".to_string()),
        );
        msg.properties = Some(props);

        let filter = SubscriptionFilter::Correlation {
            correlation_id: Some("session-42".to_string()),
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: HashMap::new(),
        };
        assert!(matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_app_properties_match() {
        let mut msg = test_message("hello");
        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(
            OrderedMap::from_iter(vec![
                (
                    "region".to_string(),
                    fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
                ),
                (
                    "priority".to_string(),
                    fe2o3_amqp_types::primitives::SimpleValue::String("high".to_string()),
                ),
            ]),
        );
        msg.application_properties = Some(app_props);

        let mut filter_props = HashMap::new();
        filter_props.insert("region".to_string(), "us-east".to_string());
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: filter_props,
        };
        assert!(matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_app_properties_mismatch() {
        let mut msg = test_message("hello");
        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(
            OrderedMap::from_iter(vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("eu-west".to_string()),
            )]),
        );
        msg.application_properties = Some(app_props);

        let mut filter_props = HashMap::new();
        filter_props.insert("region".to_string(), "us-east".to_string());
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: filter_props,
        };
        assert!(!matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_app_properties_missing_key() {
        let msg = test_message("hello"); // no application_properties
        let mut filter_props = HashMap::new();
        filter_props.insert("region".to_string(), "us-east".to_string());
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: filter_props,
        };
        assert!(!matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_multi_field() {
        // Both subject AND application property must match
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);

        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(
            OrderedMap::from_iter(vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
            )]),
        );
        msg.application_properties = Some(app_props);

        let mut filter_props = HashMap::new();
        filter_props.insert("region".to_string(), "us-east".to_string());
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: filter_props,
        };
        assert!(matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_correlation_multi_field_partial_mismatch() {
        // Subject matches but app property doesn't
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);

        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(
            OrderedMap::from_iter(vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("eu-west".to_string()),
            )]),
        );
        msg.application_properties = Some(app_props);

        let mut filter_props = HashMap::new();
        filter_props.insert("region".to_string(), "us-east".to_string());
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: filter_props,
        };
        assert!(!matches_filter(&msg, &Some(filter)));
    }

    #[test]
    fn test_matches_filter_empty_correlation_accepts_all() {
        // Correlation filter with all fields None and empty properties = TrueFilter
        let msg = test_message("anything");
        let filter = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: HashMap::new(),
        };
        assert!(matches_filter(&msg, &Some(filter)));
    }

    #[tokio::test]
    async fn test_router_topic_fanout_with_correlation_filter() {
        let yaml = r#"
topology:
    queues: []
    topics:
      - name: "orders"
        subscriptions:
          - name: "high-priority"
            filter:
              type: correlation
              subject: "high"
          - name: "low-priority"
            filter:
              type: correlation
              subject: "low"
          - name: "all-orders"
"#;
        let config = crate::config::Config::from_yaml(yaml).unwrap();
        let router = Router::from_topology(&config.topology);

        // Send a "high" subject message
        let mut msg = test_message("order-1");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("high".to_string());
        msg.properties = Some(props);

        let count = router.publish("orders", msg).await;
        // Should go to "high-priority" and "all-orders" (no filter = accept all)
        assert_eq!(count, PublishResult::Accepted(2));

        let high_store = router
            .get_store("orders/subscriptions/high-priority")
            .unwrap();
        let low_store = router
            .get_store("orders/subscriptions/low-priority")
            .unwrap();
        let all_store = router
            .get_store("orders/subscriptions/all-orders")
            .unwrap();

        // high-priority and all-orders should have messages
        let timeout = std::time::Duration::from_millis(100);
        assert!(
            tokio::time::timeout(timeout, high_store.receive_and_delete())
                .await
                .is_ok(),
            "high-priority should receive the message"
        );
        assert!(
            tokio::time::timeout(timeout, low_store.receive_and_delete())
                .await
                .is_err(),
            "low-priority should NOT receive the message"
        );
        assert!(
            tokio::time::timeout(timeout, all_store.receive_and_delete())
                .await
                .is_ok(),
            "all-orders should receive the message"
        );
    }

    #[tokio::test]
    async fn test_router_topic_fanout_with_app_property_filter() {
        let yaml = r#"
topology:
    queues: []
    topics:
      - name: "events"
        subscriptions:
          - name: "us-only"
            filter:
              type: correlation
              properties:
                region: "us-east"
          - name: "eu-only"
            filter:
              type: correlation
              properties:
                region: "eu-west"
"#;
        let config = crate::config::Config::from_yaml(yaml).unwrap();
        let router = Router::from_topology(&config.topology);

        // Send US message
        let mut msg = test_message("event-1");
        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(
            OrderedMap::from_iter(vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
            )]),
        );
        msg.application_properties = Some(app_props);

        let count = router.publish("events", msg).await;
        assert_eq!(count, PublishResult::Accepted(1));

        let us_store = router.get_store("events/subscriptions/us-only").unwrap();
        let eu_store = router.get_store("events/subscriptions/eu-only").unwrap();

        let timeout = std::time::Duration::from_millis(100);
        assert!(
            tokio::time::timeout(timeout, us_store.receive_and_delete())
                .await
                .is_ok(),
            "us-only should receive the message"
        );
        assert!(
            tokio::time::timeout(timeout, eu_store.receive_and_delete())
                .await
                .is_err(),
            "eu-only should NOT receive the message"
        );
    }

    #[tokio::test]
    async fn test_router_topic_fanout_no_matching_filter() {
        let yaml = r#"
topology:
    queues: []
    topics:
      - name: "events"
        subscriptions:
          - name: "filtered"
            filter:
              type: correlation
              subject: "special"
"#;
        let config = crate::config::Config::from_yaml(yaml).unwrap();
        let router = Router::from_topology(&config.topology);

        // Send a message with no subject — should not match
        let msg = test_message("event-1");
        let count = router.publish("events", msg).await;
        assert_eq!(count, PublishResult::Accepted(0));

        let store = router.get_store("events/subscriptions/filtered").unwrap();
        let timeout = std::time::Duration::from_millis(100);
        assert!(
            tokio::time::timeout(timeout, store.receive_and_delete())
                .await
                .is_err(),
            "filtered should NOT receive the message"
        );
    }

    // --- Backpressure tests ---

    #[tokio::test]
    async fn test_publish_rejects_when_queue_full() {
        let yaml = r#"
topology:
    queues:
      - name: "small-queue"
    topics: []
"#;
        let config = crate::config::Config::from_yaml(yaml).unwrap();
        let router = Router::from_topology(&config.topology);

        // DEFAULT_CHANNEL_CAPACITY is 1000 — fill the queue
        for i in 0..DEFAULT_CHANNEL_CAPACITY {
            let result = router
                .publish("small-queue", test_message(&format!("msg-{}", i)))
                .await;
            assert_eq!(
                result,
                PublishResult::Accepted(1),
                "message {} should be accepted",
                i
            );
        }

        // Next message should be rejected
        let result = router
            .publish("small-queue", test_message("overflow"))
            .await;
        assert_eq!(result, PublishResult::Full);
    }

    #[tokio::test]
    async fn test_publish_unknown_address_returns_unknown() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let result = router.publish("no-such-queue", test_message("test")).await;
        assert_eq!(result, PublishResult::UnknownAddress);
    }
}
