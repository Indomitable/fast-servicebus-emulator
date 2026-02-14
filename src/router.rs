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
use fe2o3_amqp::types::messaging::{Body, Message};
use fe2o3_amqp::types::primitives::{Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use fe2o3_amqp_types::definitions::{AmqpError, Error as AmqpErrorDef};
use crate::config::{
    QueueConfig, SubscriptionEntry, SubscriptionFilter, Topology, DEFAULT_CHANNEL_CAPACITY,
};
use crate::store::{DlqStore, EntityConfig, MessageState, MessageStore};
use crate::helpers::router_message::{stamp_broker_properties, matches_filter};


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

