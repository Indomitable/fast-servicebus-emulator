//! Message router with competing-consumer queues and topic fan-out.
//!
//! Each queue and each topic subscription gets an `async_channel` (MPMC).
//! When multiple receivers subscribe to the same queue or the same
//! subscription, each message is delivered to exactly **one** receiver
//! (competing consumers).
//!
//! Topics themselves have no channel — publishing to a topic sends the
//! message to every subscription's channel independently.
//!
//! Subscription paths like `events-topic/subscriptions/sub-1` are
//! resolved case-insensitively.

use anyhow::Result;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::types::messaging::{Body, Message};
use fe2o3_amqp::types::primitives::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::config::Topology;

/// Channel capacity for each queue/subscription.
const CHANNEL_CAPACITY: usize = 1000;

/// Type alias for a message routed through channels.
pub type RouterMessage = Message<Body<Value>>;

/// Immutable message router. Created once at startup from the topology
/// configuration and shared across all connections via `Arc`.
#[derive(Clone)]
pub struct Router {
    /// Channels for queues and subscriptions. Keyed by queue name or
    /// the lowercased subscription path (e.g. `events-topic/subscriptions/sub-1`).
    channels: HashMap<String, async_channel::Sender<RouterMessage>>,

    /// Matching receivers for each channel. Cloning an `async_channel::Receiver`
    /// gives competing-consumer semantics — each message goes to one consumer.
    receivers: HashMap<String, async_channel::Receiver<RouterMessage>>,

    /// Maps topic name → list of subscription channel keys.
    /// Used by `publish()` to fan out to all subscriptions.
    topic_subscriptions: HashMap<String, Vec<String>>,

    /// Maps lowercased subscription paths to their channel key.
    /// The channel key is the same lowercased subscription path.
    subscription_index: HashMap<String, String>,

    /// Set of topic names, so we can distinguish topics from queues
    /// in `resolve_address` and `has_address`.
    topics: std::collections::HashSet<String>,
}

impl Router {
    /// Creates a new router from the topology configuration.
    ///
    /// - Each queue gets its own `async_channel`.
    /// - Each topic subscription gets its own `async_channel`.
    /// - Topics are recorded for fan-out but have no channel of their own.
    pub fn from_topology(topology: &Topology) -> Self {
        let mut channels = HashMap::new();
        let mut receivers = HashMap::new();
        let mut topic_subscriptions = HashMap::new();
        let mut subscription_index = HashMap::new();
        let mut topics = std::collections::HashSet::new();

        for queue in &topology.queues {
            let (tx, rx) = async_channel::bounded(CHANNEL_CAPACITY);
            channels.insert(queue.name.clone(), tx);
            receivers.insert(queue.name.clone(), rx);
        }

        for topic in &topology.topics {
            topics.insert(topic.name.clone());
            let mut sub_keys = Vec::new();

            for sub in &topic.subscriptions {
                let key = format!("{}/subscriptions/{}", topic.name, sub).to_lowercase();
                let (tx, rx) = async_channel::bounded(CHANNEL_CAPACITY);
                channels.insert(key.clone(), tx);
                receivers.insert(key.clone(), rx);
                subscription_index.insert(key.clone(), key.clone());
                sub_keys.push(key);
            }

            topic_subscriptions.insert(topic.name.clone(), sub_keys);
        }

        Self {
            channels,
            receivers,
            topic_subscriptions,
            subscription_index,
            topics,
        }
    }

    /// Resolves an address to the channel key it maps to.
    ///
    /// - Queue names map directly to their channel key.
    /// - Topic names resolve to the topic name (for publishing — fan-out
    ///   is handled by `publish()`).
    /// - Subscription paths like `events-topic/Subscriptions/sub-1` are
    ///   resolved case-insensitively to the subscription's channel key.
    /// - Returns `None` if the address is unknown.
    pub fn resolve_address<'a>(&'a self, address: &'a str) -> Option<&'a str> {
        // Direct queue channel match
        if self.channels.contains_key(address) {
            return Some(address);
        }

        // Topic match (topics have no channel but are valid addresses for publishing)
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

    /// Returns a list of all direct channel addresses (queues and topics).
    pub fn addresses(&self) -> Vec<&str> {
        let mut addrs: Vec<&str> = self.channels.keys().map(|s| s.as_str()).collect();
        for t in &self.topics {
            if !self.channels.contains_key(t.as_str()) {
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
    /// - For queues: sends directly to the queue's channel.
    /// - For topics: fans out to every subscription's channel.
    /// - For subscription paths: sends directly to that subscription's channel.
    ///
    /// Returns `Some(count)` with the number of subscription channels the
    /// message was sent to, or `None` if the address is unknown.
    pub fn publish(&self, address: &str, message: RouterMessage) -> Option<usize> {
        // Direct queue/subscription channel?
        if let Some(tx) = self.channels.get(address) {
            let _ = tx.try_send(message);
            return Some(1);
        }

        // Topic fan-out?
        if let Some(sub_keys) = self.topic_subscriptions.get(address) {
            let mut count = 0;
            for key in sub_keys {
                if let Some(tx) = self.channels.get(key) {
                    let _ = tx.try_send(message.clone());
                    count += 1;
                }
            }
            return Some(count);
        }

        // Try case-insensitive subscription path
        let lower = address.to_lowercase();
        if let Some(key) = self.subscription_index.get(&lower) {
            if let Some(tx) = self.channels.get(key) {
                let _ = tx.try_send(message);
                return Some(1);
            }
        }

        None
    }

    /// Returns a receiver for the given address (competing-consumer).
    ///
    /// Multiple calls to `subscribe()` on the same address return clones of
    /// the same `async_channel::Receiver`. Each message will be delivered to
    /// exactly one of the receivers.
    ///
    /// Subscription paths are resolved case-insensitively.
    /// Returns `None` if the address is unknown.
    pub fn subscribe(&self, address: &str) -> Option<async_channel::Receiver<RouterMessage>> {
        // Direct queue/subscription channel?
        if let Some(rx) = self.receivers.get(address) {
            return Some(rx.clone());
        }

        // Case-insensitive subscription path lookup
        let lower = address.to_lowercase();
        if let Some(key) = self.subscription_index.get(&lower) {
            return self.receivers.get(key).cloned();
        }

        None
    }
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
/// corresponding channel.
pub async fn handle_incoming_messages(
    mut receiver: Receiver,
    address: String,
    router: &Router,
) -> Result<()> {
    if router.has_address(&address) {
        while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
            debug!(address = %address, "Message received");
            let message = delivery.message().clone();
            router.publish(&address, message);
            receiver.accept(&delivery).await?;
        }
    } else {
        warn!(address = %address, "Address not found in router, discarding messages");
        while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
            receiver.accept(&delivery).await?;
        }
    }

    Ok(())
}

/// Handles an outgoing sender link (server sending messages to a client).
///
/// Receives messages from a competing-consumer channel and forwards them
/// to the client over the AMQP link.
pub async fn handle_outgoing_messages(
    mut sender: Sender,
    address: String,
    rx: async_channel::Receiver<RouterMessage>,
) -> Result<()> {
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        debug!(address = %address, "Forwarding message to client");
                        if let Err(e) = sender.send(msg).await {
                            debug!(address = %address, error = ?e, "Client disconnected");
                            break;
                        }
                    }
                    Err(_) => {
                        debug!(address = %address, "Channel closed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{QueueConfig, TopicConfig};

    fn test_topology() -> Topology {
        Topology {
            queues: vec![
                QueueConfig {
                    name: "queue-a".to_string(),
                },
                QueueConfig {
                    name: "queue-b".to_string(),
                },
            ],
            topics: vec![TopicConfig {
                name: "topic-x".to_string(),
                subscriptions: vec!["sub-1".to_string(), "sub-2".to_string()],
            }],
        }
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
        // Addresses now include queues + subscription paths + topic names
        assert!(addrs.contains(&"queue-a"));
        assert!(addrs.contains(&"queue-b"));
        assert!(addrs.contains(&"topic-x"));
    }

    #[test]
    fn test_router_publish_no_subscribers() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("test".to_string()),
            )))
            .build();

        // Publishing to a queue with no active receivers should still succeed
        // (message is buffered in the channel)
        let count = router.publish("queue-a", msg);
        assert_eq!(count, Some(1));
    }

    #[test]
    fn test_router_publish_nonexistent_address() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("test".to_string()),
            )))
            .build();

        let count = router.publish("nonexistent", msg);
        assert_eq!(count, None);
    }

    #[test]
    fn test_router_subscribe_and_receive() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let rx = router.subscribe("queue-a").unwrap();

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("hello".to_string()),
            )))
            .build();

        let count = router.publish("queue-a", msg);
        assert_eq!(count, Some(1));

        let received = rx.try_recv().unwrap();
        if let Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Value::String(s))) =
            &received.body
        {
            assert_eq!(s, "hello");
        } else {
            panic!("Unexpected message body");
        }
    }

    #[test]
    fn test_router_subscribe_nonexistent() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert!(router.subscribe("nonexistent").is_none());
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

    #[test]
    fn test_router_competing_consumers_queue() {
        // Two receivers on the same queue — only one should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let rx1 = router.subscribe("queue-a").unwrap();
        let rx2 = router.subscribe("queue-a").unwrap();

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("competing".to_string()),
            )))
            .build();

        let count = router.publish("queue-a", msg);
        assert_eq!(count, Some(1));

        let r1 = rx1.try_recv();
        let r2 = rx2.try_recv();

        // Exactly one should succeed
        assert!(
            (r1.is_ok() && r2.is_err()) || (r1.is_err() && r2.is_ok()),
            "Expected exactly one receiver to get the message"
        );
    }

    #[test]
    fn test_router_topic_fanout_different_subscriptions() {
        // Two receivers on different subscriptions — both should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let rx1 = router
            .subscribe("topic-x/Subscriptions/sub-1")
            .expect("sub-1 should resolve");
        let rx2 = router
            .subscribe("topic-x/Subscriptions/sub-2")
            .expect("sub-2 should resolve");

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("fanout".to_string()),
            )))
            .build();

        let count = router.publish("topic-x", msg);
        assert_eq!(count, Some(2));

        let r1 = rx1.try_recv().unwrap();
        let r2 = rx2.try_recv().unwrap();

        if let Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Value::String(s))) = &r1.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-1");
        }

        if let Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Value::String(s))) = &r2.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-2");
        }
    }

    #[test]
    fn test_router_competing_consumers_same_subscription() {
        // Two receivers on the SAME subscription — only one should get the message
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let rx1 = router.subscribe("topic-x/subscriptions/sub-1").unwrap();
        let rx2 = router.subscribe("topic-x/subscriptions/sub-1").unwrap();

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("compete".to_string()),
            )))
            .build();

        router.publish("topic-x", msg);

        let r1 = rx1.try_recv();
        let r2 = rx2.try_recv();

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

    #[test]
    fn test_router_subscribe_via_subscription_path() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // Subscribe via two different subscription paths — both resolve to different channels
        let rx1 = router
            .subscribe("topic-x/Subscriptions/sub-1")
            .expect("sub-1 should resolve");
        let rx2 = router
            .subscribe("topic-x/Subscriptions/sub-2")
            .expect("sub-2 should resolve");

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("fanout".to_string()),
            )))
            .build();

        // Publish to the topic directly
        let count = router.publish("topic-x", msg);
        assert_eq!(count, Some(2));

        // Both subscription receivers should get the message
        let r1 = rx1.try_recv().unwrap();
        let r2 = rx2.try_recv().unwrap();

        if let Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Value::String(s))) = &r1.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-1");
        }

        if let Body::Value(fe2o3_amqp::types::messaging::AmqpValue(Value::String(s))) = &r2.body {
            assert_eq!(s, "fanout");
        } else {
            panic!("Unexpected body for sub-2");
        }
    }
}
