//! Message router backed by `tokio::sync::broadcast` channels.
//!
//! Each queue or topic gets a broadcast channel. Senders publish messages to
//! the channel, and receivers subscribe to it. This provides simple
//! "fire and forget" message delivery with no persistence or locking.
//!
//! Subscriptions are resolved by mapping paths like
//! `events-topic/subscriptions/sub-1` (case-insensitive) to the parent
//! topic's broadcast channel, so all subscribers receive every message.

use anyhow::Result;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::types::messaging::{Body, Message};
use fe2o3_amqp::types::primitives::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::config::Topology;

/// Channel capacity for each queue/topic.
const CHANNEL_CAPACITY: usize = 1000;

/// Type alias for a message routed through broadcast channels.
pub type BroadcastMessage = Message<Body<Value>>;

/// Immutable message router. Created once at startup from the topology
/// configuration and shared across all connections via `Arc`.
///
/// `channels` maps queue/topic names to their broadcast sender.
/// `subscriptions` maps lowercased subscription paths (e.g.
/// `"events-topic/subscriptions/sub-1"`) to the parent topic name.
#[derive(Clone)]
pub struct Router {
    channels: HashMap<String, broadcast::Sender<BroadcastMessage>>,
    subscriptions: HashMap<String, String>,
}

impl Router {
    /// Creates a new router with a broadcast channel for each queue and topic
    /// defined in the topology, and a subscription lookup for each
    /// `{topic}/subscriptions/{sub}` path.
    pub fn from_topology(topology: &Topology) -> Self {
        let mut channels = HashMap::new();
        let mut subscriptions = HashMap::new();

        for queue in &topology.queues {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            channels.insert(queue.name.clone(), tx);
        }

        for topic in &topology.topics {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            channels.insert(topic.name.clone(), tx);

            for sub in &topic.subscriptions {
                let path = format!("{}/subscriptions/{}", topic.name, sub).to_lowercase();
                subscriptions.insert(path, topic.name.clone());
            }
        }

        Self {
            channels,
            subscriptions,
        }
    }

    /// Resolves an address to the channel name it maps to.
    ///
    /// - Direct channel names (queues and topics) are returned as-is.
    /// - Subscription paths like `events-topic/Subscriptions/sub-1` are
    ///   resolved case-insensitively to the parent topic name.
    /// - Returns `None` if the address is unknown.
    pub fn resolve_address(&self, address: &str) -> Option<&str> {
        // Direct channel match — return the key owned by the HashMap
        if let Some((key, _)) = self.channels.get_key_value(address) {
            return Some(key.as_str());
        }

        // Case-insensitive subscription path lookup
        let lower = address.to_lowercase();
        self.subscriptions.get(&lower).map(|s| s.as_str())
    }

    /// Returns true if the given address can be resolved to a channel.
    pub fn has_address(&self, address: &str) -> bool {
        self.resolve_address(address).is_some()
    }

    /// Returns a list of all direct channel addresses (queues and topics).
    pub fn addresses(&self) -> Vec<&str> {
        self.channels.keys().map(|s| s.as_str()).collect()
    }

    /// Returns a list of all registered subscription paths (lowercased).
    pub fn subscription_paths(&self) -> Vec<&str> {
        self.subscriptions.keys().map(|s| s.as_str()).collect()
    }

    /// Publishes a message to the channel for the given address.
    /// Returns the number of receivers that received the message,
    /// or `None` if the address cannot be resolved.
    pub fn publish(&self, address: &str, message: BroadcastMessage) -> Option<usize> {
        let resolved = self.resolve_address(address)?;
        self.channels
            .get(resolved)
            .map(|tx| tx.send(message).unwrap_or(0))
    }

    /// Subscribes to messages on the given address.
    /// Subscription paths are resolved to the parent topic's channel.
    /// Returns `None` if the address cannot be resolved.
    pub fn subscribe(&self, address: &str) -> Option<broadcast::Receiver<BroadcastMessage>> {
        let resolved = self.resolve_address(address)?;
        self.channels.get(resolved).map(|tx| tx.subscribe())
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
/// corresponding broadcast channel.
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
/// Uses a pre-subscribed broadcast receiver to forward messages to the
/// client over the AMQP link. The subscription must be created before this
/// function is called to avoid missing messages published between link
/// attachment and subscription.
pub async fn handle_outgoing_messages(
    mut sender: Sender,
    address: String,
    mut rx: broadcast::Receiver<BroadcastMessage>,
) -> Result<()> {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                debug!(address = %address, "Forwarding message to client");
                if let Err(e) = sender.send(msg).await {
                    debug!(address = %address, error = ?e, "Client disconnected");
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!(address = %address, count, "Receiver lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!(address = %address, "Channel closed");
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
        assert_eq!(addrs, vec!["queue-a", "queue-b", "topic-x"]);
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

        let count = router.publish("queue-a", msg);
        assert_eq!(count, Some(0));
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

        let mut rx = router.subscribe("queue-a").unwrap();

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

        assert!(router.addresses().is_empty());
        assert!(!router.has_address("anything"));
    }

    #[test]
    fn test_router_multiple_subscribers() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        let mut rx1 = router.subscribe("queue-a").unwrap();
        let mut rx2 = router.subscribe("queue-a").unwrap();

        let msg = Message::builder()
            .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
                Value::String("broadcast".to_string()),
            )))
            .build();

        let count = router.publish("queue-a", msg);
        assert_eq!(count, Some(2));

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
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
            Some("topic-x")
        );
    }

    #[test]
    fn test_router_resolve_subscription_capitalized() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        // Azure SDK sends "Subscriptions" with capital S
        assert_eq!(
            router.resolve_address("topic-x/Subscriptions/sub-1"),
            Some("topic-x")
        );
    }

    #[test]
    fn test_router_resolve_subscription_mixed_case() {
        let topology = test_topology();
        let router = Router::from_topology(&topology);

        assert_eq!(
            router.resolve_address("topic-x/SUBSCRIPTIONS/SUB-1"),
            Some("topic-x")
        );
        assert_eq!(
            router.resolve_address("Topic-X/Subscriptions/Sub-1"),
            Some("topic-x")
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

        // Subscribe via two different subscription paths — both resolve to topic-x
        let mut rx1 = router
            .subscribe("topic-x/Subscriptions/sub-1")
            .expect("sub-1 should resolve");
        let mut rx2 = router
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
