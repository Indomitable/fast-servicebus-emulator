//! Message router backed by `tokio::sync::broadcast` channels.
//!
//! Each queue or topic gets a broadcast channel. Senders publish messages to
//! the channel, and receivers subscribe to it. This provides simple
//! "fire and forget" message delivery with no persistence or locking.

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
#[derive(Clone)]
pub struct Router {
    channels: HashMap<String, broadcast::Sender<BroadcastMessage>>,
}

impl Router {
    /// Creates a new router with a broadcast channel for each queue and topic
    /// defined in the topology.
    pub fn from_topology(topology: &Topology) -> Self {
        let mut channels = HashMap::new();

        for queue in &topology.queues {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            channels.insert(queue.name.clone(), tx);
        }

        for topic in &topology.topics {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            channels.insert(topic.name.clone(), tx);
        }

        Self { channels }
    }

    /// Returns true if the given address has a registered channel.
    pub fn has_address(&self, address: &str) -> bool {
        self.channels.contains_key(address)
    }

    /// Returns a list of all registered addresses.
    pub fn addresses(&self) -> Vec<&str> {
        self.channels.keys().map(|s| s.as_str()).collect()
    }

    /// Publishes a message to the channel for the given address.
    /// Returns the number of receivers that received the message,
    /// or `None` if the address is not registered.
    pub fn publish(&self, address: &str, message: BroadcastMessage) -> Option<usize> {
        self.channels
            .get(address)
            .map(|tx| tx.send(message).unwrap_or(0))
    }

    /// Subscribes to messages on the given address.
    /// Returns `None` if the address is not registered.
    pub fn subscribe(&self, address: &str) -> Option<broadcast::Receiver<BroadcastMessage>> {
        self.channels.get(address).map(|tx| tx.subscribe())
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
/// Subscribes to the broadcast channel for the given address and forwards
/// messages to the client over the AMQP link.
pub async fn handle_outgoing_messages(
    mut sender: Sender,
    address: String,
    router: &Router,
) -> Result<()> {
    let mut rx = match router.subscribe(&address) {
        Some(rx) => rx,
        None => {
            warn!(address = %address, "Address not found in router");
            return Ok(());
        }
    };

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
                subscriptions: vec!["sub-1".to_string()],
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

        // No subscribers, so send returns 0
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

        // Both subscribers should receive the message
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
