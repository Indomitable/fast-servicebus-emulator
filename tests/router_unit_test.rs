use fast_servicebus_emulator::config::{
    Config, QueueConfig, SubscriptionEntry, TopicConfig, Topology,
    DEFAULT_CHANNEL_CAPACITY,
};
use fast_servicebus_emulator::router::{
    normalize_address, PublishResult, Router, RouterMessage,
};
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message};
use fe2o3_amqp::types::primitives::Value;
use fe2o3_amqp_types::primitives::OrderedMap;

fn test_topology() -> Topology {
    Topology {
        queues: vec![QueueConfig::new("queue-a"), QueueConfig::new("queue-b")],
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
    let config = Config::from_yaml(yaml).unwrap();
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
    let config = Config::from_yaml(yaml).unwrap();
    let router = Router::from_topology(&config.topology);

    // Send US message
    let mut msg = test_message("event-1");
    let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(OrderedMap::from_iter(
        vec![(
            "region".to_string(),
            fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
        )],
    ));
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
    let config = Config::from_yaml(yaml).unwrap();
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
    let config = Config::from_yaml(yaml).unwrap();
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
