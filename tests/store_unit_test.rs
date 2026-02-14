use std::sync::Arc;
use std::time::Duration;

use fast_servicebus_emulator::router::RouterMessage;
use fast_servicebus_emulator::store::{
    DlqStore, EntityConfig, MessageState, MessageStore, SettlementResult,
};
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message};
use fe2o3_amqp::types::primitives::Value;

fn test_message(body: &str) -> RouterMessage {
    Message::builder()
        .body(Body::Value(AmqpValue(Value::String(body.to_string()))))
        .build()
}

fn test_config() -> EntityConfig {
    EntityConfig {
        lock_duration: Duration::from_secs(30),
        max_delivery_count: 10,
        default_message_ttl_ms: 0,
        dead_lettering_on_expiration: false,
        max_size: 0, // unbounded for tests
    }
}

fn make_store(config: EntityConfig) -> (MessageStore, Arc<DlqStore>) {
    let dlq = Arc::new(DlqStore::new());
    let store = MessageStore::new(config, dlq.clone());
    (store, dlq)
}

#[tokio::test]
async fn test_enqueue_and_receive_delete() {
    let (store, _dlq) = make_store(test_config());

    store.enqueue(test_message("hello")).await;
    let envelope = store.receive_and_delete().await;

    assert_eq!(envelope.sequence_number, 1);
    assert_eq!(envelope.delivery_count, 0);
    assert_eq!(store.total_count().await, 0);
}

#[tokio::test]
async fn test_enqueue_and_receive_lock() {
    let (store, _dlq) = make_store(test_config());

    store.enqueue(test_message("hello")).await;
    let envelope = store.receive_and_lock().await;

    assert_eq!(envelope.sequence_number, 1);
    assert_eq!(envelope.delivery_count, 1);
    assert!(matches!(envelope.state, MessageState::Locked { .. }));
    // Message still in store (locked)
    assert_eq!(store.total_count().await, 1);
    assert_eq!(store.available_count().await, 0);
}

#[tokio::test]
async fn test_complete() {
    let (store, _dlq) = make_store(test_config());

    store.enqueue(test_message("hello")).await;
    let envelope = store.receive_and_lock().await;

    let lock_token = match envelope.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked state"),
    };

    let result = store.complete(lock_token).await;
    assert_eq!(result, SettlementResult::Completed);
    assert_eq!(store.total_count().await, 0);
}

#[tokio::test]
async fn test_abandon() {
    let (store, _dlq) = make_store(test_config());

    store.enqueue(test_message("hello")).await;
    let envelope = store.receive_and_lock().await;

    let lock_token = match envelope.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked state"),
    };

    let result = store.abandon(lock_token).await;
    assert_eq!(result, SettlementResult::Abandoned);
    assert_eq!(store.total_count().await, 1);
    assert_eq!(store.available_count().await, 1);
}

#[tokio::test]
async fn test_dead_letter() {
    let (store, dlq) = make_store(test_config());

    store.enqueue(test_message("hello")).await;
    let envelope = store.receive_and_lock().await;

    let lock_token = match envelope.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked state"),
    };

    let result = store.dead_letter(lock_token).await;
    assert_eq!(result, SettlementResult::DeadLettered);
    assert_eq!(store.total_count().await, 0);
    assert_eq!(dlq.len().await, 1);
}

#[tokio::test]
async fn test_auto_dead_letter_on_max_delivery() {
    let config = EntityConfig {
        max_delivery_count: 2,
        ..test_config()
    };
    let (store, dlq) = make_store(config);

    store.enqueue(test_message("hello")).await;

    // First receive + abandon (delivery_count = 1)
    let env1 = store.receive_and_lock().await;
    assert_eq!(env1.delivery_count, 1);
    let lt1 = match env1.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked"),
    };
    store.abandon(lt1).await;

    // Second receive + abandon (delivery_count = 2 = max)
    let env2 = store.receive_and_lock().await;
    assert_eq!(env2.delivery_count, 2);
    let lt2 = match env2.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked"),
    };
    let result = store.abandon(lt2).await;
    assert_eq!(result, SettlementResult::DeadLettered);
    assert_eq!(store.total_count().await, 0);
    assert_eq!(dlq.len().await, 1);
}

#[tokio::test]
async fn test_lock_expiry_makes_available() {
    let config = EntityConfig {
        lock_duration: Duration::from_millis(50),
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    store.enqueue(test_message("hello")).await;
    let _env = store.receive_and_lock().await;

    // Message is locked
    assert_eq!(store.available_count().await, 0);

    // Wait for lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now it should be available again (receive_and_delete triggers unlock check)
    let timeout =
        tokio::time::timeout(Duration::from_millis(200), store.receive_and_delete()).await;
    assert!(timeout.is_ok());
}

#[tokio::test]
async fn test_sequence_numbers_increase() {
    let (store, _dlq) = make_store(test_config());

    store.enqueue(test_message("a")).await;
    store.enqueue(test_message("b")).await;
    store.enqueue(test_message("c")).await;

    let e1 = store.receive_and_delete().await;
    let e2 = store.receive_and_delete().await;
    let e3 = store.receive_and_delete().await;

    assert_eq!(e1.sequence_number, 1);
    assert_eq!(e2.sequence_number, 2);
    assert_eq!(e3.sequence_number, 3);
}

#[tokio::test]
async fn test_renew_lock() {
    let config = EntityConfig {
        lock_duration: Duration::from_millis(100),
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    store.enqueue(test_message("hello")).await;
    let env = store.receive_and_lock().await;
    let lock_token = match env.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked"),
    };

    // Wait 60ms then renew
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(store.renew_lock(lock_token).await);

    // Wait another 60ms — lock would have expired without renewal
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Should still be able to complete (lock was renewed)
    let result = store.complete(lock_token).await;
    assert_eq!(result, SettlementResult::Completed);
}

#[tokio::test]
async fn test_complete_with_expired_lock() {
    let config = EntityConfig {
        lock_duration: Duration::from_millis(50),
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    store.enqueue(test_message("hello")).await;
    let env = store.receive_and_lock().await;
    let lock_token = match env.state {
        MessageState::Locked { lock_token, .. } => lock_token,
        _ => panic!("Expected locked"),
    };

    // Wait for lock to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = store.complete(lock_token).await;
    assert_eq!(result, SettlementResult::LockLost);
}

#[tokio::test]
async fn test_competing_consumers_receive_delete() {
    let (store, _dlq) = make_store(test_config());
    let store = Arc::new(store);

    store.enqueue(test_message("hello")).await;

    let store1 = store.clone();
    let store2 = store.clone();

    let (r1, r2) = tokio::join!(
        tokio::time::timeout(Duration::from_millis(200), store1.receive_and_delete()),
        tokio::time::timeout(Duration::from_millis(200), store2.receive_and_delete()),
    );

    // Exactly one should succeed, the other should timeout
    let got1 = r1.is_ok();
    let got2 = r2.is_ok();
    assert!(
        (got1 && !got2) || (!got1 && got2),
        "Expected exactly one consumer to get the message"
    );
}

#[tokio::test]
async fn test_dlq_receive() {
    let dlq = Arc::new(DlqStore::new());

    dlq.enqueue(test_message("dead-letter-1")).await;
    dlq.enqueue(test_message("dead-letter-2")).await;

    assert_eq!(dlq.len().await, 2);

    let env1 = dlq.receive().await;
    assert_eq!(env1.sequence_number, 1);
    assert_eq!(dlq.len().await, 1);

    let env2 = dlq.try_receive().await;
    assert!(env2.is_some());
    assert_eq!(dlq.len().await, 0);

    let env3 = dlq.try_receive().await;
    assert!(env3.is_none());
}

#[tokio::test]
async fn test_message_ttl_discard() {
    let config = EntityConfig {
        default_message_ttl_ms: 50,
        dead_lettering_on_expiration: false,
        ..test_config()
    };
    let (store, dlq) = make_store(config);

    store.enqueue(test_message("ephemeral")).await;

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Enqueue a non-expiring message so receive_and_delete has something to return
    // after discarding the expired one
    store.enqueue(test_message("persistent")).await;

    let env = tokio::time::timeout(Duration::from_millis(200), store.receive_and_delete())
        .await
        .expect("Should receive persistent message");

    // The expired message should have been discarded, not dead-lettered
    assert_eq!(env.sequence_number, 2); // persistent message
    assert_eq!(dlq.len().await, 0);
}

#[tokio::test]
async fn test_message_ttl_dead_letter() {
    let config = EntityConfig {
        default_message_ttl_ms: 50,
        dead_lettering_on_expiration: true,
        ..test_config()
    };
    let (store, dlq) = make_store(config);

    store.enqueue(test_message("ephemeral")).await;

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Enqueue a non-expiring persistent message (no TTL since config default
    // applies, so we need to trigger expiry check)
    // Actually the default_message_ttl_ms=50 applies to ALL messages in this store.
    // So we need a different approach: trigger expiry processing directly.
    // Let's just check that after a receive attempt the expired message lands in DLQ.

    // Use a second store enqueue to trigger the expiry check
    store.enqueue(test_message("second")).await;

    // Try to receive — the first message is expired, should be dead-lettered
    // The second message also has TTL=50 but was just enqueued so not expired yet
    let env = tokio::time::timeout(Duration::from_millis(200), store.receive_and_delete())
        .await
        .expect("Should receive second message");

    assert_eq!(env.sequence_number, 2);
    assert_eq!(dlq.len().await, 1);
}

#[tokio::test]
async fn test_enqueue_backpressure_rejects_when_full() {
    let config = EntityConfig {
        max_size: 2,
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    assert!(store.enqueue(test_message("a")).await);
    assert!(store.enqueue(test_message("b")).await);
    // Store is now at capacity (2)
    assert!(!store.enqueue(test_message("c")).await);
    assert_eq!(store.total_count().await, 2);
}

#[tokio::test]
async fn test_enqueue_backpressure_accepts_after_drain() {
    let config = EntityConfig {
        max_size: 1,
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    assert!(store.enqueue(test_message("a")).await);
    assert!(!store.enqueue(test_message("b")).await);

    // Consume the message and confirm removal (simulates router behavior)
    store.receive_and_delete().await;
    store.confirm_removal();
    assert_eq!(store.total_count().await, 0);

    // Now we can enqueue again
    assert!(store.enqueue(test_message("c")).await);
    assert_eq!(store.total_count().await, 1);
}

#[tokio::test]
async fn test_enqueue_unbounded_when_max_size_zero() {
    let config = EntityConfig {
        max_size: 0,
        ..test_config()
    };
    let (store, _dlq) = make_store(config);

    // Should accept many messages without issue
    for i in 0..100 {
        assert!(store.enqueue(test_message(&format!("msg-{}", i))).await);
    }
    assert_eq!(store.total_count().await, 100);
}
