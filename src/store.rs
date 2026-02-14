//! Message store with PeekLock support, delivery tracking, and dead-letter queues.
//!
//! Each queue and subscription gets a `MessageStore` that holds messages with
//! broker metadata (sequence number, enqueued time, delivery count, lock state).
//!
//! The store supports two delivery modes:
//! - **ReceiveAndDelete**: message is removed immediately on delivery.
//! - **PeekLock**: message is locked for a configurable duration. The consumer
//!   must `complete`, `abandon`, or `dead-letter` it before the lock expires.
//!   If the lock expires, the message becomes available again.
//!
//! ## DLQ Architecture
//!
//! Each `MessageStore` holds an `Option<Arc<DlqStore>>`. The `DlqStore` is a
//! separate, simpler type that has no nested DLQ — dead-lettered messages are
//! terminal. This avoids recursive types and unsafe code entirely.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::router::RouterMessage;

/// Unique identifier for a locked message, used for settlement.
pub type LockToken = Uuid;

/// Monotonically increasing sequence number assigned to each enqueued message.
pub type SequenceNumber = u64;

/// State of a message in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageState {
    /// Available for delivery.
    Available,
    /// Locked by a consumer. Contains the lock expiry instant and lock token.
    Locked {
        lock_token: LockToken,
        locked_until: Instant,
    },
}

/// A message with broker-assigned metadata.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The AMQP message.
    pub message: RouterMessage,
    /// Broker-assigned sequence number (monotonically increasing per entity).
    pub sequence_number: SequenceNumber,
    /// When the message was enqueued (as milliseconds since UNIX epoch).
    pub enqueued_time_utc: u64,
    /// Number of times delivery has been attempted.
    pub delivery_count: u32,
    /// Current state (Available or Locked).
    pub state: MessageState,
    /// Optional TTL. If set, the message expires at `enqueued_time_utc + ttl_ms`.
    pub ttl_ms: Option<u64>,
}

impl Envelope {
    /// Returns true if this message has expired based on the current time.
    pub fn is_expired(&self, now_epoch_ms: u64) -> bool {
        if let Some(ttl) = self.ttl_ms {
            if ttl > 0 {
                return now_epoch_ms >= self.enqueued_time_utc + ttl;
            }
        }
        false
    }

    /// Returns true if the lock has expired (PeekLock only).
    pub fn is_lock_expired(&self) -> bool {
        match &self.state {
            MessageState::Locked { locked_until, .. } => Instant::now() >= *locked_until,
            _ => false,
        }
    }
}

/// Configuration for a message store entity.
#[derive(Debug, Clone)]
pub struct EntityConfig {
    /// Lock duration for PeekLock mode.
    pub lock_duration: Duration,
    /// Max delivery attempts before auto-dead-lettering.
    pub max_delivery_count: u32,
    /// Default message TTL in milliseconds. 0 = no expiry.
    pub default_message_ttl_ms: u64,
    /// Whether to dead-letter expired messages (true) or discard them (false).
    pub dead_lettering_on_expiration: bool,
    /// Maximum number of messages the store can hold. 0 = unbounded.
    pub max_size: usize,
}

impl Default for EntityConfig {
    fn default() -> Self {
        Self {
            lock_duration: Duration::from_secs(30),
            max_delivery_count: 10,
            default_message_ttl_ms: 0,
            dead_lettering_on_expiration: false,
            max_size: 0,
        }
    }
}

/// Result of attempting to receive a message.
#[derive(Debug)]
pub enum ReceiveResult {
    /// Message delivered in ReceiveAndDelete mode (already removed from store).
    Consumed(Envelope),
    /// Message delivered in PeekLock mode (locked in store, needs settlement).
    Locked(Envelope),
}

/// Result of a settlement operation.
#[derive(Debug, PartialEq, Eq)]
pub enum SettlementResult {
    /// Message was successfully completed (removed).
    Completed,
    /// Message was abandoned (unlocked, delivery count incremented).
    Abandoned,
    /// Message was dead-lettered.
    DeadLettered,
    /// Lock token not found or lock expired.
    LockLost,
}

// ---------------------------------------------------------------------------
// DlqStore — a simple store for dead-lettered messages (no nested DLQ)
// ---------------------------------------------------------------------------

/// Inner mutable state shared by both store types.
struct StoreInner {
    /// Message queue (available + locked messages).
    messages: VecDeque<Envelope>,
    /// Index from lock token to sequence number for fast lookup.
    lock_index: HashMap<LockToken, SequenceNumber>,
}

impl StoreInner {
    fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            lock_index: HashMap::new(),
        }
    }
}

/// A dead-letter queue store. Simpler than `MessageStore` — no nested DLQ,
/// no TTL processing, no auto-dead-lettering. Messages here are terminal.
pub struct DlqStore {
    inner: Mutex<StoreInner>,
    notify: Notify,
    next_sequence: AtomicU64,
    config: EntityConfig,
}

impl DlqStore {
    /// Creates a new empty DLQ store.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(StoreInner::new()),
            notify: Notify::new(),
            next_sequence: AtomicU64::new(1),
            config: EntityConfig::default(),
        }
    }

    /// Enqueues a message into the dead-letter queue.
    pub async fn enqueue(&self, message: RouterMessage) {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let envelope = Envelope {
            message,
            sequence_number: seq,
            enqueued_time_utc: epoch_ms(),
            delivery_count: 0,
            state: MessageState::Available,
            ttl_ms: None, // DLQ messages don't expire
        };
        let mut inner = self.inner.lock().await;
        inner.messages.push_back(envelope);
        drop(inner);
        self.notify.notify_one();
    }

    /// Enqueues an existing envelope into the DLQ, resetting its state.
    pub async fn enqueue_envelope(&self, mut envelope: Envelope) {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        envelope.sequence_number = seq;
        envelope.state = MessageState::Available;
        envelope.ttl_ms = None; // DLQ messages don't expire
        let mut inner = self.inner.lock().await;
        inner.messages.push_back(envelope);
        drop(inner);
        self.notify.notify_one();
    }

    /// Receives a message from the DLQ in ReceiveAndDelete mode.
    /// Blocks until a message is available.
    pub async fn receive(&self) -> Envelope {
        loop {
            {
                let mut inner = self.inner.lock().await;
                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| matches!(e.state, MessageState::Available))
                {
                    return inner.messages.remove(idx).unwrap();
                }
            }
            self.notify.notified().await;
        }
    }

    /// Receives a message from the DLQ in PeekLock mode.
    /// Blocks until a message is available.
    pub async fn receive_and_lock(&self) -> Envelope {
        loop {
            {
                let mut inner = self.inner.lock().await;
                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| matches!(e.state, MessageState::Available))
                {
                    let lock_token = Uuid::new_v4();
                    let locked_until = Instant::now() + self.config.lock_duration;
                    let envelope = &mut inner.messages[idx];
                    envelope.state = MessageState::Locked {
                        lock_token,
                        locked_until,
                    };
                    envelope.delivery_count += 1;
                    let seq = envelope.sequence_number;
                    let result = envelope.clone();
                    inner.lock_index.insert(lock_token, seq);
                    return result;
                }
            }
            self.notify.notified().await;
        }
    }

    /// Completes a locked DLQ message (removes it).
    pub async fn complete(&self, lock_token: LockToken) -> SettlementResult {
        let mut inner = self.inner.lock().await;
        if let Some(seq) = inner.lock_index.remove(&lock_token) {
            if let Some(idx) = inner
                .messages
                .iter()
                .position(|e| e.sequence_number == seq)
            {
                if let MessageState::Locked {
                    lock_token: lt,
                    locked_until,
                } = &inner.messages[idx].state
                {
                    if *lt == lock_token && Instant::now() < *locked_until {
                        inner.messages.remove(idx);
                        return SettlementResult::Completed;
                    }
                }
            }
        }
        SettlementResult::LockLost
    }

    /// Abandons a locked DLQ message (unlocks it, no further dead-lettering).
    pub async fn abandon(&self, lock_token: LockToken) -> SettlementResult {
        let mut inner = self.inner.lock().await;
        if let Some(seq) = inner.lock_index.remove(&lock_token) {
            if let Some(idx) = inner
                .messages
                .iter()
                .position(|e| e.sequence_number == seq)
            {
                if let MessageState::Locked {
                    lock_token: lt,
                    locked_until,
                } = &inner.messages[idx].state
                {
                    if *lt == lock_token && Instant::now() < *locked_until {
                        inner.messages[idx].state = MessageState::Available;
                        drop(inner);
                        self.notify.notify_one();
                        return SettlementResult::Abandoned;
                    }
                }
            }
        }
        SettlementResult::LockLost
    }

    /// Non-blocking receive attempt.
    pub async fn try_receive(&self) -> Option<Envelope> {
        let mut inner = self.inner.lock().await;
        if let Some(idx) = inner
            .messages
            .iter()
            .position(|e| matches!(e.state, MessageState::Available))
        {
            inner.messages.remove(idx)
        } else {
            None
        }
    }

    /// Returns the number of messages in the DLQ.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.messages.len()
    }

    /// Returns true if the DLQ is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.messages.is_empty()
    }
}

// ---------------------------------------------------------------------------
// MessageStore — the main message store for queues and subscriptions
// ---------------------------------------------------------------------------

/// A message store for a single queue or subscription.
///
/// Thread-safe and supports multiple concurrent consumers (competing consumers).
/// Uses `Notify` to wake consumers when new messages arrive.
pub struct MessageStore {
    inner: Mutex<StoreInner>,
    /// Notifies waiting consumers that a new message is available.
    notify: Notify,
    /// Monotonically increasing sequence number generator.
    next_sequence: AtomicU64,
    /// Entity configuration (lock duration, max delivery count, etc.).
    config: EntityConfig,
    /// Dead-letter queue for this entity. `None` only if this entity has no DLQ
    /// (which shouldn't happen for real entities — always `Some` in practice).
    dlq: Option<Arc<DlqStore>>,
    /// Logical queue depth for backpressure enforcement.
    ///
    /// Tracks messages from the sender's perspective: incremented on enqueue,
    /// decremented only when a message is fully settled (ReceiveAndDelete delivery
    /// confirmed, PeekLock completed/dead-lettered, or TTL expired).
    ///
    /// This differs from `inner.messages.len()` because `receive_and_delete()`
    /// removes messages from the VecDeque before the AMQP delivery is confirmed.
    /// Without this counter, a fast consumer drains the store and the `max_size`
    /// check never triggers.
    logical_count: AtomicUsize,
}

impl MessageStore {
    /// Creates a new message store with the given configuration and DLQ.
    pub fn new(config: EntityConfig, dlq: Arc<DlqStore>) -> Self {
        Self {
            inner: Mutex::new(StoreInner::new()),
            notify: Notify::new(),
            next_sequence: AtomicU64::new(1),
            config,
            dlq: Some(dlq),
            logical_count: AtomicUsize::new(0),
        }
    }

    /// Enqueues a message into the store.
    ///
    /// Assigns a sequence number and enqueued timestamp. If the message doesn't
    /// have a TTL set, applies the entity's default TTL.
    ///
    /// Returns `true` if the message was accepted, `false` if the store is at
    /// capacity (backpressure). When `max_size` is 0 the store is unbounded.
    pub async fn enqueue(&self, message: RouterMessage) -> bool {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let now_ms = epoch_ms();

        // Determine TTL: use message header TTL, or entity default
        let msg_ttl = message
            .header
            .as_ref()
            .and_then(|h| h.ttl)
            .map(|ms| ms as u64);
        let ttl_ms = msg_ttl.or(if self.config.default_message_ttl_ms > 0 {
            Some(self.config.default_message_ttl_ms)
        } else {
            None
        });

        let envelope = Envelope {
            message,
            sequence_number: seq,
            enqueued_time_utc: now_ms,
            delivery_count: 0,
            state: MessageState::Available,
            ttl_ms,
        };

        let mut inner = self.inner.lock().await;
        if self.config.max_size > 0 {
            let current_logical = self.logical_count.load(Ordering::Acquire);
            tracing::debug!(
                max_size = self.config.max_size,
                logical_count = current_logical,
                seq = seq,
                "Enqueue check: max_size={}, logical_count={}",
                self.config.max_size,
                current_logical
            );
            if current_logical >= self.config.max_size {
                tracing::warn!(
                    max_size = self.config.max_size,
                    logical_count = current_logical,
                    "REJECTING message — store at capacity"
                );
                return false;
            }
        }
        self.logical_count.fetch_add(1, Ordering::Release);
        inner.messages.push_back(envelope);
        drop(inner);
        self.notify.notify_one();
        true
    }

    /// Receives a message in ReceiveAndDelete mode.
    ///
    /// Blocks until a message is available, then removes and returns it.
    pub async fn receive_and_delete(&self) -> Envelope {
        loop {
            // Collect expired messages to dead-letter outside the lock
            let expired = {
                let mut inner = self.inner.lock().await;
                self.unlock_expired_locks(&mut inner);
                let expired = self.collect_expired_messages(&mut inner);

                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| matches!(e.state, MessageState::Available))
                {
                    // Process any collected expired messages before returning
                    self.dead_letter_expired(expired).await;
                    return inner.messages.remove(idx).unwrap();
                }
                expired
            };
            // Process expired messages outside the lock
            self.dead_letter_expired(expired).await;
            self.notify.notified().await;
        }
    }

    /// Receives a message in PeekLock mode.
    ///
    /// Blocks until a message is available, then locks it and returns it.
    /// The consumer must settle (complete/abandon/dead-letter) within the
    /// lock duration.
    pub async fn receive_and_lock(&self) -> Envelope {
        loop {
            let expired = {
                let mut inner = self.inner.lock().await;
                self.unlock_expired_locks(&mut inner);
                let expired = self.collect_expired_messages(&mut inner);

                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| matches!(e.state, MessageState::Available))
                {
                    let lock_token = Uuid::new_v4();
                    let locked_until = Instant::now() + self.config.lock_duration;
                    let envelope = &mut inner.messages[idx];
                    envelope.state = MessageState::Locked {
                        lock_token,
                        locked_until,
                    };
                    envelope.delivery_count += 1;
                    let seq = envelope.sequence_number;
                    let result = envelope.clone();
                    inner.lock_index.insert(lock_token, seq);

                    // Process expired outside lock
                    self.dead_letter_expired(expired).await;
                    return result;
                }
                expired
            };
            self.dead_letter_expired(expired).await;
            self.notify.notified().await;
        }
    }

    /// Completes a locked message (removes it from the store).
    pub async fn complete(&self, lock_token: LockToken) -> SettlementResult {
        let mut inner = self.inner.lock().await;
        if let Some(seq) = inner.lock_index.remove(&lock_token) {
            if let Some(idx) = inner
                .messages
                .iter()
                .position(|e| e.sequence_number == seq)
            {
                if let MessageState::Locked {
                    lock_token: lt,
                    locked_until,
                } = &inner.messages[idx].state
                {
                    if *lt == lock_token && Instant::now() < *locked_until {
                        inner.messages.remove(idx);
                        self.logical_count.fetch_sub(1, Ordering::Release);
                        return SettlementResult::Completed;
                    }
                }
            }
        }
        SettlementResult::LockLost
    }

    /// Abandons a locked message (unlocks it).
    ///
    /// If the delivery count has reached `max_delivery_count`, the message is
    /// automatically dead-lettered instead.
    pub async fn abandon(&self, lock_token: LockToken) -> SettlementResult {
        let maybe_dead_letter = {
            let mut inner = self.inner.lock().await;
            if let Some(seq) = inner.lock_index.remove(&lock_token) {
                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| e.sequence_number == seq)
                {
                    if let MessageState::Locked {
                        lock_token: lt,
                        locked_until,
                    } = &inner.messages[idx].state
                    {
                        if *lt == lock_token && Instant::now() < *locked_until {
                            // Check max delivery count
                            if inner.messages[idx].delivery_count >= self.config.max_delivery_count {
                                let envelope = inner.messages.remove(idx).unwrap();
                                self.logical_count.fetch_sub(1, Ordering::Release);
                                Some(envelope) // Will dead-letter outside lock
                            } else {
                                inner.messages[idx].state = MessageState::Available;
                                drop(inner);
                                self.notify.notify_one();
                                return SettlementResult::Abandoned;
                            }
                        } else {
                            return SettlementResult::LockLost;
                        }
                    } else {
                        return SettlementResult::LockLost;
                    }
                } else {
                    return SettlementResult::LockLost;
                }
            } else {
                return SettlementResult::LockLost;
            }
        };

        if let Some(envelope) = maybe_dead_letter {
            if let Some(dlq) = &self.dlq {
                dlq.enqueue(envelope.message).await;
            }
            return SettlementResult::DeadLettered;
        }

        SettlementResult::LockLost
    }

    /// Dead-letters a locked message (moves it to the DLQ).
    pub async fn dead_letter(&self, lock_token: LockToken) -> SettlementResult {
        let maybe_envelope = {
            let mut inner = self.inner.lock().await;
            if let Some(seq) = inner.lock_index.remove(&lock_token) {
                if let Some(idx) = inner
                    .messages
                    .iter()
                    .position(|e| e.sequence_number == seq)
                {
                    if let MessageState::Locked {
                        lock_token: lt,
                        locked_until,
                    } = &inner.messages[idx].state
                    {
                        if *lt == lock_token && Instant::now() < *locked_until {
                            let envelope = inner.messages.remove(idx).unwrap();
                            self.logical_count.fetch_sub(1, Ordering::Release);
                            Some(envelope)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(envelope) = maybe_envelope {
            if let Some(dlq) = &self.dlq {
                dlq.enqueue(envelope.message).await;
            }
            return SettlementResult::DeadLettered;
        }

        SettlementResult::LockLost
    }

    /// Renews the lock on a message.
    pub async fn renew_lock(&self, lock_token: LockToken) -> bool {
        let mut inner = self.inner.lock().await;
        if let Some(&seq) = inner.lock_index.get(&lock_token) {
            if let Some(envelope) = inner
                .messages
                .iter_mut()
                .find(|e| e.sequence_number == seq)
            {
                if let MessageState::Locked {
                    lock_token: lt,
                    locked_until,
                } = &mut envelope.state
                {
                    if *lt == lock_token && Instant::now() < *locked_until {
                        *locked_until = Instant::now() + self.config.lock_duration;
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Returns the DLQ store for this entity.
    pub fn dlq(&self) -> Option<&Arc<DlqStore>> {
        self.dlq.as_ref()
    }

    /// Decrements the logical queue depth counter.
    ///
    /// Called after a ReceiveAndDelete delivery is confirmed by the consumer
    /// (i.e. `sender.send()` succeeded). In PeekLock mode, `complete()` and
    /// `dead_letter()` handle this automatically.
    pub fn confirm_removal(&self) {
        self.logical_count.fetch_sub(1, Ordering::Release);
    }

    /// Returns the number of available messages.
    pub async fn available_count(&self) -> usize {
        let inner = self.inner.lock().await;
        inner
            .messages
            .iter()
            .filter(|e| matches!(e.state, MessageState::Available))
            .count()
    }

    /// Returns the total number of messages (available + locked).
    pub async fn total_count(&self) -> usize {
        self.inner.lock().await.messages.len()
    }

    /// Returns the entity configuration.
    pub fn config(&self) -> &EntityConfig {
        &self.config
    }

    /// Notifies waiting consumers (used when messages are added externally).
    pub fn notify_consumers(&self) {
        self.notify.notify_one();
    }

    /// Unlocks messages whose lock has expired, making them available again.
    fn unlock_expired_locks(&self, inner: &mut StoreInner) {
        let now = Instant::now();
        let mut expired_tokens = Vec::new();

        for envelope in inner.messages.iter_mut() {
            if let MessageState::Locked {
                lock_token,
                locked_until,
            } = &envelope.state
            {
                if now >= *locked_until {
                    expired_tokens.push(*lock_token);
                    envelope.state = MessageState::Available;
                }
            }
        }

        for token in expired_tokens {
            inner.lock_index.remove(&token);
        }
    }

    /// Collects expired messages (TTL) from the store, removing them.
    /// Returns the envelopes that should be dead-lettered (if configured)
    /// or simply discards them. Must be called while holding the lock.
    /// The actual DLQ enqueue happens outside the lock via `dead_letter_expired`.
    fn collect_expired_messages(&self, inner: &mut StoreInner) -> Vec<Envelope> {
        let now_ms = epoch_ms();
        let mut to_dlq = Vec::new();
        let mut to_discard = Vec::new();

        for (idx, envelope) in inner.messages.iter().enumerate() {
            if matches!(envelope.state, MessageState::Available) && envelope.is_expired(now_ms) {
                if self.config.dead_lettering_on_expiration {
                    to_dlq.push(idx);
                } else {
                    to_discard.push(idx);
                }
            }
        }

        let total_expired = to_dlq.len() + to_discard.len();

        // Remove in reverse order to preserve indices
        let mut expired_envelopes = Vec::new();
        for &idx in to_dlq.iter().rev() {
            expired_envelopes.push(inner.messages.remove(idx).unwrap());
        }
        for &idx in to_discard.iter().rev() {
            inner.messages.remove(idx);
        }

        // Decrement logical count for all expired messages
        if total_expired > 0 {
            self.logical_count.fetch_sub(total_expired, Ordering::Release);
        }

        expired_envelopes
    }

    /// Dead-letters previously collected expired messages. Called outside the
    /// store lock to avoid deadlock.
    async fn dead_letter_expired(&self, expired: Vec<Envelope>) {
        if expired.is_empty() {
            return;
        }
        if let Some(dlq) = &self.dlq {
            for envelope in expired {
                dlq.enqueue(envelope.message).await;
            }
        }
    }
}

/// Returns current time as milliseconds since UNIX epoch.
pub fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

