use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;

/// Default lock duration in seconds (30s, matching Azure Service Bus default).
pub const DEFAULT_LOCK_DURATION_SECS: u64 = 30;

/// Default max delivery count before dead-lettering (10, matching Azure default).
pub const DEFAULT_MAX_DELIVERY_COUNT: u32 = 10;

/// Default message time-to-live in seconds (disabled / very large).
/// Azure default is TimeSpan.MaxValue; we use 0 to mean "no TTL".
pub const DEFAULT_MESSAGE_TTL_SECS: u64 = 0;

/// Channel capacity for each queue/subscription.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueueConfig {
    pub name: String,
    /// Lock duration in seconds for PeekLock mode. Default: 30.
    #[serde(default = "default_lock_duration")]
    pub lock_duration_secs: u64,
    /// Max delivery attempts before auto-dead-lettering. Default: 10.
    #[serde(default = "default_max_delivery_count")]
    pub max_delivery_count: u32,
    /// Default message TTL in seconds. 0 = no expiry. Default: 0.
    #[serde(default)]
    pub default_message_ttl_secs: u64,
    /// Whether to dead-letter expired messages. Default: false (discard).
    #[serde(default)]
    pub dead_lettering_on_message_expiration: bool,
    /// Maximum number of messages the store can hold. 0 = use DEFAULT_CHANNEL_CAPACITY.
    #[serde(default)]
    pub max_size: usize,
}

impl QueueConfig {
    /// Creates a QueueConfig with the given name and all default settings.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            lock_duration_secs: DEFAULT_LOCK_DURATION_SECS,
            max_delivery_count: DEFAULT_MAX_DELIVERY_COUNT,
            default_message_ttl_secs: DEFAULT_MESSAGE_TTL_SECS,
            dead_lettering_on_message_expiration: false,
            max_size: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionConfig {
    pub name: String,
    /// Lock duration in seconds for PeekLock mode. Default: 30.
    #[serde(default = "default_lock_duration")]
    pub lock_duration_secs: u64,
    /// Max delivery attempts before auto-dead-lettering. Default: 10.
    #[serde(default = "default_max_delivery_count")]
    pub max_delivery_count: u32,
    /// Default message TTL in seconds. 0 = no expiry. Default: 0.
    #[serde(default)]
    pub default_message_ttl_secs: u64,
    /// Whether to dead-letter expired messages. Default: false (discard).
    #[serde(default)]
    pub dead_lettering_on_message_expiration: bool,
    /// Optional filter rule for this subscription.
    #[serde(default)]
    pub filter: Option<SubscriptionFilter>,
    /// Maximum number of messages the store can hold. 0 = use DEFAULT_CHANNEL_CAPACITY.
    #[serde(default)]
    pub max_size: usize,
}

/// Filter types for subscription message routing.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum SubscriptionFilter {
    /// SQL-like boolean expression evaluated against message properties.
    /// Example: "color = 'blue' AND quantity > 5"
    #[serde(rename = "sql")]
    Sql { expression: String },
    /// Matches on exact equality of message properties.
    #[serde(rename = "correlation")]
    Correlation {
        #[serde(default)]
        correlation_id: Option<String>,
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        to: Option<String>,
        #[serde(default)]
        reply_to: Option<String>,
        #[serde(default)]
        subject: Option<String>,
        #[serde(default)]
        content_type: Option<String>,
        /// Custom application properties to match on.
        #[serde(default)]
        properties: std::collections::HashMap<String, String>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub subscriptions: Vec<SubscriptionEntry>,
}

/// A subscription entry can be either a simple string name or a full config object.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubscriptionEntry {
    /// Simple string subscription name (backwards compatible).
    Name(String),
    /// Full subscription configuration with optional properties.
    Config(SubscriptionConfig),
}

impl SubscriptionEntry {
    /// Returns the subscription name regardless of entry type.
    pub fn name(&self) -> &str {
        match self {
            SubscriptionEntry::Name(name) => name,
            SubscriptionEntry::Config(config) => &config.name,
        }
    }

    /// Returns lock duration in seconds.
    pub fn lock_duration_secs(&self) -> u64 {
        match self {
            SubscriptionEntry::Name(_) => DEFAULT_LOCK_DURATION_SECS,
            SubscriptionEntry::Config(config) => config.lock_duration_secs,
        }
    }

    /// Returns max delivery count.
    pub fn max_delivery_count(&self) -> u32 {
        match self {
            SubscriptionEntry::Name(_) => DEFAULT_MAX_DELIVERY_COUNT,
            SubscriptionEntry::Config(config) => config.max_delivery_count,
        }
    }

    /// Returns default message TTL in seconds (0 = no expiry).
    pub fn default_message_ttl_secs(&self) -> u64 {
        match self {
            SubscriptionEntry::Name(_) => DEFAULT_MESSAGE_TTL_SECS,
            SubscriptionEntry::Config(config) => config.default_message_ttl_secs,
        }
    }

    /// Whether to dead-letter expired messages.
    pub fn dead_lettering_on_message_expiration(&self) -> bool {
        match self {
            SubscriptionEntry::Name(_) => false,
            SubscriptionEntry::Config(config) => config.dead_lettering_on_message_expiration,
        }
    }

    /// Returns the subscription filter, if any.
    pub fn filter(&self) -> Option<&SubscriptionFilter> {
        match self {
            SubscriptionEntry::Name(_) => None,
            SubscriptionEntry::Config(config) => config.filter.as_ref(),
        }
    }

    /// Returns the max store size (0 = use default).
    pub fn max_size(&self) -> usize {
        match self {
            SubscriptionEntry::Name(_) => 0,
            SubscriptionEntry::Config(config) => config.max_size,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Topology {
    pub queues: Vec<QueueConfig>,
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub topology: Topology,
}

fn default_lock_duration() -> u64 {
    DEFAULT_LOCK_DURATION_SECS
}

fn default_max_delivery_count() -> u32 {
    DEFAULT_MAX_DELIVERY_COUNT
}

impl Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // serialize to yaml using serde
        let yaml = serde_yaml::to_string(self).unwrap();
        write!(f, "\n{}", yaml)
    }
}

impl Config {
    /// Loads topology from a YAML file.
    pub fn load(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let config: Config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Parses topology from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topology() {
        let yaml = r#"
topology:
    queues:
      - name: "queue-a"
      - name: "queue-b"
    topics:
      - name: "topic-x"
        subscriptions:
          - "sub-1"
          - "sub-2"
"#;
        let topology = Config::from_yaml(yaml).unwrap().topology;
        assert_eq!(topology.queues.len(), 2);
        assert_eq!(topology.queues[0].name, "queue-a");
        assert_eq!(topology.queues[1].name, "queue-b");
        assert_eq!(topology.topics.len(), 1);
        assert_eq!(topology.topics[0].name, "topic-x");
        assert_eq!(topology.topics[0].subscriptions[0].name(), "sub-1");
        assert_eq!(topology.topics[0].subscriptions[1].name(), "sub-2");
    }

    #[test]
    fn test_parse_topology_with_properties() {
        let yaml = r#"
topology:
    queues:
      - name: "queue-a"
        lock_duration_secs: 60
        max_delivery_count: 5
        default_message_ttl_secs: 3600
        dead_lettering_on_message_expiration: true
    topics:
      - name: "topic-x"
        subscriptions:
          - name: "sub-1"
            lock_duration_secs: 45
            max_delivery_count: 3
          - "sub-2"
"#;
        let config = Config::from_yaml(yaml).unwrap();
        let topology = &config.topology;

        // Queue with explicit properties
        let q = &topology.queues[0];
        assert_eq!(q.lock_duration_secs, 60);
        assert_eq!(q.max_delivery_count, 5);
        assert_eq!(q.default_message_ttl_secs, 3600);
        assert!(q.dead_lettering_on_message_expiration);

        // Subscription with explicit properties
        let sub1 = &topology.topics[0].subscriptions[0];
        assert_eq!(sub1.name(), "sub-1");
        assert_eq!(sub1.lock_duration_secs(), 45);
        assert_eq!(sub1.max_delivery_count(), 3);

        // Simple string subscription gets defaults
        let sub2 = &topology.topics[0].subscriptions[1];
        assert_eq!(sub2.name(), "sub-2");
        assert_eq!(sub2.lock_duration_secs(), DEFAULT_LOCK_DURATION_SECS);
        assert_eq!(sub2.max_delivery_count(), DEFAULT_MAX_DELIVERY_COUNT);
    }

    #[test]
    fn test_parse_topology_with_sql_filter() {
        let yaml = r#"
topology:
    queues: []
    topics:
      - name: "orders"
        subscriptions:
          - name: "high-priority"
            filter:
              type: sql
              expression: "priority = 'high'"
"#;
        let config = Config::from_yaml(yaml).unwrap();
        let sub = &config.topology.topics[0].subscriptions[0];
        match sub.filter().unwrap() {
            SubscriptionFilter::Sql { expression } => {
                assert_eq!(expression, "priority = 'high'");
            }
            _ => panic!("Expected SQL filter"),
        }
    }

    #[test]
    fn test_parse_topology_with_correlation_filter() {
        let yaml = r#"
topology:
    queues: []
    topics:
      - name: "events"
        subscriptions:
          - name: "by-subject"
            filter:
              type: correlation
              subject: "order-created"
              properties:
                region: "us-east"
"#;
        let config = Config::from_yaml(yaml).unwrap();
        let sub = &config.topology.topics[0].subscriptions[0];
        match sub.filter().unwrap() {
            SubscriptionFilter::Correlation {
                subject,
                properties,
                ..
            } => {
                assert_eq!(subject.as_deref(), Some("order-created"));
                assert_eq!(properties.get("region").unwrap(), "us-east");
            }
            _ => panic!("Expected correlation filter"),
        }
    }

    #[test]
    fn test_queue_defaults() {
        let yaml = r#"
topology:
    queues:
      - name: "simple-queue"
    topics: []
"#;
        let config = Config::from_yaml(yaml).unwrap();
        let q = &config.topology.queues[0];
        assert_eq!(q.lock_duration_secs, DEFAULT_LOCK_DURATION_SECS);
        assert_eq!(q.max_delivery_count, DEFAULT_MAX_DELIVERY_COUNT);
        assert_eq!(q.default_message_ttl_secs, 0);
        assert!(!q.dead_lettering_on_message_expiration);
    }

    #[test]
    fn test_parse_empty_topology() {
        let yaml = "topology:\n  queues: []\n  topics: []\n";
        let topology = Config::from_yaml(yaml).unwrap().topology;
        assert!(topology.queues.is_empty());
        assert!(topology.topics.is_empty());
    }

    #[test]
    fn test_parse_invalid_yaml() {
        let yaml = "not: valid: yaml: [[[";
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn test_parse_missing_fields() {
        let yaml = "queues:\n  - name: test\n";
        // topics is missing — should fail
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn test_load_topology_file() {
        let topology = Config::load("config.yaml").unwrap().topology;
        assert_eq!(topology.queues.len(), 12);
        assert_eq!(topology.topics.len(), 4);
    }

    #[test]
    fn test_load_nonexistent_file() {
        assert!(Config::load("nonexistent.yaml").is_err());
    }
}
