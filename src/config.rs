use anyhow::Result;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize, Clone)]
pub struct QueueConfig {
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub subscriptions: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Topology {
    pub queues: Vec<QueueConfig>,
    pub topics: Vec<TopicConfig>,
}

impl Topology {
    /// Loads topology from a YAML file.
    pub fn load(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let topology: Topology = serde_yaml::from_str(&content)?;
        Ok(topology)
    }

    /// Parses topology from a YAML string.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let topology: Topology = serde_yaml::from_str(yaml)?;
        Ok(topology)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topology() {
        let yaml = r#"
queues:
  - name: "queue-a"
  - name: "queue-b"
topics:
  - name: "topic-x"
    subscriptions:
      - "sub-1"
      - "sub-2"
"#;
        let topology = Topology::from_yaml(yaml).unwrap();
        assert_eq!(topology.queues.len(), 2);
        assert_eq!(topology.queues[0].name, "queue-a");
        assert_eq!(topology.queues[1].name, "queue-b");
        assert_eq!(topology.topics.len(), 1);
        assert_eq!(topology.topics[0].name, "topic-x");
        assert_eq!(topology.topics[0].subscriptions, vec!["sub-1", "sub-2"]);
    }

    #[test]
    fn test_parse_empty_topology() {
        let yaml = "queues: []\ntopics: []\n";
        let topology = Topology::from_yaml(yaml).unwrap();
        assert!(topology.queues.is_empty());
        assert!(topology.topics.is_empty());
    }

    #[test]
    fn test_parse_invalid_yaml() {
        let yaml = "not: valid: yaml: [[[";
        assert!(Topology::from_yaml(yaml).is_err());
    }

    #[test]
    fn test_parse_missing_fields() {
        let yaml = "queues:\n  - name: test\n";
        // topics is missing — should fail
        assert!(Topology::from_yaml(yaml).is_err());
    }

    #[test]
    fn test_load_topology_file() {
        let topology = Topology::load("topology.yaml").unwrap();
        assert_eq!(topology.queues.len(), 2);
        assert_eq!(topology.topics.len(), 2);
    }

    #[test]
    fn test_load_nonexistent_file() {
        assert!(Topology::load("nonexistent.yaml").is_err());
    }
}
