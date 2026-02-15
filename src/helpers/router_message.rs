use fe2o3_amqp_types::messaging::{Header, MessageAnnotations};
use fe2o3_amqp_types::primitives::{Timestamp, Value};
use tracing::warn;
use crate::config::SubscriptionFilter;
use crate::router::RouterMessage;
use crate::store::LockToken;

pub(crate) fn matches_filters(message: &RouterMessage, filters: &Vec<SubscriptionFilter>) -> bool {
    for filter in filters {
        if !matches_filter(message, filter) {
            return false;
        }
    }
    true
}


/// Checks whether a message matches a subscription filter.
///
/// - `None` filter means accept all messages (equivalent to Azure's TrueFilter).
/// - `Correlation` filter matches on exact equality of system properties
///   (correlation_id, message_id, to, reply_to, subject, content_type) and
///   custom application properties. All specified fields must match.
/// - `Sql` filter is not implemented — logs a warning and accepts all messages.
fn matches_filter(message: &RouterMessage, filter: &SubscriptionFilter) -> bool {
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
pub(crate) fn stamp_broker_properties(
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use fe2o3_amqp_types::messaging::{AmqpValue, Body, Message};
    use fe2o3_amqp_types::messaging::annotations::OwnedKey;
    use fe2o3_amqp_types::primitives::{OrderedMap, Symbol, Value};
    use uuid::Uuid;
    use super::*;

    fn test_message(body: &str) -> RouterMessage {
        Message::builder()
            .body(Body::Value(AmqpValue(Value::String(body.to_string()))))
            .build()
    }
    // ── Correlation filter tests ──────────────────────────────────────

    #[test]
    fn test_matches_filters_none_true() {
        let msg = test_message("anything");
        assert!(matches_filters(&msg, &vec![]));
    }

    #[test]
    fn test_matches_filters_accepts_all() {
        let mut msg = test_message("anything");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);
        let app_properties = fe2o3_amqp_types::messaging::ApplicationProperties::builder()
            .insert("region".to_string(), fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string())).build();
        msg.application_properties = Some(app_properties);

        let filter1 = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: Some("order-created".to_string()),
            content_type: None,
            properties: HashMap::new(),
        };
        let filter2 = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: HashMap::from([
                ("region".to_string(), "us-east".to_string()),
            ]),
        };

        assert!(matches_filters(&msg, &vec![filter1, filter2]));
    }

    #[test]
    fn test_matches_filters_some_false() {
        let mut msg = test_message("anything");
        let app_properties = fe2o3_amqp_types::messaging::ApplicationProperties::builder()
            .insert("region".to_string(), fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()))
            .insert("from".to_string(), fe2o3_amqp_types::primitives::SimpleValue::String("service2".to_string()))
            .build();
        msg.application_properties = Some(app_properties);

        let filter1 = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: HashMap::from([
                ("from".to_string(), "service1".to_string()),
            ]),
        };
        let filter2 = SubscriptionFilter::Correlation {
            correlation_id: None,
            message_id: None,
            to: None,
            reply_to: None,
            subject: None,
            content_type: None,
            properties: HashMap::from([
                ("region".to_string(), "us-east".to_string()),
            ]),
        };

        assert!(!matches_filters(&msg, &vec![filter1, filter2]));
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
        assert!(matches_filter(&msg, &filter));
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
        assert!(!matches_filter(&msg, &filter));
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
        assert!(!matches_filter(&msg, &filter));
    }

    #[test]
    fn test_matches_filter_correlation_id_match() {
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.correlation_id = Some(fe2o3_amqp_types::messaging::MessageId::String(
            "session-42".to_string(),
        ));
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
        assert!(matches_filter(&msg, &filter));
    }

    #[test]
    fn test_matches_filter_correlation_app_properties_match() {
        let mut msg = test_message("hello");
        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(OrderedMap::from_iter(
            vec![
                (
                    "region".to_string(),
                    fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
                ),
                (
                    "priority".to_string(),
                    fe2o3_amqp_types::primitives::SimpleValue::String("high".to_string()),
                ),
            ],
        ));
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
        assert!(matches_filter(&msg, &filter));
    }

    #[test]
    fn test_matches_filter_correlation_app_properties_mismatch() {
        let mut msg = test_message("hello");
        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(OrderedMap::from_iter(
            vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("eu-west".to_string()),
            )],
        ));
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
        assert!(!matches_filter(&msg, &filter));
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
        assert!(!matches_filter(&msg, &filter));
    }

    #[test]
    fn test_matches_filter_correlation_multi_field() {
        // Both subject AND application property must match
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);

        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(OrderedMap::from_iter(
            vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("us-east".to_string()),
            )],
        ));
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
        assert!(matches_filter(&msg, &filter));
    }

    #[test]
    fn test_matches_filter_correlation_multi_field_partial_mismatch() {
        // Subject matches but app property doesn't
        let mut msg = test_message("hello");
        let mut props = fe2o3_amqp_types::messaging::Properties::default();
        props.subject = Some("order-created".to_string());
        msg.properties = Some(props);

        let app_props = fe2o3_amqp_types::messaging::ApplicationProperties(OrderedMap::from_iter(
            vec![(
                "region".to_string(),
                fe2o3_amqp_types::primitives::SimpleValue::String("eu-west".to_string()),
            )],
        ));
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
        assert!(!matches_filter(&msg, &filter));
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
        assert!(matches_filter(&msg, &filter));
    }

    // --- Broker property stamping tests ---

    #[test]
    fn test_stamp_broker_properties_on_empty_message() {
        let mut msg = test_message("hello");
        assert!(msg.message_annotations.is_none());
        assert!(msg.header.is_none());

        stamp_broker_properties(&mut msg, 42, 1_700_000_000_000, 0, None);

        // Check message annotations
        let annotations = msg
            .message_annotations
            .as_ref()
            .expect("annotations should be set");
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        let enq_key = OwnedKey::Symbol(Symbol::from("x-opt-enqueued-time"));

        let seq = annotations.0.get(&seq_key).expect("seq should exist");
        assert_eq!(*seq, Value::Long(42));

        let enq = annotations
            .0
            .get(&enq_key)
            .expect("enqueued time should exist");
        assert_eq!(
            *enq,
            Value::Timestamp(Timestamp::from_milliseconds(1_700_000_000_000))
        );

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
        let custom = annotations
            .0
            .get(&custom_key)
            .expect("custom key should be preserved");
        assert_eq!(*custom, Value::String("custom-value".to_string()));

        // Broker annotations added
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        let enq_key = OwnedKey::Symbol(Symbol::from("x-opt-enqueued-time"));

        let seq = annotations.0.get(&seq_key).unwrap();
        assert_eq!(*seq, Value::Long(7));

        let enq = annotations.0.get(&enq_key).unwrap();
        assert_eq!(
            *enq,
            Value::Timestamp(Timestamp::from_milliseconds(1_600_000_000_000))
        );
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
        use fe2o3_amqp::types::messaging::Header;

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
        let lock_val = annotations
            .0
            .get(&lock_key)
            .expect("lock token should exist");
        let expected_amqp_uuid = fe2o3_amqp_types::primitives::Uuid::from(*token.as_bytes());
        assert_eq!(*lock_val, Value::Uuid(expected_amqp_uuid));

        // Other annotations should still be present
        let seq_key = OwnedKey::Symbol(Symbol::from("x-opt-sequence-number"));
        assert!(annotations.0.get(&seq_key).is_some());
    }

}