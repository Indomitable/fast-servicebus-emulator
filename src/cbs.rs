//! Mock CBS (Claims-Based Security) handler.
//!
//! Accepts any `put-token` request and responds with status 200 OK.
//! Correlates responses using the request's `message-id` and `reply-to` properties.

use anyhow::Result;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Message, Properties};
use fe2o3_amqp::types::primitives::Value;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Holds the CBS reply sender link for a session.
///
/// The CBS protocol requires two links: a receiver (for `put-token` requests)
/// and a sender (for responses). The sender is registered separately when the
/// client attaches a receiver link to `$cbs`.
pub struct CbsState {
    reply_sender: Mutex<Option<Sender>>,
}

impl Default for CbsState {
    fn default() -> Self {
        Self::new()
    }
}

impl CbsState {
    pub fn new() -> Self {
        Self {
            reply_sender: Mutex::new(None),
        }
    }

    /// Registers the sender link used to deliver CBS responses.
    pub async fn set_reply_sender(&self, sender: Sender) {
        let mut guard = self.reply_sender.lock().await;
        *guard = Some(sender);
    }

    /// Sends a CBS response through the reply sender link.
    async fn send_response(&self, message: Message<Body<Value>>) -> Result<()> {
        let mut guard = self.reply_sender.lock().await;
        if let Some(sender) = guard.as_mut() {
            sender.send(message).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("CBS reply sender not registered"))
        }
    }
}

/// Handles incoming CBS `put-token` requests on the given receiver link.
///
/// For each request:
/// 1. Extracts `message-id` and `reply-to` from properties.
/// 2. Accepts (settles) the delivery.
/// 3. Sends a response with `status-code: 200` and `status-description: OK`.
pub async fn handle_cbs_requests(mut receiver: Receiver, state: &CbsState) -> Result<()> {
    while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
        let request = delivery.message();
        let message_id = request.properties.as_ref().and_then(|p| p.message_id.clone());
        let reply_to = request.properties.as_ref().and_then(|p| p.reply_to.clone());

        debug!(?message_id, ?reply_to, "CBS put-token request received");
        receiver.accept(&delivery).await?;

        let response = build_response(message_id, reply_to);
        match state.send_response(response).await {
            Ok(()) => debug!("CBS 200 OK response sent"),
            Err(e) => warn!("Failed to send CBS response: {:?}", e),
        }
    }

    Ok(())
}

/// Builds a CBS response message with status 200 OK.
fn build_response(
    message_id: Option<fe2o3_amqp::types::messaging::MessageId>,
    reply_to: Option<fe2o3_amqp::types::messaging::Address>,
) -> Message<Body<Value>> {
    let app_props = ApplicationProperties::builder()
        .insert("status-code", 200)
        .insert("status-description", "OK")
        .build();

    let mut builder = Message::builder()
        .application_properties(app_props)
        .body(Body::Value(fe2o3_amqp::types::messaging::AmqpValue(
            Value::Null,
        )));

    // Correlate response to request
    if let Some(msg_id) = message_id {
        let mut props = Properties {
            correlation_id: Some(msg_id),
            ..Default::default()
        };
        if let Some(addr) = reply_to {
            props.to = Some(addr);
        }
        builder = builder.properties(props);
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp::types::messaging::MessageId;
    use fe2o3_amqp::types::primitives::SimpleValue;

    #[test]
    fn test_build_response_with_correlation() {
        let msg_id = MessageId::String("test-123".to_string());
        let response = build_response(Some(msg_id.clone()), None);

        // Verify application properties
        let app_props = response.application_properties.as_ref().unwrap();
        let status = app_props.0.get("status-code").unwrap();
        assert_eq!(*status, SimpleValue::Int(200));

        // Verify correlation
        let props = response.properties.as_ref().unwrap();
        assert_eq!(props.correlation_id, Some(msg_id));
        assert!(props.to.is_none());
    }

    #[test]
    fn test_build_response_with_reply_to() {
        let msg_id = MessageId::String("test-456".to_string());
        let reply_to = fe2o3_amqp::types::messaging::Address::from("$cbs");
        let response = build_response(Some(msg_id), Some(reply_to.clone()));

        let props = response.properties.as_ref().unwrap();
        assert_eq!(props.to, Some(reply_to));
    }

    #[test]
    fn test_build_response_no_message_id() {
        let response = build_response(None, None);

        // Should still have application properties
        let app_props = response.application_properties.as_ref().unwrap();
        assert!(app_props.0.contains_key("status-code"));

        // No properties when no message-id
        assert!(response.properties.is_none());
    }

    #[tokio::test]
    async fn test_cbs_state_new_has_no_sender() {
        let state = CbsState::new();
        let guard = state.reply_sender.lock().await;
        assert!(guard.is_none());
    }
}
