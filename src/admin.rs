use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
    routing::delete,
    Router,
};
use chrono::DateTime;
use fe2o3_amqp::types::messaging::{ApplicationProperties, Body, Data, Message, MessageId, Properties};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Timestamp};
use serde_json::json;
use tracing::info;

use crate::router::{PublishResult, RouterMessage, SharedRouter};

#[derive(Clone)]
struct AppState {
    router: SharedRouter,
}

pub fn app(router: SharedRouter) -> Router {
    let state = AppState { router };

    Router::new()
        .route("/testing/messages", delete(delete_all_messages).get(get_all_messages))
        .route(
            "/testing/messages/queues/:queue",
            delete(delete_queue_messages)
                .get(get_queue_messages)
                .post(post_queue_messages),
        )
        .route(
            "/testing/messages/topics/:topic",
            delete(delete_topic_messages)
                .get(get_topic_messages)
                .post(post_topic_messages),
        )
        .route(
            "/testing/messages/topics/:topic/subscriptions/:sub",
            delete(delete_subscription_messages).get(get_subscription_messages),
        )
        .with_state(state)
}

async fn delete_all_messages(State(state): State<AppState>) -> impl IntoResponse {
    let mut total_deleted = 0;
    for (_, store) in state.router.get_all_stores() {
        total_deleted += store.clear().await;
        if let Some(dlq) = store.dlq() {
            total_deleted += dlq.clear().await;
        }
    }
    info!(count = total_deleted, "Deleted all messages");
    (StatusCode::OK, Json(json!({ "deleted": total_deleted })))
}

async fn get_all_messages(State(state): State<AppState>) -> impl IntoResponse {
    let mut all_messages = Vec::new();
    for (name, store) in state.router.get_all_stores() {
        for msg in store.snapshot().await {
            all_messages.push(json!({ "entity": name, "message": msg }));
        }
        if let Some(dlq) = store.dlq() {
            for msg in dlq.snapshot().await {
                all_messages.push(json!({ "entity": name, "dlq": true, "message": msg }));
            }
        }
    }
    Json(all_messages)
}

async fn post_queue_messages(
    State(state): State<AppState>,
    Path(queue): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if state.router.get_queue_store(&queue).is_none() {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "Queue not found" })));
    }

    let message = match build_message(&headers, body) {
        Ok(msg) => msg,
        Err(err) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": err }))),
    };

    match state.router.publish(&queue, message).await {
        PublishResult::Accepted(count) => {
            (StatusCode::OK, Json(json!({ "accepted": count, "target": queue })))
        }
        PublishResult::Full => (
            StatusCode::INSUFFICIENT_STORAGE,
            Json(json!({ "error": "Queue is full" })),
        ),
        PublishResult::UnknownAddress => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Queue not found" })),
        ),
    }
}

async fn delete_queue_messages(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> impl IntoResponse {
    if let Some(store) = state.router.get_queue_store(&queue) {
        let mut count = store.clear().await;
        if let Some(dlq) = store.dlq() {
            count += dlq.clear().await;
        }
        info!(queue = %queue, count = count, "Deleted queue messages");
        (StatusCode::OK, Json(json!({ "deleted": count })))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "Queue not found" })))
    }
}

async fn get_queue_messages(
    State(state): State<AppState>,
    Path(queue): Path<String>,
) -> impl IntoResponse {
    if let Some(store) = state.router.get_queue_store(&queue) {
        let messages = store.snapshot().await;
        let mut response = Vec::new();
        for msg in messages {
            response.push(json!({ "message": msg }));
        }
        if let Some(dlq) = store.dlq() {
            for msg in dlq.snapshot().await {
                response.push(json!({ "dlq": true, "message": msg }));
            }
        }
        (StatusCode::OK, Json(json!(response)))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "Queue not found" })))
    }
}

async fn post_topic_messages(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if state.router.get_topic_subscriptions(&topic).is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Topic not found or no subscriptions" })),
        );
    }

    let message = match build_message(&headers, body) {
        Ok(msg) => msg,
        Err(err) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": err }))),
    };

    match state.router.publish(&topic, message).await {
        PublishResult::Accepted(count) => {
            (StatusCode::OK, Json(json!({ "accepted": count, "target": topic })))
        }
        PublishResult::Full => (
            StatusCode::INSUFFICIENT_STORAGE,
            Json(json!({ "error": "Topic subscription store is full" })),
        ),
        PublishResult::UnknownAddress => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Topic not found" })),
        ),
    }
}

async fn delete_topic_messages(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let stores = state.router.get_topic_subscriptions(&topic);
    if stores.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Topic not found or no subscriptions" })),
        );
    }

    let mut total_deleted = 0;
    for (_, store) in stores {
        total_deleted += store.clear().await;
        if let Some(dlq) = store.dlq() {
            total_deleted += dlq.clear().await;
        }
    }
    info!(topic = %topic, count = total_deleted, "Deleted topic messages");
    (StatusCode::OK, Json(json!({ "deleted": total_deleted })))
}

async fn get_topic_messages(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let stores = state.router.get_topic_subscriptions(&topic);

    let mut all_messages = Vec::new();
    for (sub_name, store) in stores {
        for msg in store.snapshot().await {
            all_messages.push(json!({ "subscription": sub_name, "message": msg }));
        }
        if let Some(dlq) = store.dlq() {
            for msg in dlq.snapshot().await {
                all_messages.push(json!({ "subscription": sub_name, "dlq": true, "message": msg }));
            }
        }
    }
    Json(all_messages)
}

async fn delete_subscription_messages(
    State(state): State<AppState>,
    Path((topic, sub)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Some(store) = state.router.get_subscription_store(&topic, &sub) {
        let mut count = store.clear().await;
        if let Some(dlq) = store.dlq() {
            count += dlq.clear().await;
        }
        info!(topic = %topic, subscription = %sub, count = count, "Deleted subscription messages");
        (StatusCode::OK, Json(json!({ "deleted": count })))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "Subscription not found" })))
    }
}

async fn get_subscription_messages(
    State(state): State<AppState>,
    Path((topic, sub)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Some(store) = state.router.get_subscription_store(&topic, &sub) {
        let mut response = Vec::new();
        for msg in store.snapshot().await {
            response.push(json!({ "message": msg }));
        }
        if let Some(dlq) = store.dlq() {
            for msg in dlq.snapshot().await {
                response.push(json!({ "dlq": true, "message": msg }));
            }
        }
        (StatusCode::OK, Json(json!(response)))
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "Subscription not found" })))
    }
}

fn build_message(headers: &HeaderMap, body: Bytes) -> Result<RouterMessage, String> {
    let mut message = Message::builder().body(Body::from(Data(body.to_vec().into()))).build();

    let mut props = Properties::default();
    let mut has_props = false;

    if let Some(v) = header_value(headers, "x-message-subject") {
        props.subject = Some(v.to_string());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-message-id") {
        props.message_id = Some(MessageId::String(v.to_string()));
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-user-id") {
        props.user_id = Some(v.as_bytes().to_vec().into());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-to") {
        props.to = Some(v.to_string());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-reply-to") {
        props.reply_to = Some(v.to_string());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-correlation-id") {
        props.correlation_id = Some(MessageId::String(v.to_string()));
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-content-type") {
        props.content_type = Some(Symbol::from(v.to_string()));
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-group-id") {
        props.group_id = Some(v.to_string());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-reply-to-group-id") {
        props.reply_to_group_id = Some(v.to_string());
        has_props = true;
    }
    if let Some(v) = header_value(headers, "x-message-absolute-expiry-time") {
        let ts = parse_expiry_timestamp(v)?;
        props.absolute_expiry_time = Some(ts);
        has_props = true;
    }

    if has_props {
        message.properties = Some(props);
    }

    let mut app_props = ApplicationProperties::default();
    for value in headers.get_all("x-message-property") {
        let raw = value
            .to_str()
            .map_err(|_| "invalid header value for X-MESSAGE-PROPERTY".to_string())?;
        let (key, val) = parse_app_property(raw)?;
        app_props.0.insert(key, SimpleValue::String(val));
    }
    if !app_props.0.is_empty() {
        message.application_properties = Some(app_props);
    }

    Ok(message)
}

fn header_value<'a>(headers: &'a HeaderMap, name: &'static str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn parse_expiry_timestamp(value: &str) -> Result<Timestamp, String> {
    if let Ok(ms) = value.parse::<i64>() {
        return Ok(Timestamp::from_milliseconds(ms));
    }

    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| "X-MESSAGE-ABSOLUTE-EXPIRY-TIME must be epoch millis or RFC3339".to_string())?;
    Ok(Timestamp::from_milliseconds(parsed.timestamp_millis()))
}

fn parse_app_property(raw: &str) -> Result<(String, String), String> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| "X-MESSAGE-PROPERTY must be in the form key=value".to_string())?;

    let key = key.trim();
    if key.is_empty() {
        return Err("X-MESSAGE-PROPERTY key cannot be empty".to_string());
    }

    Ok((key.to_string(), value.trim().to_string()))
}
