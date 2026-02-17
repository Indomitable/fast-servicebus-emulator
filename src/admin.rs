use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::delete,
    Router,
};
use serde_json::json;
use tracing::info;

use crate::router::SharedRouter;

#[derive(Clone)]
struct AppState {
    router: SharedRouter,
}

pub fn app(router: SharedRouter) -> Router {
    let state = AppState { router };

    Router::new()
        .route("/testing/messages", delete(delete_all_messages).get(get_all_messages))
        .route("/testing/messages/queues/:queue", delete(delete_queue_messages).get(get_queue_messages))
        .route("/testing/messages/topics/:topic", delete(delete_topic_messages).get(get_topic_messages))
        .route("/testing/messages/topics/:topic/subscriptions/:sub", delete(delete_subscription_messages).get(get_subscription_messages))
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

async fn delete_topic_messages(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let stores = state.router.get_topic_subscriptions(&topic);
    if stores.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "Topic not found or no subscriptions" })));
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
