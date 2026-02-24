use fast_servicebus_emulator::admin;
use fast_servicebus_emulator::config::Config;
use fast_servicebus_emulator::server::Server;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message};
use fe2o3_amqp::types::primitives::Value;
use reqwest::header::HeaderMap;
use tokio::net::TcpListener;

async fn start_admin_server() -> (String, tokio::task::JoinHandle<()>, fast_servicebus_emulator::router::SharedRouter) {
    let config = Config::load("config.yaml").unwrap();
    let server = Server::new(config);
    let router = server.router();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = admin::app(router.clone());
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{}", addr), handle, router)
}

#[tokio::test]
async fn test_admin_api_clear_messages() {
    let (base_url, server_handle, router) = start_admin_server().await;

    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String("test-msg".to_string()))))
        .build();
    let _ = router.publish("input-queue", message).await;

    let store = router.get_queue_store("input-queue").unwrap();
    assert_eq!(store.total_count().await, 1);

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/testing/messages/queues/input-queue", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json.as_array().unwrap().len(), 1);

    let resp = client
        .delete(format!("{}/testing/messages/queues/input-queue", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(store.total_count().await, 0);

    server_handle.abort();
}

#[tokio::test]
async fn test_admin_api_post_queue_with_properties() {
    let (base_url, server_handle, router) = start_admin_server().await;
    let client = reqwest::Client::new();

    let mut headers = HeaderMap::new();
    headers.insert("X-MESSAGE-SUBJECT", "order-created".parse().unwrap());
    headers.insert("X-MESSAGE-MESSAGE-ID", "msg-123".parse().unwrap());
    headers.insert("X-MESSAGE-CORRELATION-ID", "corr-456".parse().unwrap());
    headers.append("X-MESSAGE-PROPERTY", "Region=us-east".parse().unwrap());
    headers.append("X-MESSAGE-PROPERTY", "tenant=contoso".parse().unwrap());
    headers.append(
        "X-MESSAGE-PROPERTY",
        "environment=dev, role=backend".parse().unwrap(),
    );

    let resp = client
        .post(format!("{}/testing/messages/queues/input-queue", base_url))
        .headers(headers)
        .body("hello from rest")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let store = router.get_queue_store("input-queue").unwrap();
    let snapshot = store.snapshot().await;
    assert_eq!(snapshot.len(), 1);

    let message = &snapshot[0].message;
    let props = message.properties.as_ref().unwrap();
    assert_eq!(props.subject.as_deref(), Some("order-created"));

    let app_props = message.application_properties.as_ref().unwrap();
    assert_eq!(app_props.0.get("Region").map(|v| format!("{:?}", v)).unwrap(), "String(\"us-east\")");
    assert_eq!(app_props.0.get("tenant").map(|v| format!("{:?}", v)).unwrap(), "String(\"contoso\")");
    assert_eq!(app_props.0.get("environment").map(|v| format!("{:?}", v)).unwrap(), "String(\"dev\")");
    assert_eq!(app_props.0.get("role").map(|v| format!("{:?}", v)).unwrap(), "String(\"backend\")");

    server_handle.abort();
}

#[tokio::test]
async fn test_admin_api_post_topic_honors_filters() {
    let (base_url, server_handle, router) = start_admin_server().await;
    let client = reqwest::Client::new();

    let mut headers = HeaderMap::new();
    headers.append("X-MESSAGE-PROPERTY", "region=eu-west".parse().unwrap());

    let resp = client
        .post(format!("{}/testing/messages/topics/filter-appprop-topic", base_url))
        .headers(headers)
        .body("filtered message")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // filter-appprop-topic/subscriptions/region-sub expects region=us-east => should get none
    let region_sub = router
        .get_subscription_store("filter-appprop-topic", "region-sub")
        .unwrap();
    assert_eq!(region_sub.total_count().await, 0);

    // catch-all-sub receives all
    let catch_all = router
        .get_subscription_store("filter-appprop-topic", "catch-all-sub")
        .unwrap();
    assert_eq!(catch_all.total_count().await, 1);

    server_handle.abort();
}

#[tokio::test]
async fn test_admin_api_post_queue_property_splitting_with_quoted_commas() {
    let (base_url, server_handle, router) = start_admin_server().await;
    let client = reqwest::Client::new();

    let mut headers = HeaderMap::new();
    headers.append(
        "X-MESSAGE-PROPERTY",
        "payload=\"a,b=c\", plain=ok".parse().unwrap(),
    );
    headers.append(
        "X-MESSAGE-PROPERTY",
        "extra=\"x,y,z\"".parse().unwrap(),
    );

    let resp = client
        .post(format!("{}/testing/messages/queues/input-queue", base_url))
        .headers(headers)
        .body("hello with quoted properties")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let store = router.get_queue_store("input-queue").unwrap();
    let snapshot = store.snapshot().await;
    assert_eq!(snapshot.len(), 1);

    let message = &snapshot[0].message;
    let app_props = message.application_properties.as_ref().unwrap();
    assert_eq!(
        app_props.0.get("payload").map(|v| format!("{:?}", v)).unwrap(),
        "String(\"a,b=c\")"
    );
    assert_eq!(
        app_props.0.get("plain").map(|v| format!("{:?}", v)).unwrap(),
        "String(\"ok\")"
    );
    assert_eq!(
        app_props.0.get("extra").map(|v| format!("{:?}", v)).unwrap(),
        "String(\"x,y,z\")"
    );

    server_handle.abort();
}
