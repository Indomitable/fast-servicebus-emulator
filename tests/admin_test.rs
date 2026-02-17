use fast_servicebus_emulator::config::Config;
use fast_servicebus_emulator::server::Server;
use fast_servicebus_emulator::admin;
use fe2o3_amqp::types::messaging::{Message, Body, AmqpValue};
use fe2o3_amqp::types::primitives::Value;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_admin_api_clear_messages() {
    // Setup
    let config = Config::load("config.yaml").unwrap();
    let server = Server::new(config);
    let router = server.router();

    // Inject a message into "input-queue"
    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String("test-msg".to_string()))))
        .build();
    
    // Direct publish (simulating AMQP receive)
    // We use the public publish method which handles internal routing
    let _ = router.publish("input-queue", message.clone()).await;
    
    // Verify store has 1 message
    let store = router.get_queue_store("input-queue").unwrap();
    assert_eq!(store.total_count().await, 1);

    // Start Admin API on a random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = admin::app(router.clone());
    
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Client
    let client = reqwest::Client::new();
    let base_url = format!("http://{}", addr);

    // GET /testing/messages/queues/input-queue
    let resp = client.get(format!("{}/testing/messages/queues/input-queue", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let messages = json.as_array().unwrap();
    assert_eq!(messages.len(), 1);

    // DELETE /testing/messages/queues/input-queue
    let resp = client.delete(format!("{}/testing/messages/queues/input-queue", base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify empty via Store access
    assert_eq!(store.total_count().await, 0);
    
    // GET should return empty list
    let resp = client.get(format!("{}/testing/messages/queues/input-queue", base_url))
        .send()
        .await
        .unwrap();
    let json: serde_json::Value = resp.json().await.unwrap();
    let messages = json.as_array().unwrap();
    assert_eq!(messages.len(), 0);

    // Cleanup
    server_handle.abort();
}
