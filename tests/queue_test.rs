use azure_servicebus_emulator::{config::{Topology, QueueConfig}, server::Server};
use fe2o3_amqp::connection::Connection;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::Session;
use fe2o3_amqp::link::{Sender, Receiver};
use fe2o3_amqp::types::messaging::{Body, AmqpValue, Message};
use fe2o3_amqp::types::primitives::Value;
use tokio::time::Duration;
use anyhow::Result;

#[tokio::test]
async fn test_queue_send_receive() -> Result<()> {
    let topology = Topology {
        queues: vec![QueueConfig { name: "input-queue".to_string() }],
        topics: vec![],
    };

    let server = Server::new(topology);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut connection = Connection::builder()
        .container_id("test-client-queue")
        .sasl_profile(SaslProfile::Anonymous)
        .open("amqp://localhost:5672")
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    // Attach receiver first (to ensure it catches the broadcast)
    let mut receiver = Receiver::attach(&mut session, "test-receiver", "input-queue").await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "input-queue").await?;

    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String("hello world".to_string()))))
        .build();

    sender.send(message).await?;

    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
        .await??;

    receiver.accept(&delivery).await?;

    let msg = delivery.message();
    if let Body::Value(AmqpValue(Value::String(s))) = &msg.body {
        assert_eq!(s, "hello world");
    } else {
        panic!("Unexpected message body: {:?}", msg.body);
    }

    session.end().await?;
    connection.close().await?;

    Ok(())
}
