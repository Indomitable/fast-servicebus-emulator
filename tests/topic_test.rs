use anyhow::Result;
use azure_servicebus_emulator::{
    config::{Topology, TopicConfig},
    server::Server,
};
use fe2o3_amqp::connection::Connection;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::Session;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message};
use fe2o3_amqp::types::primitives::Value;
use tokio::time::Duration;

#[tokio::test]
async fn test_topic_fanout() -> Result<()> {
    let topology = Topology {
        queues: vec![],
        topics: vec![TopicConfig {
            name: "events-topic".to_string(),
            subscriptions: vec!["sub-1".to_string(), "sub-2".to_string()],
        }],
    };

    let server = Server::new(topology);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut connection = Connection::builder()
        .container_id("test-client-topic")
        .sasl_profile(SaslProfile::Anonymous)
        .open("amqp://localhost:5672")
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    // Attach receivers first (using SDK-style capitalized "Subscriptions" path)
    let mut receiver1 = Receiver::attach(
        &mut session,
        "test-sub1-receiver",
        "events-topic/Subscriptions/sub-1",
    )
    .await?;
    let mut receiver2 = Receiver::attach(
        &mut session,
        "test-sub2-receiver",
        "events-topic/Subscriptions/sub-2",
    )
    .await?;

    // Attach sender to the topic
    let mut sender = Sender::attach(&mut session, "test-topic-sender", "events-topic").await?;

    // Send a message to the topic
    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String(
            "fanout test".to_string(),
        ))))
        .build();

    sender.send(message).await?;

    // Both receivers should get the message
    let delivery1 =
        tokio::time::timeout(Duration::from_secs(2), receiver1.recv::<Body<Value>>()).await??;
    receiver1.accept(&delivery1).await?;

    let delivery2 =
        tokio::time::timeout(Duration::from_secs(2), receiver2.recv::<Body<Value>>()).await??;
    receiver2.accept(&delivery2).await?;

    // Verify message bodies
    let msg1 = delivery1.message();
    if let Body::Value(AmqpValue(Value::String(s))) = &msg1.body {
        assert_eq!(s, "fanout test");
    } else {
        panic!("Unexpected message body for sub-1: {:?}", msg1.body);
    }

    let msg2 = delivery2.message();
    if let Body::Value(AmqpValue(Value::String(s))) = &msg2.body {
        assert_eq!(s, "fanout test");
    } else {
        panic!("Unexpected message body for sub-2: {:?}", msg2.body);
    }

    session.end().await?;
    connection.close().await?;

    Ok(())
}
