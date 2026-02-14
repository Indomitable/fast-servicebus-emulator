use azure_servicebus_emulator::{config::Topology, server::Server};
use fe2o3_amqp::connection::Connection;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::Session;
use fe2o3_amqp::link::{Sender, Receiver};
use fe2o3_amqp::types::messaging::{Body, AmqpValue, ApplicationProperties, Message};
use fe2o3_amqp::types::primitives::Value;
use tokio::time::Duration;
use anyhow::Result;
use azure_servicebus_emulator::config::Config;

#[tokio::test]
async fn test_cbs_handshake() -> Result<()> {
    let config = Config {
        topology: Topology {
            queues: vec![],
            topics: vec![],
        }
    };

    let server = Server::new(config);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut connection = Connection::builder()
        .container_id("test-client")
        .sasl_profile(SaslProfile::Anonymous)
        .open("amqp://localhost:5672")
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    let mut sender = Sender::attach(&mut session, "cbs-sender", "$cbs").await?;
    let mut receiver = Receiver::attach(&mut session, "cbs-receiver", "$cbs").await?;

    // Send put-token request
    let props = ApplicationProperties::builder()
        .insert("operation", "put-token")
        .insert("type", "servicebus.windows.net:sastoken")
        .insert("name", "sb://test.servicebus.windows.net/queue")
        .build();

    let message = Message::builder()
        .application_properties(props)
        .body(Body::Value(AmqpValue(Value::String("mock-token".to_string()))))
        .build();

    sender.send(message).await?;

    // Wait for response
    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
        .await??;

    receiver.accept(&delivery).await?;

    // Verify response has status-code
    let msg = delivery.message();
    let props = msg.application_properties.as_ref().expect("Response should have properties");
    let status_code = props.0.get("status-code").expect("Missing status-code");
    println!("Received Status Code: {:?}", status_code);

    session.end().await?;
    connection.close().await?;

    Ok(())
}
