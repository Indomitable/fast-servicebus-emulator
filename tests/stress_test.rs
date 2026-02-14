use anyhow::Result;
use azure_servicebus_emulator::{
    config::{QueueConfig, TopicConfig, Topology},
    server::Server,
};
use fe2o3_amqp::connection::Connection;
use fe2o3_amqp::link::{Receiver, Sender};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::Session;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Message};
use fe2o3_amqp::types::primitives::Value;
use tokio::net::TcpListener;
use tokio::time::Duration;

/// Starts a server on a random free port and returns the port number.
async fn start_server(topology: Topology) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let addr = format!("127.0.0.1:{port}");
    let server = Server::new(topology);
    tokio::spawn(async move {
        if let Err(e) = server.run_on(&addr).await {
            eprintln!("Server error: {:?}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    port
}

/// Stress test: 10 sequential connections to the same server.
/// Each iteration opens a connection, does a send/receive, and closes
/// everything cleanly. Verifies that the server handles repeated
/// connection/session/link lifecycles without accumulating bad state.
#[tokio::test]
async fn test_sequential_connections() -> Result<()> {
    let topology = Topology {
        queues: vec![QueueConfig {
            name: "stress-queue".to_string(),
        }],
        topics: vec![TopicConfig {
            name: "stress-topic".to_string(),
            subscriptions: vec!["sub-a".to_string(), "sub-b".to_string()],
        }],
    };

    let port = start_server(topology).await;

    // 10 sequential rounds of connect -> send -> receive -> close
    for round in 0..10 {
        queue_send_receive_cycle(round, port)
            .await
            .unwrap_or_else(|e| panic!("Round {round} failed: {e}"));
    }

    // Finish with a topic fanout cycle to verify topics still work
    topic_fanout_cycle(port).await?;

    Ok(())
}

/// Stress test: concurrent connections each doing independent work on
/// separate queues. Verifies one connection closing doesn't break the other.
#[tokio::test]
async fn test_concurrent_independent_connections() -> Result<()> {
    let topology = Topology {
        queues: vec![
            QueueConfig {
                name: "conc-queue-a".to_string(),
            },
            QueueConfig {
                name: "conc-queue-b".to_string(),
            },
        ],
        topics: vec![],
    };

    let port = start_server(topology).await;

    for round in 0..5 {
        let (r1, r2) = tokio::join!(
            independent_send_receive(round, "conc-queue-a", port),
            independent_send_receive(round, "conc-queue-b", port),
        );
        r1.unwrap_or_else(|e| panic!("Round {round} queue-a failed: {e}"));
        r2.unwrap_or_else(|e| panic!("Round {round} queue-b failed: {e}"));
    }

    Ok(())
}

async fn queue_send_receive_cycle(round: usize, port: u16) -> Result<()> {
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::builder()
        .container_id(format!("stress-seq-{round}"))
        .sasl_profile(SaslProfile::Anonymous)
        .open(url.as_str())
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    let mut receiver =
        Receiver::attach(&mut session, format!("recv-{round}"), "stress-queue").await?;
    let mut sender =
        Sender::attach(&mut session, format!("send-{round}"), "stress-queue").await?;

    let body = format!("seq-round-{round}");
    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String(body.clone()))))
        .build();

    sender.send(message).await?;

    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
        .await??;
    receiver.accept(&delivery).await?;

    let msg = delivery.message();
    if let Body::Value(AmqpValue(Value::String(s))) = &msg.body {
        assert_eq!(s, &body);
    } else {
        panic!("Unexpected body in round {round}");
    }

    session.end().await?;
    connection.close().await?;

    Ok(())
}

async fn independent_send_receive(round: usize, queue: &str, port: u16) -> Result<()> {
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::builder()
        .container_id(format!("conc-{round}-{queue}"))
        .sasl_profile(SaslProfile::Anonymous)
        .open(url.as_str())
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    let mut receiver =
        Receiver::attach(&mut session, format!("recv-{round}-{queue}"), queue).await?;
    let mut sender =
        Sender::attach(&mut session, format!("send-{round}-{queue}"), queue).await?;

    let body = format!("conc-{round}-{queue}");
    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String(body.clone()))))
        .build();

    sender.send(message).await?;

    let delivery = tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>())
        .await??;
    receiver.accept(&delivery).await?;

    let msg = delivery.message();
    if let Body::Value(AmqpValue(Value::String(s))) = &msg.body {
        assert_eq!(s, &body);
    } else {
        panic!("Unexpected body in round {round} queue {queue}");
    }

    session.end().await?;
    connection.close().await?;

    Ok(())
}

async fn topic_fanout_cycle(port: u16) -> Result<()> {
    let url = format!("amqp://127.0.0.1:{port}");
    let mut connection = Connection::builder()
        .container_id("stress-topic-client")
        .sasl_profile(SaslProfile::Anonymous)
        .open(url.as_str())
        .await?;

    let mut session = Session::begin(&mut connection).await?;

    let mut rx_a = Receiver::attach(
        &mut session,
        "stress-sub-a",
        "stress-topic/Subscriptions/sub-a",
    )
    .await?;
    let mut rx_b = Receiver::attach(
        &mut session,
        "stress-sub-b",
        "stress-topic/Subscriptions/sub-b",
    )
    .await?;
    let mut sender =
        Sender::attach(&mut session, "stress-topic-sender", "stress-topic").await?;

    let message = Message::builder()
        .body(Body::Value(AmqpValue(Value::String(
            "stress-fanout".to_string(),
        ))))
        .build();

    sender.send(message).await?;

    let d1 = tokio::time::timeout(Duration::from_secs(2), rx_a.recv::<Body<Value>>()).await??;
    rx_a.accept(&d1).await?;
    let d2 = tokio::time::timeout(Duration::from_secs(2), rx_b.recv::<Body<Value>>()).await??;
    rx_b.accept(&d2).await?;

    if let Body::Value(AmqpValue(Value::String(s))) = &d1.message().body {
        assert_eq!(s, "stress-fanout");
    } else {
        panic!("Unexpected body for sub-a");
    }
    if let Body::Value(AmqpValue(Value::String(s))) = &d2.message().body {
        assert_eq!(s, "stress-fanout");
    } else {
        panic!("Unexpected body for sub-b");
    }

    session.end().await?;
    connection.close().await?;

    Ok(())
}
