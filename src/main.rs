use anyhow::Result;
use azure_servicebus_emulator::{config::Topology, server::Server};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let topology = Topology::load("topology.yaml")?;
    info!(
        queues = topology.queues.len(),
        topics = topology.topics.len(),
        "Topology loaded"
    );

    let server = Server::new(topology);
    server.run().await
}
