use anyhow::Result;
use azure_servicebus_emulator::{config::Topology, server::Server};
use std::env;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let topology_path =
        env::var("TOPOLOGY_PATH").unwrap_or_else(|_| "topology.yaml".to_string());

    let topology = Topology::load(&topology_path)?;
    info!(
        path = %topology_path,
        queues = topology.queues.len(),
        topics = topology.topics.len(),
        "Topology loaded"
    );

    let server = Server::new(topology);
    server.run().await
}
