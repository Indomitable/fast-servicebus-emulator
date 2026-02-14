use anyhow::Result;
use fast_servicebus_emulator::server::Server;
use std::env;
use tracing::info;
use tracing_subscriber::EnvFilter;
use fast_servicebus_emulator::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let topology_path =
        env::var("CONFIG_PATH").unwrap_or_else(|_| "config.yaml".to_string());

    let config = Config::load(&topology_path)?;
    info!(
        path = %topology_path,
        config = %config,
        queues = config.topology.queues.len(),
        topics = config.topology.topics.len(),
        "Topology loaded",
    );

    let server = Server::new(config);
    server.run().await
}
