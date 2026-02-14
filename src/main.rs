use anyhow::Result;
use fast_servicebus_emulator::config::Config;
use fast_servicebus_emulator::server::Server;
use std::env;
use tokio::signal;
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
    server.run_with_shutdown(shutdown_signal()).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
