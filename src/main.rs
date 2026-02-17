use anyhow::Result;
use fast_servicebus_emulator::admin;
use fast_servicebus_emulator::config::Config;
use fast_servicebus_emulator::server::Server;
use std::env;
use tokio::signal;
use tokio::sync::broadcast;
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
    let router = server.router(); // Access router for admin API

    // Create a broadcast channel for shutdown signals
    let (shutdown_tx, _) = broadcast::channel(1);

    // Spawn the signal handler task
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        let _ = shutdown_tx_clone.send(());
    });

    // AMQP Server Task
    let mut rx1 = shutdown_tx.subscribe();
    let amqp_shutdown = async move {
        let _ = rx1.recv().await;
    };
    let amqp_handle = tokio::spawn(async move {
        server.run_with_shutdown(amqp_shutdown).await
    });

    // Admin API Task
    let mut rx2 = shutdown_tx.subscribe();
    let admin_shutdown = async move {
        let _ = rx2.recv().await;
    };
    let admin_router = admin::app(router);
    let admin_port = env::var("ADMIN_PORT").unwrap_or_else(|_| "45672".to_string());
    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;
    info!("Admin API listening on {}", admin_addr);
    
    let admin_handle = tokio::spawn(async move {
        axum::serve(admin_listener, admin_router)
            .with_graceful_shutdown(admin_shutdown)
            .await
            .map_err(anyhow::Error::from)
    });

    // Wait for both servers to finish
    let _ = tokio::try_join!(amqp_handle, admin_handle)?;

    Ok(())
}

async fn wait_for_signal() {
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
