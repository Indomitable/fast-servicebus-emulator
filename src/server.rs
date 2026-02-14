//! AMQP 1.0 server that accepts connections and routes messages.
//!
//! Orchestrates the SASL handshake, AMQP connection/session/link lifecycle,
//! CBS authentication, and message routing between clients and queues/topics.

use anyhow::Result;
use fe2o3_amqp::acceptor::error::AcceptorAttachError;
use fe2o3_amqp::acceptor::{ConnectionAcceptor, LinkAcceptor, LinkEndpoint, SessionAcceptor};
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::performatives::Attach;
use fe2o3_amqp_types::definitions::{ReceiverSettleMode, Role};
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::cbs::CbsState;
use crate::config::Config;
use crate::router::{self, SharedRouter, Router};
use crate::sasl::MockSaslAcceptor;
use crate::store::{DlqStore, MessageStore};

/// Pre-resolved store for outgoing sender links.
/// Resolved before spawning the link handler task to avoid race conditions.
enum PreStore {
    /// Regular message store (queue or subscription).
    Regular(Arc<MessageStore>),
    /// Dead-letter queue store.
    Dlq(Arc<DlqStore>),
}

/// The Azure Service Bus emulator server.
pub struct Server {
    router: SharedRouter,
}

impl Server {
    /// Creates a new server from the given topology configuration.
    pub fn new(config: Config) -> Self {
        Self {
            router: Arc::new(Router::from_topology(&config.topology)),
        }
    }

    /// Starts listening for AMQP connections on `0.0.0.0:5672`.
    pub async fn run(&self) -> Result<()> {
        self.run_on("0.0.0.0:5672").await
    }

    /// Starts listening for AMQP connections on the given address.
    pub async fn run_on(&self, addr: &str) -> Result<()> {
        self.run_on_with_shutdown(addr, std::future::pending()).await
    }

    /// Starts listening for AMQP connections with a graceful shutdown signal.
    pub async fn run_with_shutdown<F>(&self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.run_on_with_shutdown("0.0.0.0:5672", shutdown).await
    }

    /// Internal runner with address and shutdown support.
    async fn run_on_with_shutdown<F>(&self, addr: &str, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}", addr);

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, addr) = accept_result?;
                    debug!(peer = %addr, "Connection accepted");

                    let router = self.router.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, router).await {
                            debug!(peer = %addr, error = ?e, "Connection ended");
                        }
                    });
                }
                _ = &mut shutdown => {
                    info!("Shutdown signal received, stopping listener");
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Handles a single TCP connection: SASL handshake, AMQP connection, and sessions.
async fn handle_connection(stream: tokio::net::TcpStream, router: SharedRouter) -> Result<()> {
    let acceptor = ConnectionAcceptor::builder()
        .container_id("fast-servicebus-emulator")
        .max_frame_size(1024 * 1024u32)
        .sasl_acceptor(MockSaslAcceptor)
        .build();

    let mut connection = acceptor.accept(stream).await?;
    debug!("AMQP connection established");

    // Accept sessions until the connection is closed.
    let session_acceptor = SessionAcceptor::new();
    loop {
        match session_acceptor.accept(&mut connection).await {
            Ok(mut session) => {
                debug!("New session accepted");
                let router = router.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_session(&mut session, router).await {
                        debug!(error = ?e, "Session ended");
                    }
                });
            }
            Err(e) => {
                debug!(error = ?e, "Session accept ended");
                break;
            }
        }
    }

    // Graceful close — peer may have already disconnected
    match connection.close().await {
        Ok(()) => debug!("Connection closed"),
        Err(_) => debug!("Peer already disconnected"),
    }
    Ok(())
}

/// Handles a single AMQP session: accepts links and routes them to the
/// appropriate handler (CBS or queue).
async fn handle_session(
    session: &mut SessionHandle<tokio::sync::mpsc::Receiver<Attach>>,
    router: SharedRouter,
) -> Result<()> {
    let link_acceptor = LinkAcceptor::builder()
        .max_message_size(256 * 1024 * 1024u64) // 256 MB
        .build();

    let cbs_state = Arc::new(CbsState::new());

    loop {
        debug!("Waiting for next incoming attach...");
        let remote_attach = match session.next_incoming_attach().await {
            Some(attach) => {
                debug!(
                    link_name = %attach.name,
                    role = ?attach.role,
                    source = ?attach.source.as_ref().and_then(|s| s.address.as_ref()),
                    target = ?attach.target,
                    "Received incoming attach"
                );
                patch_attach_if_needed(attach)
            }
            None => {
                debug!("No more incoming attaches (session closing)");
                break;
            }
        };

        debug!(link_name = %remote_attach.name, "Calling accept_incoming_attach...");

        // Capture the receiver settle mode from the remote attach BEFORE
        // accept_incoming_attach consumes it. For sender links (our side sends
        // to the client), this tells us whether the client wants PeekLock
        // (Second) or ReceiveAndDelete (First).
        let rcv_settle_mode = remote_attach.rcv_settle_mode.clone();

        match link_acceptor
            .accept_incoming_attach(remote_attach, session)
            .await
        {
            Ok(endpoint) => {
                debug!("Link accepted successfully");
                // For outgoing sender links (server → client), get the store
                // NOW, before spawning, to guarantee we don't miss messages.
                let pre_store = if let LinkEndpoint::Sender(ref sender) = endpoint {
                    let addr = sender
                        .source()
                        .as_ref()
                        .and_then(|s| s.address.as_ref())
                        .map(|a| a.to_string())
                        .unwrap_or_default();
                    let normalized = router::normalize_address(&addr);
                    if normalized == "$cbs" {
                        None
                    } else if router.is_dlq_address(normalized) {
                        router.get_dlq_store(normalized).map(PreStore::Dlq)
                    } else {
                        router.get_store(normalized).map(PreStore::Regular)
                    }
                } else {
                    None
                };

                let router = router.clone();
                let cbs_state = cbs_state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_link(endpoint, router, cbs_state, pre_store, rcv_settle_mode).await {
                        debug!(error = ?e, "Link ended");
                    }
                });
            }
            Err(AcceptorAttachError::IllegalSessionState) => {
                // Session engine has stopped (peer sent End, connection closed, etc.)
                // This is a normal condition — break and clean up.
                debug!("IllegalSessionState during accept_incoming_attach — session engine stopped");
                break;
            }
            Err(e) => {
                // Link-level attach error — the session is still alive,
                // so continue accepting other links.
                warn!(error = ?e, "Failed to accept link, continuing session");
            }
        }
    }

    // Explicitly end the session. If the session engine already stopped,
    // this returns an error which we can safely ignore.
    if let Err(e) = session.end().await {
        debug!(error = ?e, "Session end (already ended by peer)");
    }

    Ok(())
}

/// Patches an incoming Attach frame to work around an Azure SDK bug.
///
/// The Azure Service Bus .NET SDK sends Attach frames with `role: Sender` but
/// `initial_delivery_count: None`, violating AMQP 1.0 Section 2.7.4 which
/// requires `initial-delivery-count` for sender role. We patch it to `Some(0)`.
fn patch_attach_if_needed(mut attach: Attach) -> Attach {
    if matches!(attach.role, Role::Sender) && attach.initial_delivery_count.is_none() {
        debug!(
            link_name = %attach.name,
            "Patching Attach: role=Sender with missing initial-delivery-count (Azure SDK bug)"
        );
        attach.initial_delivery_count = Some(0);
    }
    attach
}

/// Routes a link endpoint to the appropriate handler based on its address.
///
/// `pre_store` is an optional pre-resolved store that was looked up before spawning
/// this task, to avoid a race between store lookup and message publication.
///
/// `rcv_settle_mode` is the receiver settle mode from the remote Attach frame.
/// For sender links (our side), `ReceiverSettleMode::Second` means PeekLock.
async fn handle_link(
    endpoint: LinkEndpoint,
    router: SharedRouter,
    cbs_state: Arc<CbsState>,
    pre_store: Option<PreStore>,
    rcv_settle_mode: ReceiverSettleMode,
) -> Result<()> {
    match endpoint {
        LinkEndpoint::Sender(sender) => {
            let addr = sender
                .source()
                .as_ref()
                .and_then(|s| s.address.as_ref())
                .map(|a| a.to_string())
                .unwrap_or_default();

            let normalized = router::normalize_address(&addr);
            if normalized == "$cbs" {
                cbs_state.set_reply_sender(sender).await;
                debug!("CBS reply sender registered");
            } else {
                let store = match pre_store {
                    Some(store) => store,
                    None => {
                        warn!(address = %normalized, "No pre-store for outgoing link");
                        return Ok(());
                    }
                };
                let is_peek_lock = rcv_settle_mode == ReceiverSettleMode::Second;
                debug!(
                    address = %normalized,
                    peek_lock = is_peek_lock,
                    dlq = matches!(store, PreStore::Dlq(_)),
                    "Starting outgoing message handler"
                );
                match store {
                    PreStore::Regular(store) => {
                        if is_peek_lock {
                            router::handle_outgoing_messages_peek_lock(
                                sender,
                                normalized.to_string(),
                                store,
                            )
                            .await?;
                        } else {
                            router::handle_outgoing_messages(sender, normalized.to_string(), store)
                                .await?;
                        }
                    }
                    PreStore::Dlq(store) => {
                        if is_peek_lock {
                            router::handle_outgoing_dlq_messages_peek_lock(
                                sender,
                                normalized.to_string(),
                                store,
                            )
                            .await?;
                        } else {
                            router::handle_outgoing_dlq_messages(
                                sender,
                                normalized.to_string(),
                                store,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        LinkEndpoint::Receiver(receiver) => {
            let addr = receiver
                .target()
                .as_ref()
                .and_then(|t| t.address.as_ref())
                .map(|a| a.to_string())
                .unwrap_or_default();

            let normalized = router::normalize_address(&addr);
            if normalized == "$cbs" {
                crate::cbs::handle_cbs_requests(receiver, &cbs_state).await?;
            } else {
                router::handle_incoming_messages(receiver, normalized.to_string(), &router).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp::types::performatives::Attach;
    use fe2o3_amqp_types::definitions::{Handle, Role};

    fn make_attach(role: Role, initial_delivery_count: Option<u32>) -> Attach {
        Attach {
            name: "test-link".to_string(),
            handle: Handle::from(0u32),
            role,
            snd_settle_mode: Default::default(),
            rcv_settle_mode: Default::default(),
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    #[test]
    fn test_patch_attach_sender_missing_delivery_count() {
        let attach = make_attach(Role::Sender, None);
        let patched = patch_attach_if_needed(attach);
        assert_eq!(patched.initial_delivery_count, Some(0));
    }

    #[test]
    fn test_patch_attach_sender_with_delivery_count() {
        let attach = make_attach(Role::Sender, Some(5));
        let patched = patch_attach_if_needed(attach);
        assert_eq!(patched.initial_delivery_count, Some(5));
    }

    #[test]
    fn test_patch_attach_receiver_no_delivery_count() {
        let attach = make_attach(Role::Receiver, None);
        let patched = patch_attach_if_needed(attach);
        // Receivers don't need initial_delivery_count — should not be patched
        assert_eq!(patched.initial_delivery_count, None);
    }
}
