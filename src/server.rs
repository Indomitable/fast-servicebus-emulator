//! AMQP 1.0 server that accepts connections and routes messages.
//!
//! Orchestrates the SASL handshake, AMQP connection/session/link lifecycle,
//! CBS authentication, and message routing between clients and queues/topics.

use anyhow::Result;
use fe2o3_amqp::acceptor::{ConnectionAcceptor, LinkAcceptor, LinkEndpoint, SessionAcceptor};
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::performatives::Attach;
use fe2o3_amqp_types::definitions::Role;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::cbs::CbsState;
use crate::config::Topology;
use crate::router::{self, SharedRouter, Router};
use crate::sasl::MockSaslAcceptor;

/// The Azure Service Bus emulator server.
pub struct Server {
    router: SharedRouter,
}

impl Server {
    /// Creates a new server from the given topology configuration.
    pub fn new(topology: Topology) -> Self {
        Self {
            router: Arc::new(Router::from_topology(&topology)),
        }
    }

    /// Starts listening for AMQP connections on `0.0.0.0:5672`.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind("0.0.0.0:5672").await?;
        info!("Listening on 0.0.0.0:5672");

        loop {
            let (stream, addr) = listener.accept().await?;
            debug!(peer = %addr, "Connection accepted");

            let router = self.router.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, router).await {
                    debug!(peer = %addr, error = ?e, "Connection ended");
                }
            });
        }
    }
}

/// Handles a single TCP connection: SASL handshake, AMQP connection, and sessions.
async fn handle_connection(stream: tokio::net::TcpStream, router: SharedRouter) -> Result<()> {
    let acceptor = ConnectionAcceptor::builder()
        .container_id("azure-servicebus-emulator")
        .max_frame_size(1024 * 1024u32)
        .sasl_acceptor(MockSaslAcceptor)
        .build();

    let mut connection = acceptor.accept(stream).await?;
    debug!("AMQP connection established");

    // Accept sessions
    let session_acceptor = SessionAcceptor::new();
    while let Ok(mut session) = session_acceptor.accept(&mut connection).await {
        let router = router.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_session(&mut session, router).await {
                debug!(error = ?e, "Session ended");
            }
        });
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
        let remote_attach = match session.next_incoming_attach().await {
            Some(attach) => patch_attach_if_needed(attach),
            None => break,
        };

        match link_acceptor
            .accept_incoming_attach(remote_attach, session)
            .await
        {
            Ok(endpoint) => {
                let router = router.clone();
                let cbs_state = cbs_state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_link(endpoint, router, cbs_state).await {
                        debug!(error = ?e, "Link ended");
                    }
                });
            }
            Err(e) => {
                error!(error = ?e, "Failed to accept link");
                break;
            }
        }
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
        warn!(
            link_name = %attach.name,
            "Patching Attach: role=Sender with missing initial-delivery-count (Azure SDK bug)"
        );
        attach.initial_delivery_count = Some(0);
    }
    attach
}

/// Routes a link endpoint to the appropriate handler based on its address.
async fn handle_link(
    endpoint: LinkEndpoint,
    router: SharedRouter,
    cbs_state: Arc<CbsState>,
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
                router::handle_outgoing_messages(sender, normalized.to_string(), &router).await?;
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
