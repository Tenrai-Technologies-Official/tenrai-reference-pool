pub mod admission;
pub mod jobs;
pub mod protocol;
mod session;
mod submit;
pub mod v2;
pub mod vardiff;
mod web;
mod wire;

pub use web::WebOrigins;

use crate::{config::Mode, hub::Hub};
use admission::{Lease, Refusal};
use protocol::Protocol;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::TlsAcceptor;
use wire::Wire;

const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);
const FULL_WARNING_EVERY: Duration = Duration::from_secs(60);

static FULL_WARNED: Mutex<Option<Instant>> = Mutex::new(None);

#[derive(Clone)]
pub enum Transport {
    Plain,
    Tls(TlsAcceptor),
    Web(WebOrigins),
}

pub async fn serve(
    listener: TcpListener,
    mode: Mode,
    protocol: Protocol,
    transport: Transport,
    hub: Arc<Hub>,
) {
    let handshakes = Arc::new(Semaphore::new(hub.config.max_clients as usize));
    let mut shutdown = hub.shutdown.clone();
    let mut sessions = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {biased;
            _ = shutdown.changed() => break,
            Some(_) = sessions.join_next(),if !sessions.is_empty()=>continue,
            accepted = listener.accept() => accepted,
        };
        let (socket, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::warn!("cannot accept a connection: {error}");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        if socket.set_nodelay(true).is_err() {
            continue;
        }
        if let Transport::Web(origins) = &transport
            && peer.ip().is_loopback()
        {
            let (origins, handshakes, hub) =
                (origins.clone(), Arc::clone(&handshakes), Arc::clone(&hub));
            sessions.spawn(proxied(socket, peer, mode, origins, handshakes, hub));
            continue;
        }
        let Some(lease) = lease(&hub, peer) else {
            continue;
        };
        let (hub, transport) = (Arc::clone(&hub), transport.clone());
        sessions.spawn(async move {
            match transport {
                Transport::Tls(acceptor) => {
                    match crate::tls::accept(&acceptor, socket, protocol.alpn()).await {
                        Ok(stream) => {
                            let wire = Wire::new(stream, protocol);
                            session::run(wire, peer, mode, protocol, lease, hub).await;
                        }
                        Err(error) => tracing::debug!("{peer}: {error:#}"),
                    }
                }
                Transport::Web(origins) => match web::accept(socket, &origins, false).await {
                    Ok((stream, _)) => {
                        session::run(Wire::Web(stream), peer, mode, protocol, lease, hub).await;
                    }
                    Err(error) => tracing::debug!("{peer}: {error:#}"),
                },
                Transport::Plain => {
                    session::run(
                        Wire::new(socket, protocol),
                        peer,
                        mode,
                        protocol,
                        lease,
                        hub,
                    )
                    .await;
                }
            }
        });
    }
    // Allow sessions to send their shutdown message before aborting them.
    let _ = tokio::time::timeout(
        session::GOODBYE_TIMEOUT + Duration::from_millis(500),
        async { while sessions.join_next().await.is_some() {} },
    )
    .await;
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
}

fn lease(hub: &Hub, peer: SocketAddr) -> Option<Lease> {
    match hub.admission.admit(peer.ip()) {
        Ok(lease) => Some(lease),
        Err(refusal) => {
            hub.stats.refused();
            refused(peer, refusal);
            None
        }
    }
}

/// Use the forwarded client address for loopback WebSocket proxy connections.
async fn proxied(
    socket: TcpStream,
    proxy: SocketAddr,
    mode: Mode,
    origins: WebOrigins,
    handshakes: Arc<Semaphore>,
    hub: Arc<Hub>,
) {
    let Ok(permit) = handshakes.try_acquire_owned() else {
        tracing::debug!("{proxy} refused: too many WebSocket handshakes at once");
        return;
    };
    let (stream, forwarded) = match web::accept(socket, &origins, true).await {
        Ok(accepted) => accepted,
        Err(error) => {
            tracing::debug!("{proxy}: {error:#}");
            return;
        }
    };
    drop(permit);
    let peer = SocketAddr::new(forwarded.unwrap_or(proxy.ip()), proxy.port());
    let Some(lease) = lease(&hub, peer) else {
        return;
    };
    session::run(Wire::Web(stream), peer, mode, Protocol::V1, lease, hub).await;
}

fn refused(peer: std::net::SocketAddr, refusal: Refusal) {
    match refusal {
        Refusal::Full => {
            let now = Instant::now();
            if let Ok(mut warned) = FULL_WARNED.lock()
                && warned.is_none_or(|at| now.duration_since(at) >= FULL_WARNING_EVERY)
            {
                *warned = Some(now);
                tracing::warn!("refusing miners: all --max-clients connections are in use");
            }
        }
        Refusal::PerIp => tracing::debug!("{peer} refused: --max-clients-per-ip reached"),
        Refusal::TooFast => tracing::debug!("{peer} refused: reconnecting too fast"),
        Refusal::Banned => tracing::debug!("{peer} refused: banned"),
    }
}
