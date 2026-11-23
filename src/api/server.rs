use crate::stratum::admission::{canonical, client_key};
use axum::{Router, extract::ConnectInfo, http::Request};
use hyper::{body::Incoming, server::conn::http1};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use std::{
    collections::HashMap,
    net::IpAddr,
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{net::TcpListener, sync::watch};
use tower::ServiceExt;

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_CONNECTIONS: usize = 4_096;
/// Loopback proxy connections count toward the total limit.
const CONNECTIONS_PER_CLIENT: usize = 32;
/// Close gracefully at the connection lifetime, then enforce `GRACE`.
const LIFETIME: Duration = Duration::from_secs(120);
const GRACE: Duration = Duration::from_secs(10);
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

pub async fn run(listener: TcpListener, app: Router, mut shutdown: watch::Receiver<bool>) {
    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        .keep_alive(true)
        .max_buf_size(MAX_HEADER_BYTES)
        .max_headers(MAX_HEADERS)
        .header_read_timeout(HEADER_TIMEOUT);
    let open = Arc::new(Open::default());
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {biased;
            _ = shutdown.changed() => break,
            Some(_) = connections.join_next(),if !connections.is_empty()=>continue,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::debug!("cannot accept a web connection: {error}");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            },
        };
        let Some(slot) = Open::take(&open, peer.ip()) else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        let service = app
            .clone()
            .map_request(move |mut request: Request<Incoming>| {
                request.extensions_mut().insert(ConnectInfo(peer));
                request
            });
        let connection =
            http.serve_connection(TokioIo::new(stream), TowerToHyperService::new(service));
        let mut stop = shutdown.clone();
        connections.spawn(async move {
            let _slot = slot;
            let mut connection = pin!(connection);
            tokio::select! {
                _ = connection.as_mut() => return,
                () = tokio::time::sleep(LIFETIME) => {}
                _ = stop.changed() => {}
            }
            connection.as_mut().graceful_shutdown();
            let _ = tokio::time::timeout(GRACE, connection).await;
        });
    }
    // Share the shutdown deadline across connections.
    while connections.join_next().await.is_some() {}
}

#[derive(Default)]
struct Open {
    counts: Mutex<(usize, HashMap<IpAddr, usize>)>,
}

struct Slot {
    open: Arc<Open>,
    client: Option<IpAddr>,
}

impl Open {
    fn take(open: &Arc<Self>, ip: IpAddr) -> Option<Slot> {
        let client = (!canonical(ip).is_loopback()).then(|| client_key(ip));
        let mut counts = open.counts();
        let (total, clients) = &mut *counts;
        if *total >= MAX_CONNECTIONS {
            return None;
        }
        if let Some(client) = client {
            let count = clients.entry(client).or_default();
            if *count >= CONNECTIONS_PER_CLIENT {
                return None;
            }
            *count += 1;
        }
        *total += 1;
        Some(Slot {
            open: Arc::clone(open),
            client,
        })
    }

    fn counts(&self) -> MutexGuard<'_, (usize, HashMap<IpAddr, usize>)> {
        self.counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut counts = self.open.counts();
        let (total, clients) = &mut *counts;
        *total = total.saturating_sub(1);
        if let Some(client) = self.client
            && let Some(count) = clients.get_mut(&client)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                clients.remove(&client);
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/server.rs"]
mod tests;
