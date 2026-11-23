//! The node's finalized-cut stream, resumed by height after every reconnect.

use super::{CutRef, endpoint};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    Connector, connect_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header},
        protocol::WebSocketConfig,
    },
};
use url::Url;

const PROTOCOL: &str = "tenrai.node-events.v1";
const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
const MIN_HINT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub enum ChainEvent {
    /// The node's finalized height when a stream starts.
    Head(u64),
    /// Hint that a finalized cut may be available.
    Cut(u64),
    /// Cuts were missed; open blocks must be looked up.
    Gap,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum Frame {
    Checkpoint(Checkpoint),
    CutFinalized(CutFinalized),
    ResyncRequired(Resync),
}

#[derive(Deserialize)]
struct Checkpoint {
    finalized: CutRef,
}

#[derive(Deserialize)]
struct CutFinalized {
    cut: CutRef,
}

#[derive(Deserialize)]
struct Resync {
    reason: String,
}

enum End {
    Closed,
    HistoryUnavailable,
}

/// Follows finalized cuts. The ledger checks every hint against the node API.
pub async fn follow(node: Url, after: Option<u64>, events: mpsc::Sender<ChainEvent>) {
    let mut last = after;
    let mut delay = Duration::from_secs(1);
    loop {
        let started = last;
        match stream(&node, &mut last, &events).await {
            Ok(End::Closed) => {}
            Ok(End::HistoryUnavailable) => {
                tracing::warn!(
                    "the node no longer replays cuts after height {}; looking up open blocks instead",
                    started.unwrap_or_default()
                );
                last = None;
                if deliver(&events, ChainEvent::Gap).await.is_err() {
                    return;
                }
            }
            Err(error) => tracing::debug!("node event stream: {error}"),
        }
        if events.is_closed() {
            return;
        }
        if last != started {
            delay = Duration::from_secs(1);
        }
        sleep(delay).await;
        delay = delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
    }
}

async fn stream(
    node: &Url,
    last: &mut Option<u64>,
    events: &mpsc::Sender<ChainEvent>,
) -> Result<End, String> {
    let mut url = endpoint(node, "v1/events");
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|()| "the node URL cannot carry a socket".to_owned())?;
    if let Some(after) = last {
        url.set_query(Some(&format!("after_height={after}")));
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(PROTOCOL),
    );
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let connector = Connector::Rustls(Arc::new(
        crate::tls::client_config(Vec::new()).map_err(|error| error.to_string())?,
    ));
    let (mut socket, _) = timeout(
        CONNECT_TIMEOUT,
        connect_async_tls_with_config(request, Some(config), true, Some(connector)),
    )
    .await
    .map_err(|_| "connection timed out".to_owned())?
    .map_err(|error| error.to_string())?;

    let mut expected: Option<u64> = None;
    let mut last_hint = None;
    loop {
        let message = timeout(IDLE_TIMEOUT, socket.next())
            .await
            .map_err(|_| "no frame within the idle timeout".to_owned())?
            .ok_or_else(|| "stream closed".to_owned())?
            .map_err(|error| error.to_string())?;
        let text = match message {
            Message::Text(text) => text,
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| error.to_string())?;
                continue;
            }
            Message::Close(_) => return Ok(End::Closed),
            Message::Pong(_) | Message::Frame(_) => continue,
            Message::Binary(_) => return Err("unexpected binary frame".into()),
        };
        let frame: Frame =
            serde_json::from_str(&text).map_err(|error| format!("malformed event: {error}"))?;
        match frame {
            Frame::Checkpoint(checkpoint) => {
                let head = checkpoint.finalized.height;
                expected = Some(last.unwrap_or(head) + 1);
                if last.is_none() {
                    *last = Some(head);
                }
                deliver(events, ChainEvent::Head(head)).await?;
            }
            Frame::CutFinalized(cut) => {
                let next = expected.ok_or("a cut arrived before the checkpoint")?;
                let height = cut.cut.height;
                if height < next {
                    continue;
                }
                if height > next {
                    deliver(events, ChainEvent::Gap).await?;
                } else if last_hint
                    .is_none_or(|last_hint: Instant| last_hint.elapsed() >= MIN_HINT_INTERVAL)
                {
                    deliver(events, ChainEvent::Cut(height)).await?;
                    last_hint = Some(Instant::now());
                }
                expected = Some(height + 1);
                *last = Some(height);
            }
            Frame::ResyncRequired(resync) => {
                if resync.reason == "history_unavailable" {
                    return Ok(End::HistoryUnavailable);
                }
                return Err(format!(
                    "the node asked to resynchronize: {}",
                    resync.reason
                ));
            }
        }
    }
}

async fn deliver(events: &mpsc::Sender<ChainEvent>, event: ChainEvent) -> Result<(), String> {
    match events.try_send(event) {
        Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => Err("the pool stopped".to_owned()),
    }
}
