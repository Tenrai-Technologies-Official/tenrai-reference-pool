use super::protocol::MAX_LINE_BYTES;
use anyhow::{Result, anyhow};
use std::{net::IpAddr, sync::Mutex, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderName, StatusCode, header},
        protocol::WebSocketConfig,
    },
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");

/// Origins allowed to mine over WebSocket; an empty list allows any origin.
#[derive(Clone, Debug, Default)]
pub struct WebOrigins(Vec<String>);

impl WebOrigins {
    /// Bare origins such as `https://miner.example`.
    pub fn parse(values: &[String]) -> Result<Self> {
        values
            .iter()
            .map(|value| {
                let origin = value.trim().trim_end_matches('/').to_ascii_lowercase();
                let host = origin
                    .strip_prefix("https://")
                    .or_else(|| origin.strip_prefix("http://"))
                    .unwrap_or_default();
                let bare = !host.is_empty()
                    && host
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b".-:[]".contains(&byte));
                if bare {
                    Ok(origin)
                } else {
                    Err(anyhow!("{value} is not a bare http or https origin"))
                }
            })
            .collect::<Result<_>>()
            .map(Self)
    }

    /// A request without `Origin` comes from a program, not a page, and is admitted.
    pub fn admits(&self, origin: Option<&str>) -> bool {
        match origin {
            None => true,
            Some(origin) => {
                self.0.is_empty()
                    || self
                        .0
                        .iter()
                        .any(|allowed| allowed.eq_ignore_ascii_case(origin))
            }
        }
    }
}

/// WebSocket handshake; `X-Real-IP` is read only on `proxied` listeners.
pub(super) async fn accept<S>(
    socket: S,
    origins: &WebOrigins,
    proxied: bool,
) -> Result<(WebSocketStream<S>, Option<IpAddr>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_LINE_BYTES))
        .max_frame_size(Some(MAX_LINE_BYTES));
    let forwarded = Mutex::new(None);
    #[allow(clippy::result_large_err)]
    let check = |request: &Request, response: Response| {
        let headers = request.headers();
        if proxied {
            let address = headers
                .get(REAL_IP)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<IpAddr>().ok());
            if let Ok(mut slot) = forwarded.lock() {
                *slot = address;
            }
        }
        let origin = headers
            .get(header::ORIGIN)
            .map(|value| value.to_str().unwrap_or("?"));
        if origins.admits(origin) {
            return Ok(response);
        }
        let mut refusal = ErrorResponse::new(Some("this page may not mine here".into()));
        *refusal.status_mut() = StatusCode::FORBIDDEN;
        Err(refusal)
    };
    let stream = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async_with_config(socket, check, Some(config)),
    )
    .await
    .map_err(|_| anyhow!("no WebSocket handshake in time"))?
    .map_err(|error| anyhow!("WebSocket handshake failed: {error}"))?;
    let address = forwarded.lock().ok().and_then(|slot| *slot);
    Ok((stream, address))
}

#[cfg(test)]
#[path = "../tests/web.rs"]
mod tests;
