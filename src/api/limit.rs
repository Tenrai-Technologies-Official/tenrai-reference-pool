//! Request, concurrency and uncached build limits.

use crate::{rate::TokenBucket, stratum::admission::client_key};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const MAX_CLIENTS: usize = 65_536;
const MAX_URI_BYTES: usize = 512;
const IN_FLIGHT: usize = 256;
const TOTAL_PER_MINUTE: u64 = 600_000;
const TOTAL_BURST: u32 = 20_000;
const RETRY_SECONDS: &str = "10";

pub struct Limits {
    per_minute: u64,
    burst: u32,
    /// Drop idle buckets after a full refill.
    refill: Duration,
    clients: Mutex<HashMap<IpAddr, (TokenBucket, Instant)>>,
    total: Mutex<TokenBucket>,
    in_flight: Semaphore,
}

impl Limits {
    /// Zero disables the per-client limit.
    pub fn new(per_minute: u64) -> Self {
        let burst = u32::try_from((per_minute / 5).clamp(20, 2_000)).unwrap_or(2_000);
        Self {
            per_minute,
            burst,
            refill: Duration::from_millis(u64::from(burst) * 60_000 / per_minute.max(1)),
            clients: Mutex::new(HashMap::new()),
            total: Mutex::new(
                TokenBucket::per_minute(TOTAL_PER_MINUTE, TOTAL_BURST).expect("positive limits"),
            ),
            in_flight: Semaphore::new(IN_FLIGHT),
        }
    }

    pub fn allow(&self, client: IpAddr, now: Instant) -> bool {
        let total = self.total.lock().is_ok_and(|mut total| total.allow_at(now));
        if !total {
            return false;
        }
        if self.per_minute == 0 {
            return true;
        }
        let Ok(mut clients) = self.clients.lock() else {
            return false;
        };
        if clients.len() >= MAX_CLIENTS && !clients.contains_key(&client) {
            // Reclaim refilled buckets first and bound the table size.
            let refill = self.refill;
            clients.retain(|_, (_, seen)| now.saturating_duration_since(*seen) < refill);
            if clients.len() >= MAX_CLIENTS {
                clients.clear();
            }
        }
        let Some(fresh) = TokenBucket::per_minute(self.per_minute, self.burst) else {
            return false;
        };
        let (bucket, seen) = clients.entry(client).or_insert((fresh, now));
        *seen = now;
        bucket.allow_at(now)
    }
}

/// Shared time budget for uncached responses.
pub struct BuildTime {
    /// Remaining nanoseconds. Negative values carry build debt.
    left: i128,
    burst: i128,
    refilled: Instant,
}

impl BuildTime {
    pub fn new(burst: Duration) -> Self {
        Self {
            left: nanos(burst),
            burst: nanos(burst),
            refilled: Instant::now(),
        }
    }

    pub fn admit(&mut self, now: Instant) -> bool {
        let elapsed = nanos(now.saturating_duration_since(self.refilled));
        self.refilled = self.refilled.max(now);
        self.left = self.left.saturating_add(elapsed).min(self.burst);
        self.left > 0
    }

    /// Charge the build time, with debt capped at one burst.
    pub fn charge(&mut self, took: Duration) {
        self.left = self.left.saturating_sub(nanos(took)).max(-self.burst);
    }
}

fn nanos(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

/// Trust proxy headers from loopback and group IPv6 clients by /64.
pub fn client(peer: SocketAddr, headers: &HeaderMap) -> IpAddr {
    let ip = peer.ip();
    if !ip.is_loopback() {
        return client_key(ip);
    }
    let real = headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .map(str::trim);
    let forwarded = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .map(str::trim);
    real.or(forwarded)
        .and_then(|text| text.parse::<IpAddr>().ok())
        .map_or_else(|| client_key(ip), client_key)
}

pub async fn guard(
    State(limits): State<Arc<Limits>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().to_string().len() > MAX_URI_BYTES {
        return refuse(StatusCode::URI_TOO_LONG, "the address is too long");
    }
    if carries_body(request.headers()) {
        return refuse(StatusCode::PAYLOAD_TOO_LARGE, "requests carry no body");
    }
    if !limits.allow(client(peer, request.headers()), Instant::now()) {
        let mut response = refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "too many requests, try again later",
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static(RETRY_SECONDS));
        return response;
    }
    let Ok(_permit) = limits.in_flight.try_acquire() else {
        return refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "the pool is busy, try again",
        );
    };
    next.run(request).await
}

fn carries_body(headers: &HeaderMap) -> bool {
    headers.contains_key(header::TRANSFER_ENCODING)
        || headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim() != "0")
}

fn refuse(status: StatusCode, message: &'static str) -> Response {
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
#[path = "../tests/limit.rs"]
mod tests;
