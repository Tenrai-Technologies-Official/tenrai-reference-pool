//! Pool website, JSON APIs and Prometheus metrics.

mod answers;
mod limit;
mod metrics;
mod public;
mod server;
mod views;

use crate::{
    chain::parse_address,
    config::Mode,
    db::{Reader, read},
    hub::Hub,
    work::Hashers,
};
use answers::Answers;
use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;

const PAGE: &str = include_str!("../../web/index.html");
const SCRIPT: &str = include_str!("../../web/app.js");
const COMMON: &str = include_str!("../../web/common.js");
const QR: &str = include_str!("../../web/qr.js");
const THEME: &str = include_str!("../../web/theme.js");
const STYLE: &str = include_str!("../../web/pool.css");
const FONTS: [(&str, &[u8]); 4] = [
    (
        "/fonts/inter-latin.woff2",
        include_bytes!("../../web/fonts/inter-latin.woff2"),
    ),
    (
        "/fonts/inter-latin-ext.woff2",
        include_bytes!("../../web/fonts/inter-latin-ext.woff2"),
    ),
    (
        "/fonts/jetbrains-mono-latin.woff2",
        include_bytes!("../../web/fonts/jetbrains-mono-latin.woff2"),
    ),
    (
        "/fonts/jetbrains-mono-latin-ext.woff2",
        include_bytes!("../../web/fonts/jetbrains-mono-latin-ext.woff2"),
    ),
];
const LOGO: &[u8] = include_bytes!("../../web/tenrai-logo-white.png");
const LOGO_TECHNOLOGIES: &[u8] = include_bytes!("../../web/tenrai-logo-technologies-white.png");
const ICON: &[u8] = include_bytes!("../../web/favicon.ico");
const MANIFEST: &str = include_str!("../../web/manifest.webmanifest");
const ICON_192: &[u8] = include_bytes!("../../web/tenrai-icon-192.png");
const ICON_512: &[u8] = include_bytes!("../../web/tenrai-icon-512.png");
const TOUCH_ICON: &[u8] = include_bytes!("../../web/tenrai-apple-touch-icon.png");
const POLICY: &str = "default-src 'self'; img-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SUMMARY_TTL: Duration = Duration::from_secs(2);
const LIST_TTL: Duration = Duration::from_secs(2);
const DETAIL_TTL: Duration = Duration::from_secs(30);
const HISTORY_TTL: Duration = Duration::from_secs(60);
const PAGE_SIZE: usize = 25;
const BUILD_BURST: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Api {
    pub hub: Arc<Hub>,
    pub reader: Reader,
    pub hashers: Arc<Hashers>,
    answers: Answers,
    build_time: Arc<Mutex<limit::BuildTime>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Build {
    /// Shared overview and first-page responses.
    Shared,
    /// Responses keyed by request parameters.
    Counted,
}

impl Build {
    const fn page(first: bool) -> Self {
        if first { Self::Shared } else { Self::Counted }
    }
}

#[derive(Deserialize)]
struct ListQuery {
    mode: Option<String>,
    before: Option<i64>,
}

#[derive(Serialize)]
struct Problem<'a> {
    error: &'a str,
}

pub async fn serve(bind: SocketAddr, api: Api) -> std::io::Result<SocketAddr> {
    serve_tracked(bind, api).await.map(|(address, _)| address)
}

pub async fn serve_tracked(
    bind: SocketAddr,
    api: Api,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let metrics = api.hub.config.metrics;
    let limits = Arc::new(limit::Limits::new(api.hub.config.web_rate_limit));
    let shutdown = api.hub.shutdown.clone();
    let mut routes = Router::new()
        .route("/", get(page))
        .route("/blocks", get(page))
        .route("/payments", get(page))
        .route("/connect", get(page))
        .route("/miner/{address}", get(page))
        .route(
            "/app.js",
            get(|| asset("text/javascript; charset=utf-8", SCRIPT.as_bytes())),
        )
        .route(
            "/common.js",
            get(|| asset("text/javascript; charset=utf-8", COMMON.as_bytes())),
        )
        .route(
            "/qr.js",
            get(|| asset("text/javascript; charset=utf-8", QR.as_bytes())),
        )
        .route(
            "/theme.js",
            get(|| asset("text/javascript; charset=utf-8", THEME.as_bytes())),
        )
        .route(
            "/pool.css",
            get(|| asset("text/css; charset=utf-8", STYLE.as_bytes())),
        )
        .route("/tenrai-logo-white.png", get(|| asset("image/png", LOGO)))
        .route(
            "/tenrai-logo-technologies-white.png",
            get(|| asset("image/png", LOGO_TECHNOLOGIES)),
        )
        .route("/favicon.ico", get(|| asset("image/x-icon", ICON)))
        .route(
            "/manifest.webmanifest",
            get(|| asset("application/manifest+json", MANIFEST.as_bytes())),
        )
        .route("/tenrai-icon-192.png", get(|| asset("image/png", ICON_192)))
        .route("/tenrai-icon-512.png", get(|| asset("image/png", ICON_512)))
        .route(
            "/tenrai-apple-touch-icon.png",
            get(|| asset("image/png", TOUCH_ICON)),
        )
        .route("/api/v1/pool", get(pool))
        .route("/api/v1/blocks", get(blocks))
        .route("/api/v1/blocks/{hash}", get(block))
        .route("/api/v1/payments", get(payments))
        .route("/api/v1/miners/{address}", get(miner))
        .route("/api/v1/history", get(history))
        .route("/api/pools", get(public::pools))
        .route("/api/pools/{id}", get(public::pool))
        .route("/api/pools/{id}/blocks", get(public::blocks))
        .route("/api/pools/{id}/miners/{address}", get(public::miner))
        .route("/api/blocks", get(public::all_blocks))
        .route("/api/miners/{address}", get(public::all_miner));
    for (path, body) in FONTS {
        routes = routes.route(path, get(move || font(body)));
    }
    if metrics {
        routes = routes.route("/metrics", get(metrics::serve));
    }
    let app = routes
        .fallback(|| async { problem(StatusCode::NOT_FOUND, "no such route") })
        .with_state(api)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(axum::middleware::from_fn_with_state(limits, limit::guard))
        .layer(axum::middleware::map_response(harden));
    let handle = tokio::spawn(server::run(listener, app, shutdown));
    Ok((address, handle))
}

impl Api {
    pub fn new(hub: Arc<Hub>, reader: Reader, hashers: Arc<Hashers>) -> Self {
        Self {
            hub,
            reader,
            hashers,
            answers: Answers::default(),
            build_time: Arc::new(Mutex::new(limit::BuildTime::new(BUILD_BURST))),
        }
    }

    /// Serve cached data or charge an uncached build against the shared budget.
    async fn cached<F, Fut>(&self, key: String, ttl: Duration, build: Build, make: F) -> Response
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, anyhow::Error>>,
    {
        let budget = (build == Build::Counted).then(|| Arc::clone(&self.build_time));
        let made = || async move {
            let Some(budget) = budget else {
                return make().await;
            };
            let time = || {
                budget
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            };
            if !time().admit(Instant::now()) {
                anyhow::bail!("no build time left");
            }
            let started = Instant::now();
            let made = make().await;
            time().charge(started.elapsed());
            made
        };
        match self.answers.get_or_build(&key, ttl, made).await {
            Ok(Some(bytes)) => json_bytes(bytes),
            Ok(None) => problem(StatusCode::NOT_FOUND, "not found"),
            Err(error) => {
                tracing::debug!("API request failed: {error:#}");
                problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the pool is busy, try again",
                )
            }
        }
    }
}

async fn page() -> Response {
    asset("text/html; charset=utf-8", PAGE.as_bytes()).await
}

async fn asset(content_type: &'static str, body: &'static [u8]) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

async fn font(body: &'static [u8]) -> Response {
    (
        [
            (header::CONTENT_TYPE, "font/woff2"),
            (header::CACHE_CONTROL, "public, max-age=604800"),
        ],
        body,
    )
        .into_response()
}

fn json_bytes(bytes: Bytes) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        bytes,
    )
        .into_response()
}

fn problem(status: StatusCode, message: &str) -> Response {
    let body = serde_json::to_vec(&Problem { error: message }).unwrap_or_default();
    (status, json_bytes(Bytes::from(body))).into_response()
}

fn encode<T: Serialize>(value: &T) -> Result<Option<Bytes>, anyhow::Error> {
    Ok(Some(Bytes::from(serde_json::to_vec(value)?)))
}

async fn pool(State(api): State<Api>) -> Response {
    let builder = api.clone();
    api.cached("pool".into(), SUMMARY_TTL, Build::Shared, || async move {
        encode(&views::pool(&builder).await?)
    })
    .await
}

async fn blocks(
    State(api): State<Api>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return problem(
            StatusCode::BAD_REQUEST,
            "mode is pplns or solo, before a block id",
        );
    };
    let mode = match query.mode.as_deref() {
        None | Some("") => None,
        Some("pplns") => Some(Mode::Pplns),
        Some("solo") => Some(Mode::Solo),
        Some(_) => return problem(StatusCode::BAD_REQUEST, "mode is pplns or solo"),
    };
    let key = format!("blocks:{:?}:{:?}", mode, query.before);
    let builder = api.clone();
    api.cached(
        key,
        LIST_TTL,
        Build::page(query.before.is_none()),
        || async move {
            let before = query.before;
            let rows = builder
                .reader
                .read(move |connection| read::blocks(connection, mode, before, PAGE_SIZE + 1))
                .await?;
            encode(&views::block_page(&builder.hub.blocks, &rows, PAGE_SIZE))
        },
    )
    .await
}

async fn block(State(api): State<Api>, Path(hash): Path<String>) -> Response {
    if hash.len() != 96 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return problem(StatusCode::BAD_REQUEST, "a block hash is 96 hex characters");
    }
    let hash = hash.to_ascii_lowercase();
    let builder = api.clone();
    api.cached(
        format!("block:{hash}"),
        DETAIL_TTL,
        Build::Counted,
        || async move {
            let found = builder.reader.block(&hash).await?;
            match found {
                Some(found) => encode(&views::block_detail(&builder, found).await?),
                None => Ok(None),
            }
        },
    )
    .await
}

async fn payments(
    State(api): State<Api>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return problem(StatusCode::BAD_REQUEST, "before is a payment id");
    };
    let key = format!("payments:{:?}", query.before);
    let builder = api.clone();
    api.cached(
        key,
        LIST_TTL,
        Build::page(query.before.is_none()),
        || async move {
            let before = query.before;
            let rows = builder
                .reader
                .read(move |connection| read::payouts(connection, before, PAGE_SIZE + 1))
                .await?;
            encode(&views::payment_page(&rows, PAGE_SIZE))
        },
    )
    .await
}

async fn miner(State(api): State<Api>, Path(address): Path<String>) -> Response {
    if address.len() > 128 || parse_address(&address, api.hub.network).is_err() {
        return problem(StatusCode::BAD_REQUEST, "not an address of this network");
    }
    let builder = api.clone();
    api.cached(
        format!("miner:{address}"),
        LIST_TTL,
        Build::Counted,
        || async move {
            match views::miner(&builder, &address).await? {
                Some(view) => encode(&view),
                None => Ok(None),
            }
        },
    )
    .await
}

async fn history(State(api): State<Api>) -> Response {
    let builder = api.clone();
    api.cached(
        "history".into(),
        HISTORY_TTL,
        Build::Shared,
        || async move { encode(&views::history(&builder, 0).await?) },
    )
    .await
}

async fn harden(mut response: Response) -> Response {
    let headers = response.headers_mut();
    for (name, value) in [
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "no-referrer"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (header::CONTENT_SECURITY_POLICY, POLICY),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}
