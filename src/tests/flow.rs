//! The whole pool against a fake node and a fake wallet daemon, mined by real TenQ shares.

use crate::{
    chain::{Hash384, Network, domain_hash, parse_address},
    cli,
    config::Config,
    pow::{
        target::{meets, target_for},
        template::{HEADER_BYTES, block_id, preimage},
    },
    test_support::{CHAIN, address, hasher, temp_path},
};
use axum::{
    Json, Router,
    extract::{
        Path, State,
        ws::{Message, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{broadcast, oneshot},
    task::JoinHandle,
};

const POOL: u8 = 1;
const TOKEN: &str = "flow-token";
const NETWORK_DIFFICULTY: u128 = 4;
const LEASED_ROOT: Hash384 = Hash384([8; 48]);
const REWARD: u64 = 1_000_000;
const WAIT: Duration = Duration::from_secs(20);
/// The node announces a cut this long before its API serves it.
const ANNOUNCE_LEAD: Duration = Duration::from_millis(50);
/// More than the pool's minimum gap between two cut hints.
const HINT_SPACING: Duration = Duration::from_millis(1_100);

#[derive(Default)]
struct Chain {
    height: u64,
    announced: u64,
    templates: u64,
    candidates: Vec<Hash384>,
    cuts: HashMap<u64, FinalizedCut>,
    blocks: HashMap<Hash384, Value>,
    lookups: usize,
    listeners: usize,
    subscriptions: usize,
    /// Templates that may carry transactions get one, as while a wallet compounds.
    leased: bool,
    submit_busy: bool,
}

#[derive(Clone)]
struct FinalizedCut {
    registered: Vec<Hash384>,
    executed: Vec<Hash384>,
    rewards: Vec<(Hash384, u64)>,
}

#[derive(Clone)]
struct FakeNode {
    chain: Arc<Mutex<Chain>>,
    frames: broadcast::Sender<String>,
}

#[derive(Default)]
struct Daemon {
    sends: Vec<(String, Vec<(String, String)>)>,
    journal: HashMap<String, String>,
    outputs: u64,
    open_compounds: u64,
    compounds: usize,
    without_largest: bool,
}

type FakeDaemon = Arc<Mutex<Daemon>>;

impl FakeNode {
    fn new(height: u64) -> Self {
        Self {
            chain: Arc::new(Mutex::new(Chain {
                height,
                announced: height,
                ..Chain::default()
            })),
            frames: broadcast::channel(64).0,
        }
    }

    /// Finalizes one cut that pays `rewards`.
    fn cut(&self, rewards: &[(Hash384, u64)]) {
        self.finalize(Vec::new(), rewards.to_vec());
    }

    /// Finalizes one cut that executes `executed` and pays nothing.
    fn execute(&self, executed: &[Hash384]) {
        self.finalize(executed.to_vec(), Vec::new());
    }

    /// Announces the next cut a moment before serving it, as the node does.
    fn finalize(&self, executed: Vec<Hash384>, rewards: Vec<(Hash384, u64)>) {
        let height = {
            let mut chain = self.chain.lock().unwrap();
            chain.announced += 1;
            chain.announced
        };
        let frame =
            json!({"type": "cut_finalized", "data": {"api_version": 1, "cut": {"height": height}}});
        let _ = self.frames.send(frame.to_string());
        let chain = Arc::clone(&self.chain);
        tokio::spawn(async move {
            tokio::time::sleep(ANNOUNCE_LEAD).await;
            let mut chain = chain.lock().unwrap();
            chain.height = chain.height.max(height);
            let registered = chain.candidates.clone();
            chain.cuts.insert(
                height,
                FinalizedCut {
                    registered,
                    executed,
                    rewards,
                },
            );
        });
    }

    /// Finalizes one cut without announcing it, so the pool sees a gap.
    fn skip(&self) {
        let mut chain = self.chain.lock().unwrap();
        chain.announced += 1;
        chain.height = chain.announced;
    }

    fn describe(&self, block: Hash384, view: Value) {
        self.chain.lock().unwrap().blocks.insert(block, view);
    }

    fn lookups(&self) -> usize {
        self.chain.lock().unwrap().lookups
    }

    fn templates(&self) -> u64 {
        self.chain.lock().unwrap().templates
    }

    fn subscriptions(&self) -> usize {
        self.chain.lock().unwrap().subscriptions
    }

    fn candidates(&self) -> Vec<Hash384> {
        self.chain.lock().unwrap().candidates.clone()
    }

    fn listeners(&self) -> usize {
        self.chain.lock().unwrap().listeners
    }
}

async fn serve_router(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

fn node_router(node: FakeNode) -> Router {
    Router::new()
        .route("/v1/node/info", get(info))
        .route("/v1/chain/head", get(settlement_head))
        .route("/v1/node/sync", get(sync))
        .route(
            "/v1/chain/rates",
            get(|| async { Json(json!({"network_hashes_per_second": 1000.0})) }),
        )
        .route(
            "/v1/network/mining",
            get(|| async {
                Json(json!({"work_blocks_per_second": 50, "target_cut_interval_millis": 2500}))
            }),
        )
        .route("/v1/mining/templates", post(template))
        .route("/v1/mining/candidates", post(candidate))
        .route("/v1/events", get(events))
        .route("/v1/blocks/{id}", get(block_view))
        .route("/v1/cuts/{height}", get(cut_view))
        .route(
            "/v1/transactions/manifest",
            get(|| async {
                Json(json!({"transaction_limits": {"max_inputs": 8, "max_outputs": 8}, "max_validity_cuts": 1800}))
            }),
        )
        .route(
            "/v1/transactions/{id}/status",
            get(|| async {
                Json(json!({"lifecycle": "finalized", "finalized_cut": {"height": 7}, "failure": null}))
            }),
        )
        .route("/v1/storage", get(|| async { Json(json!({"body_history_floor": 0})) }))
        .with_state(node)
}

async fn settlement_head(State(node): State<FakeNode>) -> Json<Value> {
    let height = node.chain.lock().unwrap().height;
    Json(json!({"chain_id": CHAIN.to_hex(), "height": height,
        "cut_id": domain_hash(b"FLOW_HEAD", &[&height.to_be_bytes()]).to_hex(),
        "state_root": "11".repeat(48), "validator_set_id": "22".repeat(48),
        "params_hash": "33".repeat(48)}))
}

async fn info(State(node): State<FakeNode>) -> Json<Value> {
    let height = node.chain.lock().unwrap().height;
    Json(
        json!({"network": "development", "chain_id": CHAIN.to_hex(), "node_version": "flow",
        "finalized": {"height": height}}),
    )
}

async fn sync(State(node): State<FakeNode>) -> Json<Value> {
    let height = node.chain.lock().unwrap().height;
    Json(json!({"synced": true, "finalized_height": height, "behind_by": 0, "peers": 1}))
}

async fn template(State(node): State<FakeNode>, Json(request): Json<Value>) -> Json<Value> {
    let payout = request["payout"].as_str().unwrap();
    let lock = parse_address(payout, Network::Development).unwrap();
    let (height, issue, leased) = {
        let mut chain = node.chain.lock().unwrap();
        chain.templates += 1;
        let leased = chain.leased && request["max_transactions"].as_u64().unwrap_or(0) > 0;
        (chain.height, chain.templates, leased)
    };
    let (body, root) = if leased {
        ("0000000001ff", LEASED_ROOT)
    } else {
        ("0000000000", Hash384([5; 48]))
    };
    Json(json!({
        "job_id": domain_hash(b"FLOW", &[&issue.to_be_bytes()]).to_hex(),
        "chain_id": CHAIN.to_hex(),
        "pow_algorithm_id": crate::pow::tenq::ALGORITHM_ID,
        "spec_revision": crate::pow::tenq::SPEC_REVISION,
        "work_ref": Hash384([3; 48]).to_hex(),
        "target": hex::encode(target_for(NETWORK_DIFFICULTY)),
        "registration_height": height + 1,
        "registration_window_end": height + 96,
        "parents_root": Hash384([4; 48]).to_hex(),
        "payload_root": root.to_hex(),
        "payload_length": body.len() / 2,
        "payload_mass": 7,
        "reward_lock": hex::encode(lock),
        "body_hex": body,
    }))
}

async fn block_view(State(node): State<FakeNode>, Path(id): Path<String>) -> Response {
    let view = {
        let mut chain = node.chain.lock().unwrap();
        chain.lookups += 1;
        Hash384::from_hex(&id).and_then(|id| chain.blocks.get(&id).cloned())
    };
    view.map_or_else(
        || (StatusCode::NOT_FOUND, Json(json!({"code":"not_found","reason":"history_not_held","message":"unknown or outside retained history"}))).into_response(),
        |view| Json(view).into_response(),
    )
}

async fn cut_view(State(node): State<FakeNode>, Path(height): Path<u64>) -> Json<Value> {
    let cut = node.chain.lock().unwrap().cuts.get(&height).cloned();
    let Some(cut) = cut else {
        return Json(json!({"registered_blocks": [], "rewards": []}));
    };
    let registered: Vec<String> = cut.registered.iter().map(Hash384::to_hex).collect();
    let executed: Vec<String> = cut.executed.iter().map(Hash384::to_hex).collect();
    let rewards: Vec<Value> = cut
        .rewards
        .iter()
        .map(|(id, amount)| {
            json!({"block_id": id.to_hex(), "transaction_id": "ee".repeat(48), "output_index": 0, "amount": amount.to_string()})
        })
        .collect();
    Json(json!({"registered_blocks": registered, "executed_blocks": executed, "rewards": rewards}))
}

async fn candidate(State(node): State<FakeNode>, Json(request): Json<Value>) -> Response {
    if node.chain.lock().unwrap().submit_busy {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"code":"unavailable","message":"fixture submission paused"})),
        )
            .into_response();
    }
    let header: [u8; HEADER_BYTES] = hex::decode(request["header_hex"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let id = block_id(&header);
    node.chain.lock().unwrap().candidates.push(id);
    Json(json!({"block_id": id.to_hex(), "status": "accepted"})).into_response()
}

async fn events(State(node): State<FakeNode>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .protocols(["tenrai.node-events.v1"])
        .on_upgrade(move |mut socket| async move {
            let mut frames = node.frames.subscribe();
            let height = {
                let mut chain = node.chain.lock().unwrap();
                chain.listeners += 1;
                chain.subscriptions += 1;
                chain.height
            };
            let checkpoint = json!({"type": "checkpoint", "data": {"api_version": 1, "finalized": {"height": height}}});
            if socket
                .send(Message::Text(checkpoint.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            while let Ok(frame) = frames.recv().await {
                if socket.send(Message::Text(frame.into())).await.is_err() {
                    break;
                }
            }
            node.chain.lock().unwrap().listeners -= 1;
        })
}

async fn rpc(
    State(daemon): State<FakeDaemon>,
    headers: HeaderMap,
    Json(call): Json<Value>,
) -> Response {
    let authorized = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(&format!("Bearer {TOKEN}"));
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let params = &call["params"];
    let result = match call["method"].as_str().unwrap() {
        "status" => {
            let daemon = daemon.lock().unwrap();
            json!({"network": "development", "chain_id": CHAIN.to_hex(), "live": true, "durable_request_ids": true, "account":0,"request_archive_id":"69".repeat(32),
                "outputs": daemon.outputs.max(2), "pending_sends": daemon.open_compounds,
                "pending_compounds": daemon.open_compounds})
        }
        "receive" => json!({"address": address(POOL)}),
        "balance" => json!({"balance": "50.00000000", "available": "50.00000000", "outputs": 2}),
        "quote_send_many" => {
            assert!(params.get("dry_run").is_none() && params.get("request_id").is_none());
            json!({"unsigned":true,"submitted":false,"fee":"0.00001000"})
        }
        "send_many" => {
            let request = params["request_id"].as_str().unwrap().to_owned();
            let mut daemon = daemon.lock().unwrap();
            assert_eq!(params["expected_request_archive_id"], "69".repeat(32));
            assert_eq!(params["expected_account"], 0);
            assert_eq!(params["expected_chain_id"], CHAIN.to_hex());
            if params["recovery_only"] == true && !daemon.journal.contains_key(&request) {
                return Json(json!({"jsonrpc":"2.0","id":call["id"],
                    "error":{"code":-32002,"message":"original request absent"}}))
                .into_response();
            }
            if let Some(transaction) = daemon.journal.get(&request) {
                json!({"transaction": transaction, "fee": "0.00001000", "valid_until": 900, "duplicate": true})
            } else {
                let transaction = domain_hash(b"TX", &[request.as_bytes()]).to_hex();
                let recipients = params["recipients"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|item| {
                        (
                            item["address"].as_str().unwrap().to_owned(),
                            item["amount"].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect();
                daemon.sends.push((request.clone(), recipients));
                daemon.journal.insert(request, transaction.clone());
                json!({"transaction": transaction, "fee": "0.00001000", "valid_until": 900})
            }
        }
        "outputs" if params.get("largest").is_some() && daemon.lock().unwrap().without_largest => {
            let error =
                json!({"code": -32602, "message": "invalid params: unknown field `largest`"});
            return Json(json!({"jsonrpc": "2.0", "id": call["id"], "error": error}))
                .into_response();
        }
        "outputs" => {
            // As the daemon answers: with `largest`, the largest outputs no pending send holds.
            let held = [
                ("50.00000000", false),
                ("9.00000000", true),
                ("1.00000000", false),
            ];
            let largest = params.get("largest").and_then(Value::as_u64);
            let outputs: Vec<Value> = held
                .iter()
                .filter(|(_, reserved)| largest.is_none() || !reserved)
                .take(largest.map_or(usize::MAX, |count| count as usize))
                .map(|(amount, reserved)| json!({"amount": amount, "reserved": reserved}))
                .collect();
            json!({"outputs": outputs, "count": outputs.len(), "complete": true,
                "snapshot_complete": true, "next": null})
        }
        "compound" => {
            daemon.lock().unwrap().compounds += 1;
            json!({"transactions": []})
        }
        other => panic!("unexpected wallet call {other}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": call["id"], "result": result})).into_response()
}

struct Client {
    lines: Lines<BufReader<OwnedReadHalf>>,
    writer: OwnedWriteHalf,
    next_id: u64,
    prefix: Vec<u8>,
    job: Option<Value>,
}

impl Client {
    async fn connect(port: u16) -> Self {
        let started = Instant::now();
        let socket = loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(socket) => break socket,
                Err(_) if started.elapsed() < WAIT => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(error) => panic!("the pool does not listen on {port}: {error}"),
            }
        };
        let (read, writer) = socket.into_split();
        Self {
            lines: BufReader::new(read).lines(),
            writer,
            next_id: 1,
            prefix: Vec::new(),
            job: None,
        }
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({"id": id, "method": method, "params": params}).to_string() + "\n";
        self.writer.write_all(line.as_bytes()).await.unwrap();
        loop {
            let message = self.read().await;
            if message["id"] == json!(id) {
                return message;
            }
        }
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(WAIT, self.lines.next_line())
            .await
            .expect("the pool answers")
            .unwrap()
            .expect("the pool keeps the connection");
        let message: Value = serde_json::from_str(&line).unwrap();
        if message["method"] == "mining.notify" {
            self.job = Some(message["params"].clone());
        }
        message
    }

    async fn login(&mut self, login: &str) -> Value {
        let subscribed = self
            .call(
                "mining.subscribe",
                json!([
                    "flow-miner/1",
                    "flow",
                    crate::pow::tenq::ALGORITHM_ID,
                    crate::pow::tenq::SPEC_REVISION
                ]),
            )
            .await;
        assert_eq!(
            subscribed["result"]["pow_algorithm_id"],
            crate::pow::tenq::ALGORITHM_ID
        );
        assert_eq!(
            subscribed["result"]["spec_revision"],
            crate::pow::tenq::SPEC_REVISION
        );
        assert_eq!(subscribed["result"]["chain_id"], CHAIN.to_hex());
        self.prefix = hex::decode(subscribed["result"]["nonce_prefix"].as_str().unwrap()).unwrap();
        let authorized = self.call("mining.authorize", json!([login, "x"])).await;
        assert_eq!(authorized["result"], true, "{authorized}");
        while self.job.is_none() {
            self.read().await;
        }
        subscribed["result"].clone()
    }

    /// A nonce whose value is a share but not a block.
    fn share_nonce(&self) -> [u8; 32] {
        let hasher = hasher();
        let job = self.job.clone().unwrap();
        let mut header: [u8; HEADER_BYTES] = hex::decode(job["header"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let target: [u8; 32] = hex::decode(job["target"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        (1_000_000..)
            .map(|counter| self.nonce(counter))
            .find(|nonce| {
                header[248..280].copy_from_slice(nonce);
                !meets(&hasher.evaluate(&preimage(&header)).unwrap(), &target)
            })
            .unwrap()
    }

    fn nonce(&self, counter: u64) -> [u8; 32] {
        let mut nonce = [0_u8; 32];
        nonce[..8].copy_from_slice(&counter.to_le_bytes());
        nonce[32 - self.prefix.len()..].copy_from_slice(&self.prefix);
        nonce
    }

    /// Submits shares of the current job until one is a block; returns the block's ID.
    async fn mine_block(&mut self, start: u64) -> Hash384 {
        let hasher = hasher();
        for counter in start.. {
            let job = self.job.clone().unwrap();
            let mut header: [u8; HEADER_BYTES] = hex::decode(job["header"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap();
            let nonce = self.nonce(counter);
            header[248..280].copy_from_slice(&nonce);
            let value = hasher.evaluate(&preimage(&header)).unwrap();
            let target: [u8; 32] = hex::decode(job["target"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap();
            let share: [u8; 32] = hex::decode(job["share_target"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap();
            if !meets(&value, &share) {
                continue;
            }
            let answer = self
                .call(
                    "mining.submit",
                    json!(["rig", job["job_id"], hex::encode(nonce)]),
                )
                .await;
            assert_eq!(answer["result"], true, "{answer}");
            if meets(&value, &target) {
                return block_id(&header);
            }
        }
        unreachable!()
    }
}

struct Rig {
    node: FakeNode,
    daemon: FakeDaemon,
    config: Config,
    database: PathBuf,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn rig() -> Rig {
    rig_with(&[]).await
}

/// The flow rig with more pool options on the command line.
async fn rig_with(extra: &[String]) -> Rig {
    let node = FakeNode::new(5);
    let node_address = serve_router(node_router(node.clone())).await;
    let daemon = FakeDaemon::default();
    let daemon_address = serve_router(
        Router::new()
            .route("/v1/rpc", post(rpc))
            .with_state(Arc::clone(&daemon)),
    )
    .await;
    let token = temp_path("token");
    std::fs::write(&token, TOKEN).unwrap();
    let database = temp_path("flow").with_extension("sqlite");
    let node_url = format!("http://{node_address}");
    let chain = CHAIN.to_hex();
    let args: Vec<String> = [
        "tenrai-pool",
        "--chain-id",
        &chain,
        "--node",
        &node_url,
        "--node-api",
        &node_url,
        "--wallet",
        &format!("http://{daemon_address}"),
        "--wallet-token-file",
        token.to_str().unwrap(),
        "--pplns-listen",
        &format!("127.0.0.1:{}", free_port()),
        "--solo-listen",
        &format!("127.0.0.1:{}", free_port()),
        "--web-bind",
        &format!("127.0.0.1:{}", free_port()),
        "--db",
        database.to_str().unwrap(),
        "--start-difficulty",
        "1",
        "--min-difficulty",
        "1",
        "--pplns-fee",
        "1",
        "--solo-fee",
        "1",
        "--tenq-mode",
        "light",
        "--verify-threads",
        "2",
        "--min-payout",
        "0.00000001",
        "--payout-interval",
        "1",
        "--compound-above",
        "0",
    ]
    .iter()
    .map(|text| (*text).to_owned())
    .chain(extra.iter().cloned())
    .collect();
    let mut config =
        Config::from_cli(&cli::parse(args.into_iter().map(Into::into).collect()).unwrap()).unwrap();
    config.payouts.check_every = Duration::from_millis(500);
    config.payouts.interval = Duration::from_millis(500);
    config.web_rate_limit = 0;
    let _ = std::fs::remove_file(token);
    Rig {
        node,
        daemon,
        config,
        database,
    }
}

fn start(config: &Config) -> (oneshot::Sender<()>, JoinHandle<anyhow::Result<()>>) {
    let (stop, stopped) = oneshot::channel::<()>();
    let pool = tokio::spawn(crate::serve(config.clone(), async move {
        let _ = stopped.await;
    }));
    (stop, pool)
}

async fn until<T>(what: &str, mut check: impl AsyncFnMut() -> Option<T>) -> T {
    let started = Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn get_json(url: String) -> Option<Value> {
    let client = crate::tls::http_client(Duration::from_secs(5)).unwrap();
    let response = client.get(url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

async fn miner_view(config: &Config, address: &str) -> Option<Value> {
    get_json(format!(
        "http://{}/api/v1/miners/{address}",
        config.web.unwrap()
    ))
    .await
}

async fn pool_block(config: &Config, block: Hash384) -> Option<Value> {
    let page = get_json(format!("http://{}/api/v1/blocks", config.web.unwrap())).await?;
    page["blocks"]
        .as_array()?
        .iter()
        .find(|item| item["hash"] == block.to_hex())
        .cloned()
}

async fn until_stage(config: &Config, block: Hash384, stage: &str) -> Value {
    until(&format!("the {stage} stage"), async || {
        pool_block(config, block)
            .await
            .filter(|item| item["stage"] == stage)
    })
    .await
}

#[path = "flow_v2.rs"]
mod v2;

#[path = "flow_budget.rs"]
mod budget;

#[path = "flow_public.rs"]
mod public;

#[path = "flow_compound.rs"]
mod compound;

#[path = "flow/cases.rs"]
mod cases;

#[path = "flow/genesis.rs"]
mod genesis;
