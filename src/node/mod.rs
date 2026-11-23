mod block_history;
pub mod events;
pub(crate) mod trust;

use crate::{
    chain::Hash384,
    pow::template::{HEADER_BYTES, TemplateResponse, block_id},
};
use reqwest::{Client, Method, StatusCode, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use trust::{HEADER_NONCE, NodeTrust, NodeTrustError, SignedCall};
use url::Url;

const BUSY_RETRIES: u32 = 4;
const DEFAULT_RETRY_MILLIS: u64 = 250;
const MAX_RETRY_MILLIS: u64 = 2_000;
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

/// Client for the node's mining endpoint and public API.
#[derive(Clone)]
pub struct NodeClient {
    http: Client,
    mining: Url,
    api: Url,
    trust: Option<Arc<NodeTrust>>,
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("node is unreachable: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("node answered {status}: {message}")]
    Status { status: StatusCode, message: String },
    #[error("node response authentication failed: {0}")]
    Trust(#[from] NodeTrustError),
    #[error("node stayed busy")]
    Busy,
    #[error("node returned a block ID that does not match the submitted header")]
    BlockId,
    #[error("node sent an unreadable answer: {0}")]
    Answer(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Submission {
    Accepted,
    Known,
    Stale(String),
    Rejected(String),
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct CutRef {
    pub height: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeInfo {
    pub network: String,
    pub chain_id: String,
    pub node_version: String,
    pub finalized: CutRef,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SyncStatus {
    pub synced: bool,
    pub finalized_height: u64,
    pub behind_by: u64,
    pub peers: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChainRates {
    pub network_hashes_per_second: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NetworkMining {
    pub work_blocks_per_second: u32,
    pub target_cut_interval_millis: u32,
    #[serde(default)]
    pub target: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TransactionLimits {
    pub max_inputs: u32,
    pub max_outputs: u32,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TransactionManifest {
    pub transaction_limits: TransactionLimits,
    pub max_validity_cuts: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TransactionStatus {
    pub lifecycle: String,
    pub finalized_cut: Option<CutRef>,
    pub failure: Option<String>,
}

impl TransactionStatus {
    /// Final only once a finalized cut decided it; `dropped` may still finalize (node rule).
    pub fn failed_for_good(&self) -> bool {
        self.lifecycle == "failed"
            && self.finalized_cut.is_some()
            && matches!(self.failure.as_deref(), Some("conflict" | "expired"))
    }
}

#[derive(Deserialize)]
struct StorageDocument {
    body_history_floor: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BlockState {
    pub state: String,
    pub registration_height: Option<u64>,
    pub execution_height: Option<u64>,
    #[serde(default)]
    pub reward: Option<BlockReward>,
}

/// Reward of an executed block, paid by a later cut once its anchor closed.
#[derive(Clone, Debug, Deserialize)]
pub struct BlockReward {
    pub state: String,
    pub fees: String,
    pub subsidy: Option<String>,
    pub payable_from_height: Option<u64>,
}

impl BlockReward {
    /// Atoms paid, `None` while unpaid. A zero claim settles without an output.
    pub fn paid(&self) -> Result<Option<u64>, NodeError> {
        if self.state != "paid" {
            return Ok(None);
        }
        let atoms = |text: &str| {
            text.parse::<u64>()
                .map_err(|_| NodeError::Answer("a block reward is not an atom count".into()))
        };
        let subsidy = self.subsidy.as_deref().map_or(Ok(0), atoms)?;
        atoms(&self.fees)?
            .checked_add(subsidy)
            .map(Some)
            .ok_or_else(|| NodeError::Answer("a block reward overflows".into()))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RewardEntry {
    pub block_id: String,
    pub amount: String,
}

#[derive(Clone, Debug)]
pub struct CutDetail {
    pub registered_blocks: Vec<Hash384>,
    pub executed_blocks: Vec<Hash384>,
    pub rewards: Option<Vec<(Hash384, u64)>>,
}

#[derive(Deserialize)]
struct CutDocument {
    #[serde(default)]
    registered_blocks: Vec<String>,
    #[serde(default)]
    executed_blocks: Vec<String>,
    rewards: Option<Vec<RewardEntry>>,
}

#[derive(Serialize)]
struct TemplateRequest<'a> {
    payout: &'a str,
    max_transactions: usize,
}

#[derive(Serialize)]
struct CandidateRequest<'a> {
    header_hex: String,
    body_hex: &'a str,
}

#[derive(Deserialize)]
struct CandidateResponse {
    block_id: String,
    status: String,
}

#[derive(Deserialize)]
struct ErrorDocument {
    #[serde(default)]
    code: Option<String>,
    message: String,
    retry_after_millis: Option<u64>,
    #[serde(default)]
    reason: Option<String>,
}

struct NodeReply {
    status: StatusCode,
    body: Vec<u8>,
    authenticated_height: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SettlementHead {
    pub(crate) chain_id: String,
    pub(crate) height: u64,
    pub(crate) cut_id: String,
    pub(crate) state_root: String,
    pub(crate) validator_set_id: String,
    pub(crate) params_hash: String,
}

pub(crate) struct StatusObservation {
    pub(crate) status: Option<TransactionStatus>,
    pub(crate) authenticated_height: Option<u64>,
}

impl NodeClient {
    pub fn new(mining: Url, api: Url) -> anyhow::Result<Self> {
        Ok(Self {
            http: crate::tls::http_client(Duration::from_secs(15))?,
            mining,
            api,
            trust: None,
        })
    }

    /// Pins the configured node before any operational response is accepted.
    pub async fn establish_trust(
        &mut self,
        chain_id: Hash384,
        identity: [u8; trust::NODE_IDENTITY_BYTES],
    ) -> Result<(), NodeError> {
        let reply = self
            .request_with_trust(None, Method::GET, &self.api, "v1/node/identity", Vec::new())
            .await?;
        if !reply.status.is_success() {
            return Err(status_error(reply));
        }
        self.trust = Some(Arc::new(NodeTrust::from_identity_document(
            &reply.body,
            chain_id,
            identity,
        )?));
        Ok(())
    }

    pub const fn mining_url(&self) -> &Url {
        &self.mining
    }

    pub const fn api_url(&self) -> &Url {
        &self.api
    }

    pub async fn info(&self) -> Result<NodeInfo, NodeError> {
        self.get(&self.mining, "v1/node/info").await
    }

    pub async fn sync(&self) -> Result<SyncStatus, NodeError> {
        self.get(&self.mining, "v1/node/sync").await
    }

    pub async fn rates(&self) -> Result<ChainRates, NodeError> {
        self.get(&self.mining, "v1/chain/rates").await
    }

    /// Input and output limits per transaction.
    pub async fn transaction_manifest(&self) -> Result<TransactionManifest, NodeError> {
        self.get(&self.api, "v1/transactions/manifest").await
    }

    /// `None` only when the node itself answers that it does not know the transaction.
    pub async fn transaction_status(
        &self,
        id: &str,
    ) -> Result<Option<TransactionStatus>, NodeError> {
        transaction_document(self.transaction_status_reply(id).await?)
    }

    /// A fresh head is a trusted-node observation, not a consensus proof.
    pub(crate) async fn settlement_head(&self) -> Result<SettlementHead, NodeError> {
        let reply = self
            .request(Method::GET, &self.api, "v1/chain/head", Vec::new())
            .await?;
        let authenticated_height = reply.authenticated_height;
        let head: SettlementHead = decode(reply)?;
        let canonical =
            |text: &str| Hash384::from_hex(text).is_some_and(|hash| hash.to_hex() == text);
        if ![
            &head.chain_id,
            &head.cut_id,
            &head.state_root,
            &head.validator_set_id,
            &head.params_hash,
        ]
        .iter()
        .all(|value| canonical(value))
            || authenticated_height.is_some_and(|height| height < head.height)
        {
            return Err(NodeError::Answer(
                "the payout head has invalid or inconsistent context".into(),
            ));
        }
        Ok(head)
    }

    pub(crate) async fn payout_status(&self, id: &str) -> Result<StatusObservation, NodeError> {
        let reply = self.transaction_status_reply(id).await?;
        let authenticated_height = reply.authenticated_height;
        Ok(StatusObservation {
            status: transaction_document(reply)?,
            authenticated_height,
        })
    }

    async fn transaction_status_reply(&self, id: &str) -> Result<NodeReply, NodeError> {
        let id = Hash384::from_hex(id)
            .ok_or_else(|| NodeError::Answer(format!("{id:?} is not a transaction ID")))?;
        self.request(
            Method::GET,
            &self.api,
            &format!("v1/transactions/{}/status", id.to_hex()),
            Vec::new(),
        )
        .await
    }

    /// The lowest cut whose transactions the node still knows.
    pub async fn history_floor(&self) -> Result<u64, NodeError> {
        let storage: StorageDocument = self.get(&self.api, "v1/storage").await?;
        Ok(storage.body_history_floor)
    }

    pub async fn network_mining(&self) -> Result<NetworkMining, NodeError> {
        self.get(&self.api, "v1/network/mining").await
    }

    pub async fn template(
        &self,
        payout: &str,
        max_transactions: usize,
    ) -> Result<TemplateResponse, NodeError> {
        let body = serde_json::to_vec(&TemplateRequest {
            payout,
            max_transactions,
        })
        .map_err(|error| NodeError::Answer(error.to_string()))?;
        decode(
            self.request(Method::POST, &self.mining, "v1/mining/templates", body)
                .await?,
        )
    }

    /// Retries a busy node a few times; callers retry until the work expires.
    pub async fn submit(
        &self,
        header: &[u8; HEADER_BYTES],
        body_hex: &str,
    ) -> Result<Submission, NodeError> {
        let request = CandidateRequest {
            header_hex: hex::encode(header),
            body_hex,
        };
        let body =
            serde_json::to_vec(&request).map_err(|error| NodeError::Answer(error.to_string()))?;
        for _ in 0..BUSY_RETRIES {
            let reply = self
                .request(
                    Method::POST,
                    &self.mining,
                    "v1/mining/candidates",
                    body.clone(),
                )
                .await?;
            if reply.status.is_success() {
                let answer: CandidateResponse = decode(reply)?;
                if Hash384::from_hex(&answer.block_id) != Some(block_id(header)) {
                    return Err(NodeError::BlockId);
                }
                return Ok(match answer.status.as_str() {
                    "accepted" => Submission::Accepted,
                    "known" => Submission::Known,
                    other => Submission::Rejected(format!("unexpected status {other}")),
                });
            }
            let status = reply.status;
            let error = error_document(&reply.body, status);
            match status {
                StatusCode::SERVICE_UNAVAILABLE => {
                    let wait = error
                        .retry_after_millis
                        .unwrap_or(DEFAULT_RETRY_MILLIS)
                        .min(MAX_RETRY_MILLIS);
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                }
                StatusCode::CONFLICT => return Ok(Submission::Stale(error.message)),
                StatusCode::BAD_REQUEST => return Ok(Submission::Rejected(error.message)),
                _ => {
                    return Err(NodeError::Status {
                        status,
                        message: error.message,
                    });
                }
            }
        }
        Err(NodeError::Busy)
    }

    /// Positive retained block facts. Missing history never establishes global absence.
    pub async fn block(&self, id: Hash384) -> Result<BlockState, NodeError> {
        let reply = self
            .request(
                Method::GET,
                &self.api,
                &format!("v1/blocks/{}", id.to_hex()),
                Vec::new(),
            )
            .await?;
        decode(reply)
    }

    /// One finalized cut, including its registered work and retained reward summary.
    pub async fn cut(&self, height: u64) -> Result<CutDetail, NodeError> {
        let cut: CutDocument = self.get(&self.api, &format!("v1/cuts/{height}")).await?;
        let ids = |list: &[String]| {
            list.iter()
                .map(|id| {
                    Hash384::from_hex(id)
                        .ok_or_else(|| NodeError::Answer("a cut names an invalid block ID".into()))
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let registered_blocks = ids(&cut.registered_blocks)?;
        let executed_blocks = ids(&cut.executed_blocks)?;
        let rewards = cut
            .rewards
            .map(|rewards| parse_rewards(&rewards))
            .transpose()?;
        Ok(CutDetail {
            registered_blocks,
            executed_blocks,
            rewards,
        })
    }

    async fn get<T: DeserializeOwned>(&self, base: &Url, path: &str) -> Result<T, NodeError> {
        decode(self.request(Method::GET, base, path, Vec::new()).await?)
    }

    async fn request(
        &self,
        method: Method,
        base: &Url,
        path: &str,
        body: Vec<u8>,
    ) -> Result<NodeReply, NodeError> {
        self.request_with_trust(self.trust.as_deref(), method, base, path, body)
            .await
    }

    async fn request_with_trust(
        &self,
        trust: Option<&NodeTrust>,
        method: Method,
        base: &Url,
        path: &str,
        body: Vec<u8>,
    ) -> Result<NodeReply, NodeError> {
        let url = endpoint(base, path);
        let target = request_target(&url);
        let nonce = trust.map(NodeTrust::fresh_nonce).transpose()?;
        let mut request = self
            .http
            .request(method.clone(), url)
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity");
        if let Some(nonce) = nonce {
            request = request.header(HEADER_NONCE, hex::encode(nonce));
        }
        if method == Method::POST {
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.clone());
        }
        let response = request.send().await?;
        let status = response.status();
        let headers = response.headers().clone();
        let response_body = read_body(response).await?;
        if let (Some(trust), Some(nonce)) = (trust, nonce) {
            trust.verify(SignedCall {
                method: method.as_str(),
                target: &target,
                request_body: &body,
                status: status.as_u16(),
                response_body: &response_body,
                headers: &headers,
                nonce,
            })?;
        }
        let authenticated_height = if trust.is_some() {
            Some(
                headers
                    .get(trust::HEADER_HEIGHT)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(NodeError::Trust(NodeTrustError::Unsigned))?,
            )
        } else {
            None
        };
        Ok(NodeReply {
            status,
            body: response_body,
            authenticated_height,
        })
    }
}

fn parse_rewards(rewards: &[RewardEntry]) -> Result<Vec<(Hash384, u64)>, NodeError> {
    rewards
        .iter()
        .map(|reward| {
            let id = Hash384::from_hex(&reward.block_id)
                .ok_or_else(|| NodeError::Answer("a reward names an invalid block ID".into()))?;
            let amount = reward
                .amount
                .parse::<u64>()
                .map_err(|_| NodeError::Answer("a reward amount is not an atom count".into()))?;
            Ok((id, amount))
        })
        .collect()
}

pub fn endpoint(base: &Url, path: &str) -> Url {
    base.join(path)
        .expect("a relative API path always joins onto an origin")
}

fn request_target(url: &Url) -> String {
    match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    }
}

async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>, NodeError> {
    if response.content_length().unwrap_or(0) > MAX_RESPONSE_BYTES as u64 {
        return Err(NodeError::Answer(format!(
            "the response exceeds {MAX_RESPONSE_BYTES} bytes"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(NodeError::Answer(format!(
                "the response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn decode<T: DeserializeOwned>(reply: NodeReply) -> Result<T, NodeError> {
    if !reply.status.is_success() {
        return Err(status_error(reply));
    }
    serde_json::from_slice(&reply.body).map_err(|error| NodeError::Answer(error.to_string()))
}

fn transaction_document(reply: NodeReply) -> Result<Option<TransactionStatus>, NodeError> {
    if reply.status == StatusCode::NOT_FOUND {
        let failure = error_document(&reply.body, reply.status);
        if failure.code.as_deref() == Some("not_found") && failure.reason.is_none() {
            return Ok(None);
        }
        return Err(status_error(reply));
    }
    decode(reply).map(Some)
}

fn status_error(reply: NodeReply) -> NodeError {
    NodeError::Status {
        status: reply.status,
        message: error_document(&reply.body, reply.status).message,
    }
}

fn error_document(body: &[u8], status: StatusCode) -> ErrorDocument {
    let text = std::str::from_utf8(body).unwrap_or_default();
    serde_json::from_str(text).unwrap_or_else(|_| ErrorDocument {
        code: None,
        message: if text.trim().is_empty() {
            status.to_string()
        } else {
            text.trim().chars().take(200).collect()
        },
        retry_after_millis: None,
        reason: None,
    })
}

#[cfg(test)]
#[path = "../tests/node.rs"]
mod tests;
