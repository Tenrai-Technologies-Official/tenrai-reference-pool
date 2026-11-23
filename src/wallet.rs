//! Pool client for the Tenrai wallet daemon.

use crate::{
    chain::{Hash384, Network},
    config::{format_trai, trai_atoms},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use url::Url;

const OUTCOME_UNKNOWN: i64 = -32002;
const FAILED: i64 = -32000;
// Bound daemon replies before parsing JSON.
const MAX_WALLET_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

mod budget;
mod quote;
mod work_rewards;
pub use budget::NeverSignedBudget;
pub(crate) use work_rewards::WorkReward;

#[derive(Clone)]
pub struct Wallet {
    http: reqwest::Client,
    url: Url,
    token: String,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Error)]
pub enum WalletError {
    #[error(transparent)]
    NeverSigned(#[from] NeverSignedBudget),
    #[error("the wallet daemon is unreachable: {0}")]
    Unreachable(String),
    #[error("the wallet daemon answered {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("the wallet daemon sent an unreadable answer: {0}")]
    Answer(String),
}

#[derive(Debug, Error)]
pub enum CheckError {
    #[error(transparent)]
    Wallet(#[from] WalletError),
    #[error("the wallet daemon serves the {network} chain {chain_id}, not the node's")]
    OtherChain { network: String, chain_id: String },
    #[error(
        "the wallet daemon does not retain request IDs durably; upgrade the daemon before enabling payouts"
    )]
    DurableRequestsRequired,
}

impl WalletError {
    pub const fn outcome_unknown(&self) -> bool {
        matches!(
            self,
            Self::Rpc {
                code: OUTCOME_UNKNOWN,
                ..
            }
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Status {
    pub network: String,
    pub chain_id: String,
    pub live: bool,
    pub outputs: u64,
    pub pending_sends: u64,
    /// Absent on daemons without durable request IDs.
    #[serde(default)]
    pub durable_request_ids: bool,
    #[serde(default)]
    pub request_archive_id: Option<String>,
    #[serde(default)]
    pub account: Option<u32>,
    /// Older daemons include compounds in the combined pending count.
    #[serde(default)]
    pub pending_compounds: Option<u64>,
}

/// Daemon archive, account and chain binding from fresh status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestScope {
    pub archive_id: String,
    pub account: u32,
    pub chain_id: String,
}

#[derive(Clone, Debug)]
pub struct Balance {
    pub balance: u64,
    pub available: u64,
    pub outputs: u64,
}

#[derive(Clone, Debug)]
pub struct Receipt {
    pub transaction: String,
    pub fee: u64,
    pub valid_until: u64,
    pub duplicate: bool,
}

#[derive(Deserialize)]
struct BalanceDocument {
    balance: String,
    available: String,
    outputs: u64,
}

#[derive(Deserialize)]
struct AddressDocument {
    address: String,
}

#[derive(Deserialize)]
struct ReceiptDocument {
    transaction: String,
    fee: String,
    valid_until: u64,
    #[serde(default)]
    duplicate: bool,
}

impl Wallet {
    pub fn new(base: Url, token: String) -> anyhow::Result<Self> {
        Ok(Self {
            http: crate::tls::http_client(Duration::from_secs(120))?,
            url: crate::node::endpoint(&base, "v1/rpc"),
            token,
            next_id: Arc::new(AtomicU64::new(1)),
        })
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, WalletError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let response = self
            .http
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .send()
            .await
            .map_err(|error| WalletError::Unreachable(error.to_string()))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(WalletError::Rpc {
                code: FAILED,
                message: "the token in --wallet-token-file is not accepted".into(),
            });
        }
        let body = read_reply(response).await?;
        let mut answer: Value = serde_json::from_slice(&body)
            .map_err(|error| WalletError::Answer(error.to_string()))?;
        if let Some(error) = answer.get("error").filter(|error| !error.is_null()) {
            return Err(WalletError::Rpc {
                code: error.get("code").and_then(Value::as_i64).unwrap_or(FAILED),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect(),
            });
        }
        Ok(answer
            .get_mut("result")
            .map(Value::take)
            .unwrap_or(Value::Null))
    }

    async fn call_as<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, WalletError> {
        let value = self.call(method, params).await?;
        serde_json::from_value(value).map_err(|error| WalletError::Answer(error.to_string()))
    }

    pub async fn status(&self) -> Result<Status, WalletError> {
        self.call_as("status", json!({})).await
    }

    /// Read fresh status before committing a payout attempt.
    pub async fn payment_scope(
        &self,
        network: Network,
        chain: Hash384,
    ) -> Result<RequestScope, WalletError> {
        let status = self.status().await?;
        if !status.durable_request_ids
            || status.network != network.name()
            || status.chain_id != chain.to_hex()
        {
            return Err(WalletError::Answer(
                "the daemon does not provide durable requests for the payout chain".into(),
            ));
        }
        let archive_id = status.request_archive_id.ok_or_else(|| {
            WalletError::Answer(
                "the daemon does not expose its permanent request archive identity".into(),
            )
        })?;
        if archive_id.len() != 64
            || !archive_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(WalletError::Answer(
                "invalid request archive identity".into(),
            ));
        }
        let account = status
            .account
            .ok_or_else(|| WalletError::Answer("the daemon did not identify its account".into()))?;
        Ok(RequestScope {
            archive_id,
            account,
            chain_id: status.chain_id,
        })
    }

    /// Verify the chain before using the daemon's receive address.
    pub async fn checked_address(
        &self,
        network: Network,
        chain_id: Hash384,
    ) -> Result<String, CheckError> {
        let status = self.status().await?;
        if status.network != network.name() || Hash384::from_hex(&status.chain_id) != Some(chain_id)
        {
            return Err(CheckError::OtherChain {
                network: status.network,
                chain_id: status.chain_id,
            });
        }
        if !status.live {
            tracing::warn!("the wallet daemon does not follow the node live yet");
        }
        if !status.durable_request_ids {
            return Err(CheckError::DurableRequestsRequired);
        }
        Ok(self.receive_address().await?)
    }

    pub async fn receive_address(&self) -> Result<String, WalletError> {
        let document: AddressDocument = self.call_as("receive", json!({})).await?;
        Ok(document.address)
    }

    pub async fn balance(&self) -> Result<Balance, WalletError> {
        let document: BalanceDocument = self.call_as("balance", json!({})).await?;
        Ok(Balance {
            balance: atoms(&document.balance)?,
            available: atoms(&document.available)?,
            outputs: document.outputs,
        })
    }

    /// Submit a payout using its durable request ID.
    pub async fn send_many(
        &self,
        recipients: &[(String, u64)],
        fee: &str,
        request_id: &str,
        subtract_fee: bool,
        dry_run: bool,
    ) -> Result<Receipt, WalletError> {
        if !dry_run && !self.status().await?.durable_request_ids {
            return Err(WalletError::Answer(
                "the wallet daemon does not retain request IDs durably".into(),
            ));
        }
        self.send_many_request(
            recipients,
            fee,
            request_id,
            subtract_fee,
            dry_run,
            None,
            None,
        )
        .await
    }

    /// Use the freshly checked scope that was committed for this attempt.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_many_bound(
        &self,
        recipients: &[(String, u64)],
        fee: &str,
        request_id: &str,
        subtract_fee: bool,
        scope: &RequestScope,
        recovery_only: bool,
        maximum_fee: Option<u64>,
    ) -> Result<Receipt, WalletError> {
        self.send_many_request(
            recipients,
            fee,
            request_id,
            subtract_fee,
            false,
            Some((scope, recovery_only)),
            maximum_fee,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_many_request(
        &self,
        recipients: &[(String, u64)],
        fee: &str,
        request_id: &str,
        subtract_fee: bool,
        dry_run: bool,
        binding: Option<(&RequestScope, bool)>,
        maximum_fee: Option<u64>,
    ) -> Result<Receipt, WalletError> {
        let recipients: Vec<Value> = recipients
            .iter()
            .map(|(address, amount)| json!({"address": address, "amount": format_trai(*amount)}))
            .collect();
        let mut params = json!({
            "recipients": recipients,
            "fee": fee,
            "subtract_fee": subtract_fee,
            "dry_run": dry_run,
        });
        if let Some(maximum) = maximum_fee {
            params["max_fee"] = json!(format_trai(maximum));
        }
        if !dry_run {
            params["request_id"] = json!(request_id);
        }
        if let Some((scope, recovery_only)) = binding {
            params["expected_request_archive_id"] = json!(scope.archive_id);
            params["expected_account"] = json!(scope.account);
            params["expected_chain_id"] = json!(scope.chain_id);
            params["recovery_only"] = json!(recovery_only);
        }
        let value = self.call("send_many", params).await?;
        if value.get("outcome").is_some() {
            let (scope, _) = binding.ok_or_else(|| {
                WalletError::Answer("an unbound result cannot authorize budget rejection".into())
            })?;
            return Err(budget::validate(&value, request_id, scope, maximum_fee)?.into());
        }
        let document: ReceiptDocument = serde_json::from_value(value)
            .map_err(|error| WalletError::Answer(error.to_string()))?;
        if Hash384::from_hex(&document.transaction)
            .is_none_or(|transaction| transaction.to_hex() != document.transaction)
        {
            return Err(WalletError::Answer(
                "the payout transaction ID is not canonical".into(),
            ));
        }
        let charged = atoms(&document.fee)?;
        if maximum_fee.is_some_and(|maximum| charged > maximum) {
            return Err(WalletError::Answer(
                "the acknowledged fee exceeds the original signing cap; retain this request".into(),
            ));
        }
        Ok(Receipt {
            transaction: document.transaction,
            fee: charged,
            valid_until: document.valid_until,
            duplicate: document.duplicate,
        })
    }

    /// Sum the largest unreserved outputs, up to `count`.
    pub async fn largest_outputs(&self, count: usize) -> Result<u64, WalletError> {
        let document = self.call("outputs", json!({"largest":count})).await?;
        if document.get("snapshot_complete").and_then(Value::as_bool) != Some(true) {
            return Err(WalletError::Answer(
                "wallet output inventory is incomplete".into(),
            ));
        }
        let mut amounts = Vec::new();
        let outputs = document
            .get("outputs")
            .and_then(Value::as_array)
            .ok_or_else(|| WalletError::Answer("wallet largest output page is missing".into()))?;
        if outputs.len() > count {
            return Err(WalletError::Answer(
                "wallet largest output page exceeds the requested count".into(),
            ));
        }
        for output in outputs {
            if output.get("reserved").and_then(Value::as_bool) == Some(true) {
                continue;
            }
            let amount = output
                .get("amount")
                .and_then(Value::as_str)
                .ok_or_else(|| WalletError::Answer("wallet output amount is missing".into()))?;
            amounts.push(atoms(amount)?);
        }
        amounts.sort_unstable_by(|a, b| b.cmp(a));
        amounts.iter().take(count).try_fold(0_u64, |total, amount| {
            total
                .checked_add(*amount)
                .ok_or_else(|| WalletError::Answer("wallet output amount total overflows".into()))
        })
    }

    /// Compound address outputs and return the number of transactions sent.
    pub async fn compound(&self, fee: &str) -> Result<usize, WalletError> {
        let document = self.call("compound", json!({"fee": fee})).await?;
        Ok(document
            .get("transactions")
            .and_then(Value::as_array)
            .map_or(0, Vec::len))
    }
}

fn atoms(text: &str) -> Result<u64, WalletError> {
    trai_atoms(text).map_err(|error| WalletError::Answer(format!("{text}: {error:#}")))
}

/// Treat a malformed acknowledgement as an unresolved request.
async fn read_reply(mut response: reqwest::Response) -> Result<Vec<u8>, WalletError> {
    let oversized = || {
        WalletError::Answer(format!(
            "wallet response exceeds its {MAX_WALLET_RESPONSE_BYTES}-byte limit"
        ))
    };
    if response
        .content_length()
        .is_some_and(|length| length > MAX_WALLET_RESPONSE_BYTES as u64)
    {
        return Err(oversized());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| WalletError::Answer(error.to_string()))?
    {
        if chunk.len() > MAX_WALLET_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(oversized());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
#[path = "tests/wallet.rs"]
mod tests;
