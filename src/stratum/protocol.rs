//! tenrai-mining-v1 messages, specified in protocol/tenrai-mining-v1.md of the node repository.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

pub const PROTOCOL: &str = "tenrai-mining-v1";
pub const ALPN: &[u8] = b"tenrai-mining/1";
pub const MAX_LINE_BYTES: usize = 16_384;
pub const MAX_WORKER_BYTES: usize = 32;
pub const DEFAULT_WORKER: &str = "default";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JobNotice {
    pub job_id: String,
    pub header: String,
    pub share_target: String,
    pub target: String,
    pub registration_height: u64,
    pub network: String,
    pub clean: bool,
    /// The job's first block ends it for the miner, with transactions or without; older miners
    /// ignore the field.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub single_block: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscribeResult {
    pub protocol: String,
    pub nonce_prefix: String,
    pub pool_address: String,
    pub pow_algorithm_id: u8,
    pub spec_revision: u32,
    #[serde(deserialize_with = "deserialize_chain_id")]
    pub chain_id: String,
}

fn deserialize_chain_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if crate::chain::Hash384::from_hex(&value).is_none_or(|id| id.to_hex() != value) {
        return Err(serde::de::Error::custom("invalid mining chain id"));
    }
    Ok(value)
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClientMessage {
    Subscribe {
        id: Value,
        agent: String,
        tag: String,
        pow_algorithm_id: u8,
        spec_revision: u32,
    },
    Authorize {
        id: Value,
        login: String,
        password: String,
    },
    Submit {
        id: Value,
        job_id: String,
        nonce: String,
    },
    Unknown {
        id: Value,
        method: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ServerMessage {
    Response {
        id: Value,
        outcome: Result<Value, (i64, String)>,
    },
    Notify(JobNotice),
    SetTarget(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    Unsupported,
    StaleJob,
    Duplicate,
    LowDifficulty,
    Unauthorized,
    RateLimited,
    Malformed,
    InvalidAddress,
    InvalidTag,
    Banned,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("malformed {0}")]
    Shape(&'static str),
}

impl RejectReason {
    pub const fn code(self) -> i64 {
        match self {
            Self::Unsupported => 20,
            Self::StaleJob => 21,
            Self::Duplicate => 22,
            Self::LowDifficulty => 23,
            Self::Unauthorized => 24,
            Self::RateLimited => 25,
            Self::Malformed => 26,
            Self::InvalidAddress => 27,
            Self::InvalidTag => 28,
            Self::Banned => 29,
        }
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported request",
            Self::StaleJob => "job not found or stale",
            Self::Duplicate => "duplicate share",
            Self::LowDifficulty => "share above target",
            Self::Unauthorized => "subscribe and authorize first",
            Self::RateLimited => "submission rate limit exceeded",
            Self::Malformed => "malformed share",
            Self::InvalidAddress => "log in as <address>.<worker> with an address of this network",
            Self::InvalidTag => "the miner tag is not allowed",
            Self::Banned => "banned for invalid shares or requests; try again later",
        }
    }

    /// Name used in statistics and metrics.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::StaleJob => "stale",
            Self::Duplicate => "duplicate",
            Self::LowDifficulty => "low_difficulty",
            Self::Unauthorized => "unauthorized",
            Self::RateLimited => "rate_limited",
            Self::Malformed => "malformed",
            Self::InvalidAddress => "invalid_address",
            Self::InvalidTag => "invalid_tag",
            Self::Banned => "banned",
        }
    }
}

impl ClientMessage {
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let value: Value = serde_json::from_str(line)?;
        let id = value.get("id").cloned().unwrap_or(Value::Null);
        if !matches!(id, Value::Null | Value::Number(_) | Value::String(_)) {
            return Err(ProtocolError::Shape("request id"));
        }
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .ok_or(ProtocolError::Shape("request method"))?;
        let params = value
            .get("params")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let text = |index: usize| params.get(index).and_then(Value::as_str).map(str::to_owned);
        Ok(match method {
            "mining.subscribe" => {
                if params.len() != 4 {
                    return Err(ProtocolError::Shape("subscribe parameters"));
                }
                Self::Subscribe {
                    id,
                    agent: text(0).ok_or(ProtocolError::Shape("subscribe agent"))?,
                    tag: text(1).ok_or(ProtocolError::Shape("subscribe tag"))?,
                    pow_algorithm_id: params[2]
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .ok_or(ProtocolError::Shape("subscribe algorithm"))?,
                    spec_revision: params[3]
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or(ProtocolError::Shape("subscribe revision"))?,
                }
            }
            "mining.authorize" => Self::Authorize {
                id,
                login: text(0).ok_or(ProtocolError::Shape("authorize login"))?,
                password: text(1).unwrap_or_default(),
            },
            "mining.submit" => Self::Submit {
                id,
                job_id: text(1).ok_or(ProtocolError::Shape("submit job id"))?,
                nonce: text(2).ok_or(ProtocolError::Shape("submit nonce"))?,
            },
            other => Self::Unknown {
                id,
                method: other.chars().take(64).collect(),
            },
        })
    }
}

impl ServerMessage {
    pub fn reply(id: Value, result: Value) -> Self {
        Self::Response {
            id,
            outcome: Ok(result),
        }
    }

    pub fn reject(id: Value, reason: RejectReason) -> Self {
        Self::Response {
            id,
            outcome: Err((reason.code(), reason.message().to_owned())),
        }
    }

    pub fn to_line(&self) -> String {
        match self {
            Self::Response { id, outcome } => match outcome {
                Ok(result) => json!({"id": id, "result": result, "error": null}),
                Err((code, message)) => {
                    json!({"id": id, "result": null, "error": [code, message, null]})
                }
            },
            Self::Notify(job) => json!({"id": null, "method": "mining.notify", "params": job}),
            Self::SetTarget(target) => {
                json!({"id": null, "method": "mining.set_target", "params": [target]})
            }
        }
        .to_string()
    }
}

/// The wire format of a listener; both carry the same mining messages.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Protocol {
    V1,
    V2,
}

impl Protocol {
    pub const ALL: [Self; 2] = [Self::V1, Self::V2];

    pub const fn name(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::V1 => "V1",
            Self::V2 => "V2",
        }
    }

    pub const fn id(self) -> &'static str {
        match self {
            Self::V1 => PROTOCOL,
            Self::V2 => super::v2::PROTOCOL,
        }
    }

    pub const fn alpn(self) -> &'static [u8] {
        match self {
            Self::V1 => ALPN,
            Self::V2 => super::v2::ALPN,
        }
    }

    pub const fn scheme(self, tls: bool) -> &'static str {
        match (self, tls) {
            (Self::V1, false) => "stratum+tcp",
            (Self::V1, true) => "stratum+tls",
            (Self::V2, _) => "stratum2+tls",
        }
    }
}

/// `<address>` or `<address>.<worker>`; the worker keeps `A-Z a-z 0-9 . _ -` and at most 32 bytes.
pub fn split_login(login: &str) -> (&str, String) {
    let (address, worker) = login.split_once('.').unwrap_or((login, ""));
    (address, worker_name(worker))
}

pub fn worker_name(raw: &str) -> String {
    let name: String = raw
        .chars()
        .take(MAX_WORKER_BYTES)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        DEFAULT_WORKER.to_owned()
    } else {
        name
    }
}

/// `x`, `d=65536` or `x,d=65536`: an optional fixed share difficulty after any password text.
pub fn requested_difficulty(password: &str) -> Option<u64> {
    let option = password.rsplit(',').next().unwrap_or(password);
    let value = option.trim().strip_prefix("d=")?;
    let whole = value.split_once('.').map_or(value, |(whole, _)| whole);
    whole
        .parse::<u64>()
        .ok()
        .filter(|difficulty| *difficulty > 0)
}

/// Miner text for logs and statistics: printable ASCII only.
pub fn printable(text: &str, limit: usize) -> String {
    text.chars()
        .filter(|c| c.is_ascii_graphic() || *c == ' ')
        .take(limit)
        .collect()
}

#[cfg(test)]
#[path = "../tests/protocol.rs"]
mod tests;
