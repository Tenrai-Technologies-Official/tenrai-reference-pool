//! The node's work template and the 312-byte TenQ work header built from it.

use super::{PREIMAGE_BYTES, epoch_for, target::difficulty_of, tenq};
use crate::chain::{Hash384, LOCK_BYTES, Lock, domain_hash};
use serde::Deserialize;
use thiserror::Error;

pub const HEADER_BYTES: usize = 312;
const HEADER_VERSION: u16 = 1;
const POW_DOMAIN: &[u8; 11] = b"TRAI_POW_V1";
const BLOCK_ID_DOMAIN: &[u8] = b"TRAI_BLOCK_V1";
const MAX_PARENTS: usize = 16;
const MAX_TRANSACTIONS: u32 = 64;
// Canonical work-body budget shared by the node's mainnet and test profiles.
const MAX_BODY_BYTES: usize = 46 * 1024;

const VERSION_AT: usize = 48;
const ALGORITHM_AT: usize = 50;
const WORK_REF_AT: usize = 51;
const PARENTS_ROOT_AT: usize = 99;
const PAYLOAD_ROOT_AT: usize = 147;
const PAYLOAD_LENGTH_AT: usize = 195;
const PAYLOAD_MASS_AT: usize = 199;
const REWARD_LOCK_AT: usize = 207;
const NONCE_AT: usize = 248;
const EXTRANONCE_AT: usize = 280;

const _: () = assert!(POW_DOMAIN.len() + HEADER_BYTES == PREIMAGE_BYTES);
const _: () = assert!(POW_DOMAIN.len() + ALGORITHM_AT == tenq::OFF_ALGO);
const _: () = assert!(POW_DOMAIN.len() + WORK_REF_AT == tenq::OFF_WORKREF);
const _: () = assert!(POW_DOMAIN.len() + NONCE_AT == tenq::OFF_NONCE);
const _: () = assert!(POW_DOMAIN.len() + EXTRANONCE_AT == tenq::OFF_EXTRANONCE);
const _: () = assert!(REWARD_LOCK_AT + LOCK_BYTES == NONCE_AT);

/// `POST /v1/mining/templates` on the node's mining listener.
#[derive(Clone, Debug, Deserialize)]
pub struct TemplateResponse {
    pub job_id: String,
    pub chain_id: String,
    pub pow_algorithm_id: u8,
    pub spec_revision: u32,
    pub work_ref: String,
    pub target: String,
    pub registration_height: u64,
    pub registration_window_end: u64,
    pub parents_root: String,
    pub payload_root: String,
    pub payload_length: u32,
    pub payload_mass: u64,
    pub reward_lock: String,
    pub body_hex: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TemplateError {
    #[error("template field {0} is invalid")]
    Field(&'static str),
    #[error("template belongs to another chain")]
    ChainId,
    #[error("template requires unsupported TenQ algorithm {algorithm_id} revision {spec_revision}")]
    UnsupportedTenq {
        algorithm_id: u8,
        spec_revision: u32,
    },
    #[error("template does not pay the pool address")]
    Payout,
    #[error("mining rewards require a direct or threshold lock")]
    RewardLockProfile,
    #[error("template registration window is invalid")]
    Window,
    #[error("template body does not match its header fields")]
    Body,
}

/// A checked template; the body stays opaque and goes back to the node unchanged.
#[derive(Debug)]
pub struct Template {
    pub id: Hash384,
    pub chain_id: Hash384,
    pub work_ref: Hash384,
    pub parents_root: Hash384,
    pub payload_root: Hash384,
    pub payload_length: u32,
    pub payload_mass: u64,
    pub reward_lock: Lock,
    pub target: [u8; 32],
    pub registration_height: u64,
    pub registration_window_end: u64,
    pub epoch: u32,
    pub transactions: u32,
    pub body_hex: String,
}

impl Template {
    pub fn parse(
        response: TemplateResponse,
        chain_id: Hash384,
        payout: &Lock,
    ) -> Result<Self, TemplateError> {
        if response.pow_algorithm_id != tenq::ALGORITHM_ID
            || response.spec_revision != tenq::SPEC_REVISION
        {
            return Err(TemplateError::UnsupportedTenq {
                algorithm_id: response.pow_algorithm_id,
                spec_revision: response.spec_revision,
            });
        }
        if response.registration_height == 0
            || response.registration_window_end < response.registration_height
        {
            return Err(TemplateError::Window);
        }
        let hash = |text: &str, field| Hash384::from_hex(text).ok_or(TemplateError::Field(field));
        if hash(&response.chain_id, "chain_id")? != chain_id {
            return Err(TemplateError::ChainId);
        }
        let reward_lock: Lock = fixed(&response.reward_lock, "reward_lock")?;
        if !matches!(reward_lock[0], 1 | 2) || !matches!(payout[0], 1 | 2) {
            return Err(TemplateError::RewardLockProfile);
        }
        if &reward_lock != payout {
            return Err(TemplateError::Payout);
        }
        if response.body_hex.len() > MAX_BODY_BYTES * 2
            || response.payload_length as usize > MAX_BODY_BYTES
        {
            return Err(TemplateError::Body);
        }
        let body = hex::decode(&response.body_hex).map_err(|_| TemplateError::Field("body_hex"))?;
        if body.len() != response.payload_length as usize {
            return Err(TemplateError::Body);
        }
        let transactions = body_transactions(&body).ok_or(TemplateError::Body)?;
        let epoch = epoch_for(response.registration_height).ok_or(TemplateError::Window)?;
        Ok(Self {
            id: hash(&response.job_id, "job_id")?,
            chain_id,
            work_ref: hash(&response.work_ref, "work_ref")?,
            parents_root: hash(&response.parents_root, "parents_root")?,
            payload_root: hash(&response.payload_root, "payload_root")?,
            payload_length: response.payload_length,
            payload_mass: response.payload_mass,
            reward_lock,
            target: fixed(&response.target, "target")?,
            registration_height: response.registration_height,
            registration_window_end: response.registration_window_end,
            epoch,
            transactions,
            body_hex: response.body_hex,
        })
    }

    pub fn network_difficulty(&self) -> u128 {
        difficulty_of(&self.target)
    }

    /// Finalized height of the anchor the work builds on.
    pub const fn height(&self) -> u64 {
        self.registration_height - 1
    }

    pub fn header(&self, extranonce: &[u8; 32], nonce: &[u8; 32]) -> [u8; HEADER_BYTES] {
        let mut header = [0_u8; HEADER_BYTES];
        header[..VERSION_AT].copy_from_slice(&self.chain_id.0);
        header[VERSION_AT..ALGORITHM_AT].copy_from_slice(&HEADER_VERSION.to_be_bytes());
        header[ALGORITHM_AT] = tenq::ALGORITHM_ID;
        header[WORK_REF_AT..PARENTS_ROOT_AT].copy_from_slice(&self.work_ref.0);
        header[PARENTS_ROOT_AT..PAYLOAD_ROOT_AT].copy_from_slice(&self.parents_root.0);
        header[PAYLOAD_ROOT_AT..PAYLOAD_LENGTH_AT].copy_from_slice(&self.payload_root.0);
        header[PAYLOAD_LENGTH_AT..PAYLOAD_MASS_AT]
            .copy_from_slice(&self.payload_length.to_be_bytes());
        header[PAYLOAD_MASS_AT..REWARD_LOCK_AT].copy_from_slice(&self.payload_mass.to_be_bytes());
        header[REWARD_LOCK_AT..NONCE_AT].copy_from_slice(&self.reward_lock);
        header[NONCE_AT..EXTRANONCE_AT].copy_from_slice(nonce);
        header[EXTRANONCE_AT..].copy_from_slice(extranonce);
        header
    }
}

pub fn preimage(header: &[u8; HEADER_BYTES]) -> [u8; PREIMAGE_BYTES] {
    let mut bytes = [0_u8; PREIMAGE_BYTES];
    bytes[..POW_DOMAIN.len()].copy_from_slice(POW_DOMAIN);
    bytes[POW_DOMAIN.len()..].copy_from_slice(header);
    bytes
}

/// Reads the original payout commitment without changing canonical header bytes.
pub(crate) fn reward_lock(header: &[u8; HEADER_BYTES]) -> Lock {
    let mut lock = [0; LOCK_BYTES];
    lock.copy_from_slice(&header[REWARD_LOCK_AT..NONCE_AT]);
    lock
}

pub fn block_id(header: &[u8; HEADER_BYTES]) -> Hash384 {
    domain_hash(BLOCK_ID_DOMAIN, &[header])
}

/// The body starts with a one-byte parent count, the parent IDs and a big-endian transaction count.
fn body_transactions(body: &[u8]) -> Option<u32> {
    let parents = usize::from(*body.first()?);
    if parents > MAX_PARENTS {
        return None;
    }
    let at = 1 + parents * 48;
    let count = u32::from_be_bytes(body.get(at..at + 4)?.try_into().ok()?);
    (count <= MAX_TRANSACTIONS).then_some(count)
}

fn fixed<const N: usize>(text: &str, field: &'static str) -> Result<[u8; N], TemplateError> {
    if text.len() != N * 2 {
        return Err(TemplateError::Field(field));
    }
    hex::decode(text)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(TemplateError::Field(field))
}
