use crate::chain::{Hash384, Network};
use libcrux_ml_dsa::ml_dsa_65::{self, MLDSA65Signature, MLDSA65VerificationKey};
use rand_core::{OsRng, RngCore};
use reqwest::header::HeaderMap;
use serde::Deserialize;
use sha2::{Digest, Sha384};
use sha3::{
    Shake256,
    digest::{ExtendableOutput, Update, XofReader},
};
use thiserror::Error;

pub const HEADER_IDENTITY: &str = "x-tenrai-node-identity";
pub const HEADER_NONCE: &str = "x-tenrai-request-nonce";
pub const HEADER_HEIGHT: &str = "x-tenrai-tip-height";
pub const HEADER_SIGNATURE: &str = "x-tenrai-signature";
pub const HEADER_VERSION: &str = "x-tenrai-envelope-version";

pub const NODE_IDENTITY_BYTES: usize = 32;
const ML_DSA65_PUBLIC_KEY_BYTES: usize = 1_952;
const ML_DSA65_SIGNATURE_BYTES: usize = 3_309;
const REQUEST_NONCE_BYTES: usize = 32;
const DIGEST_BYTES: usize = 48;
const ENVELOPE_VERSION: u16 = 1;
const ENVELOPE_DOMAIN: &[u8] = b"TRAI/API/ENVELOPE/1";
const SIGNING_CONTEXT: &[u8] = b"TRAI/API/RESPONSE/1";
const NODE_ID_DOMAIN: &[u8] = b"TRAI/P2P/MLDSA65/NODE-ID/1";
const MAX_METHOD_BYTES: usize = 16;
const MAX_TARGET_BYTES: usize = 2_048;

#[derive(Clone)]
pub struct NodeTrust {
    chain_id: Hash384,
    identity: [u8; NODE_IDENTITY_BYTES],
    public_key: Box<[u8; ML_DSA65_PUBLIC_KEY_BYTES]>,
}

#[derive(Clone, Copy)]
pub struct SignedCall<'a> {
    pub method: &'a str,
    pub target: &'a str,
    pub request_body: &'a [u8],
    pub status: u16,
    pub response_body: &'a [u8],
    pub headers: &'a HeaderMap,
    pub nonce: [u8; REQUEST_NONCE_BYTES],
}

#[derive(Debug, Error)]
pub enum NodeTrustError {
    #[error("the node identity document is malformed")]
    IdentityDocument,
    #[error("the node identity does not match the configured value")]
    IdentityMismatch,
    #[error("the node serves a different chain")]
    ChainMismatch,
    #[error("the node uses an unsupported identity profile")]
    Profile,
    #[error("the node response has no valid signed envelope")]
    Unsigned,
    #[error("the node response signature is invalid")]
    Signature,
    #[error("the response envelope is outside its canonical bounds")]
    Bounds,
    #[error("the operating system did not provide a request nonce")]
    Randomness,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityDocument {
    api_version: u16,
    network: String,
    chain_id: String,
    identity: String,
    public_key: String,
    key_scheme: String,
}

impl NodeTrust {
    /// Pins a node identity after the operator selected its chain and identity.
    pub fn from_identity_document(
        body: &[u8],
        expected_chain_id: Hash384,
        expected_identity: [u8; NODE_IDENTITY_BYTES],
    ) -> Result<Self, NodeTrustError> {
        let document: IdentityDocument =
            serde_json::from_slice(body).map_err(|_| NodeTrustError::IdentityDocument)?;
        if document.api_version != 1 || Network::parse(&document.network).is_none() {
            return Err(NodeTrustError::Profile);
        }
        if document.key_scheme != "mldsa65" {
            return Err(NodeTrustError::Profile);
        }
        let chain_id =
            canonical_hash(&document.chain_id).ok_or(NodeTrustError::IdentityDocument)?;
        if chain_id != expected_chain_id {
            return Err(NodeTrustError::ChainMismatch);
        }
        let identity = decode_lower(&document.identity).ok_or(NodeTrustError::IdentityDocument)?;
        if identity != expected_identity {
            return Err(NodeTrustError::IdentityMismatch);
        }
        let public_key =
            decode_lower(&document.public_key).ok_or(NodeTrustError::IdentityDocument)?;
        if node_identity(&public_key) != identity {
            return Err(NodeTrustError::IdentityDocument);
        }
        Ok(Self {
            chain_id,
            identity,
            public_key: Box::new(public_key),
        })
    }

    pub fn fresh_nonce(&self) -> Result<[u8; REQUEST_NONCE_BYTES], NodeTrustError> {
        let mut nonce = [0_u8; REQUEST_NONCE_BYTES];
        OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| NodeTrustError::Randomness)?;
        Ok(nonce)
    }

    pub fn verify(&self, call: SignedCall<'_>) -> Result<(), NodeTrustError> {
        let header = |name: &str| {
            let mut values = call.headers.get_all(name).iter();
            let value = values.next().ok_or(NodeTrustError::Unsigned)?;
            if values.next().is_some() {
                return Err(NodeTrustError::Unsigned);
            }
            value.to_str().map_err(|_| NodeTrustError::Unsigned)
        };
        if header(HEADER_VERSION)? != ENVELOPE_VERSION.to_string() {
            return Err(NodeTrustError::Unsigned);
        }
        let nonce: [u8; REQUEST_NONCE_BYTES] =
            decode_lower(header(HEADER_NONCE)?).ok_or(NodeTrustError::Unsigned)?;
        if nonce != call.nonce || nonce == [0; REQUEST_NONCE_BYTES] {
            return Err(NodeTrustError::Unsigned);
        }
        let identity: [u8; NODE_IDENTITY_BYTES] =
            decode_lower(header(HEADER_IDENTITY)?).ok_or(NodeTrustError::Unsigned)?;
        if identity != self.identity {
            return Err(NodeTrustError::IdentityMismatch);
        }
        let signature: [u8; ML_DSA65_SIGNATURE_BYTES] =
            decode_lower(header(HEADER_SIGNATURE)?).ok_or(NodeTrustError::Unsigned)?;
        let tip_height = canonical_u64(header(HEADER_HEIGHT)?).ok_or(NodeTrustError::Unsigned)?;
        let message = envelope_bytes(
            self.chain_id,
            call.method,
            call.target,
            digest(call.request_body),
            call.status,
            digest(call.response_body),
            identity,
            nonce,
            tip_height,
        )?;
        let key = MLDSA65VerificationKey::new(*self.public_key);
        let signature = MLDSA65Signature::new(signature);
        ml_dsa_65::portable::verify(&key, &message, SIGNING_CONTEXT, &signature)
            .map_err(|_| NodeTrustError::Signature)
    }
}

pub fn parse_identity(value: &str) -> Result<[u8; NODE_IDENTITY_BYTES], NodeTrustError> {
    decode_lower(value).ok_or(NodeTrustError::IdentityDocument)
}

fn canonical_hash(value: &str) -> Option<Hash384> {
    let hash = Hash384::from_hex(value)?;
    (hash.to_hex() == value).then_some(hash)
}

fn decode_lower<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 || value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return None;
    }
    let mut bytes = [0_u8; N];
    hex::decode_to_slice(value, &mut bytes).ok()?;
    (hex::encode(bytes) == value).then_some(bytes)
}

fn canonical_u64(value: &str) -> Option<u64> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn digest(body: &[u8]) -> [u8; DIGEST_BYTES] {
    Sha384::digest(body).into()
}

fn node_identity(public_key: &[u8; ML_DSA65_PUBLIC_KEY_BYTES]) -> [u8; NODE_IDENTITY_BYTES] {
    let mut hasher = Shake256::default();
    hasher.update(NODE_ID_DOMAIN);
    hasher.update(public_key);
    let mut identity = [0_u8; NODE_IDENTITY_BYTES];
    hasher.finalize_xof().read(&mut identity);
    identity
}

#[allow(clippy::too_many_arguments)]
fn envelope_bytes(
    chain_id: Hash384,
    method: &str,
    target: &str,
    request_digest: [u8; DIGEST_BYTES],
    status: u16,
    response_digest: [u8; DIGEST_BYTES],
    identity: [u8; NODE_IDENTITY_BYTES],
    nonce: [u8; REQUEST_NONCE_BYTES],
    tip_height: u64,
) -> Result<Vec<u8>, NodeTrustError> {
    if method.is_empty()
        || method.len() > MAX_METHOD_BYTES
        || target.is_empty()
        || target.len() > MAX_TARGET_BYTES
    {
        return Err(NodeTrustError::Bounds);
    }
    let mut bytes = Vec::with_capacity(
        ENVELOPE_DOMAIN.len()
            + 2
            + 48
            + 1
            + method.len()
            + 2
            + target.len()
            + DIGEST_BYTES * 2
            + 2
            + NODE_IDENTITY_BYTES
            + REQUEST_NONCE_BYTES
            + 8,
    );
    bytes.extend_from_slice(ENVELOPE_DOMAIN);
    bytes.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&chain_id.0);
    bytes.push(method.len() as u8);
    bytes.extend_from_slice(method.as_bytes());
    bytes.extend_from_slice(&(target.len() as u16).to_le_bytes());
    bytes.extend_from_slice(target.as_bytes());
    bytes.extend_from_slice(&request_digest);
    bytes.extend_from_slice(&status.to_le_bytes());
    bytes.extend_from_slice(&response_digest);
    bytes.extend_from_slice(&identity);
    bytes.extend_from_slice(&nonce);
    bytes.extend_from_slice(&tip_height.to_le_bytes());
    Ok(bytes)
}

#[cfg(test)]
#[path = "trust_tests.rs"]
mod tests;
