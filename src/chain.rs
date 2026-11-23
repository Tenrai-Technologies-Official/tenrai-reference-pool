//! Address encoding, domain hashes and mining tags.

use bech32::{Bech32m, Hrp, primitives::decode::CheckedHrpstring};
use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha384};
use std::fmt;
use thiserror::Error;

pub const LOCK_BYTES: usize = 41;
pub const MAINNET_CHAIN_ID: &str = "8395f150390e8b3ac210696c2e095b09a219da031ed563f1a22664dbc31bfe571115fb8ef69d5bbc8250e6cb339489d1";
pub const DEVELOPMENT_CHAIN_ID: &str = "dededededededededededededededededededededededededededededededededededededededededededededededede";
pub const MAX_TAG_BYTES: usize = 20;
pub(crate) const ADDRESS_VERSION: u8 = 1;
const LOCK_PROFILES: [u8; 2] = [1, 2];
const TAG_PUNCTUATION: &[u8] = b" ._-/:+@#()!";

/// Canonical 41-byte spend or reward lock.
pub type Lock = [u8; LOCK_BYTES];

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Hash384(pub [u8; 48]);

impl Hash384 {
    pub fn from_hex(text: &str) -> Option<Self> {
        if text.len() != 96 {
            return None;
        }
        Some(Self(hex::decode(text).ok()?.try_into().ok()?))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for Hash384 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl fmt::Display for Hash384 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// SHA-384 over a length-prefixed domain and length-prefixed parts.
pub fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> Hash384 {
    let domain_length = u16::try_from(domain.len()).expect("domains are short literals");
    let mut digest = Sha384::new();
    digest.update(domain_length.to_be_bytes());
    digest.update(domain);
    digest.update((parts.len() as u64).to_be_bytes());
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    Hash384(digest.finalize().into())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Network {
    Mainnet,
    Testnet,
    Testnet100,
    Development,
}

impl Network {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "mainnet" => Some(Self::Mainnet),
            "testnet" => Some(Self::Testnet),
            "testnet100" => Some(Self::Testnet100),
            "development" => Some(Self::Development),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Testnet100 => "testnet100",
            Self::Development => "development",
        }
    }

    /// Testnet100 shares the testnet prefix.
    pub const fn hrp(self) -> &'static str {
        match self {
            Self::Mainnet => "trai",
            Self::Testnet | Self::Testnet100 => "ttrai",
            Self::Development => "dtrai",
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum AddressError {
    #[error("addresses are written in lower case")]
    Uppercase,
    #[error("a character is wrong or missing: the address checksum does not match")]
    Encoding,
    #[error("the address belongs to another network")]
    Network,
    #[error("the address payload is not a Tenrai address")]
    Payload,
    #[error(
        "mining rewards require a direct or threshold address; native timelock and hashlock addresses are not supported"
    )]
    NativeRewardLock,
}

pub fn parse_address(text: &str, network: Network) -> Result<Lock, AddressError> {
    if text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(AddressError::Uppercase);
    }
    let checked = CheckedHrpstring::new::<Bech32m>(text).map_err(|_| AddressError::Encoding)?;
    if checked.hrp().as_str() != network.hrp() {
        return Err(AddressError::Network);
    }
    let payload: Vec<u8> = checked.byte_iter().collect();
    if payload.len() != 1 + LOCK_BYTES || payload[0] != ADDRESS_VERSION {
        return Err(AddressError::Payload);
    }
    if payload[1] == 3 {
        return Err(AddressError::NativeRewardLock);
    }
    if !LOCK_PROFILES.contains(&payload[1]) {
        return Err(AddressError::Payload);
    }
    let lock: Lock = payload[1..].try_into().map_err(|_| AddressError::Payload)?;
    if address_text(&lock, network) != text {
        return Err(AddressError::Encoding);
    }
    Ok(lock)
}

pub fn address_text(lock: &Lock, network: Network) -> String {
    let mut payload = [0_u8; 1 + LOCK_BYTES];
    payload[0] = ADDRESS_VERSION;
    payload[1..].copy_from_slice(lock);
    let hrp = Hrp::parse(network.hrp()).expect("Tenrai prefixes are valid");
    bech32::encode::<Bech32m>(hrp, &payload).expect("an address payload fits bech32m")
}

/// Validate the tag alphabet and reject leading or trailing spaces.
pub fn is_miner_tag(tag: &[u8]) -> bool {
    !tag.is_empty()
        && tag.len() <= MAX_TAG_BYTES
        && tag.first() != Some(&b' ')
        && tag.last() != Some(&b' ')
        && tag
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || TAG_PUNCTUATION.contains(byte))
}

pub fn check_tag(tag: &str) -> Result<(), String> {
    if tag.is_empty() || is_miner_tag(tag.as_bytes()) {
        return Ok(());
    }
    Err(format!(
        "a miner tag has at most {MAX_TAG_BYTES} letters, digits, spaces or . _ - / : + @ # ( ) !, with no space at either end"
    ))
}

/// Encode the tag length and bytes, then fill the remaining bytes randomly.
pub fn fresh_extranonce(tag: &str) -> [u8; 32] {
    let mut extranonce = [0_u8; 32];
    OsRng.fill_bytes(&mut extranonce);
    let tag = if is_miner_tag(tag.as_bytes()) {
        tag.as_bytes()
    } else {
        &[]
    };
    extranonce[0] = tag.len() as u8;
    extranonce[1..=tag.len()].copy_from_slice(tag);
    extranonce
}

#[cfg(test)]
#[path = "tests/chain.rs"]
mod tests;
