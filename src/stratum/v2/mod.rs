//! tenrai-mining-v2, specified in protocol/tenrai-mining-v2.md of the node repository.

#[cfg(test)]
mod client;
mod fields;
mod frame;
mod message;
mod server;

#[cfg(test)]
pub use client::{ClientCodec, ClientEvent};
pub use message::{Job, Message};
pub use server::ServerCodec;

use thiserror::Error;

pub const PROTOCOL: &str = "tenrai-mining-v2";
pub const ALPN: &[u8] = b"tenrai-mining/2";
pub const MAX_FRAME_BYTES: usize = 16_384;
/// A connection carries one channel.
pub const CHANNEL: u32 = 1;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("a frame of {0} bytes is over the size limit")]
    Oversized(usize),
    #[error("malformed {0}")]
    Malformed(&'static str),
}

fn unhex<const N: usize>(text: &str, what: &'static str) -> Result<[u8; N], CodecError> {
    hex::decode(text)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(CodecError::Malformed(what))
}

#[cfg(test)]
#[path = "../../tests/v2.rs"]
mod tests;
