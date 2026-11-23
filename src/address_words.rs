//! Version 1 address verification words shared by the node and wallets.

use crate::chain::{ADDRESS_VERSION, Lock, Network, domain_hash};
use std::sync::OnceLock;

const DOMAIN: &[u8] = b"TRAI_ADDRESS_WORDS_V1";
const WORD_COUNT: usize = 6;
const WORD_BITS: usize = 11;
const WORD_LIST: &str = include_str!("address_words_v1.txt");

fn word_list() -> &'static [&'static str] {
    static LIST: OnceLock<Vec<&'static str>> = OnceLock::new();
    LIST.get_or_init(|| WORD_LIST.lines().collect())
}

/// Six words for manual address comparison.
pub fn verification_words(network: Network, lock: &Lock) -> String {
    let digest = domain_hash(
        DOMAIN,
        &[network.hrp().as_bytes(), &[ADDRESS_VERSION], lock],
    );
    let list = word_list();
    (0..WORD_COUNT)
        .map(|position| {
            let index = (position * WORD_BITS..(position + 1) * WORD_BITS)
                .fold(0_usize, |index, bit| {
                    (index << 1) | usize::from((digest.0[bit / 8] >> (7 - bit % 8)) & 1)
                });
            list[index]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
#[path = "tests/address_words.rs"]
mod tests;
