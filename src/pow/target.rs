use num_bigint::BigUint;
use num_traits::{One, ToPrimitive};

/// floor(2^256 / (target + 1)): the hashes a value at or below `target` costs on average.
pub fn difficulty_of(target: &[u8; 32]) -> u128 {
    let denominator = BigUint::from_bytes_be(target) + 1_u32;
    ((BigUint::one() << 256_u32) / denominator)
        .to_u128()
        .unwrap_or(u128::MAX)
}

/// The largest target that meets `difficulty`, floor(2^256 / difficulty) - 1.
pub fn target_for(difficulty: u128) -> [u8; 32] {
    if difficulty <= 1 {
        return [u8::MAX; 32];
    }
    let target = (BigUint::one() << 256_u32) / BigUint::from(difficulty) - 1_u32;
    let bytes = target.to_bytes_be();
    let mut result = [0_u8; 32];
    result[32 - bytes.len()..].copy_from_slice(&bytes);
    result
}

/// Values and targets compare as unsigned big-endian numbers.
pub fn meets(value: &[u8; 32], target: &[u8; 32]) -> bool {
    value <= target
}
