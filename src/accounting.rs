//! Exact integer reward splits.

use crate::config::BASIS_POINTS;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SplitError {
    #[error("a split needs a positive total weight")]
    ZeroWeight,
    #[error("split arithmetic overflowed")]
    Overflow,
}

/// Fee and per-miner credits of one block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Split {
    pub fee: u64,
    pub retained: u64,
    pub credits: Vec<(u32, u64)>,
}

/// Splits `reward` by `weights` after withholding `fee_bps`; unassigned fee stays in the pool wallet.
pub fn split(
    reward: u64,
    fee_bps: u32,
    weights: &[(u32, u128)],
    fee_recipients: &[(u32, u32)],
) -> Result<Split, SplitError> {
    let fee = mul_div_floor(
        u128::from(reward),
        u128::from(fee_bps),
        u128::from(BASIS_POINTS),
    )
    .ok_or(SplitError::Overflow)?;
    let fee = u64::try_from(fee).map_err(|_| SplitError::Overflow)?;
    let distributable = reward.checked_sub(fee).ok_or(SplitError::Overflow)?;
    let mut credits: BTreeMap<u32, u64> = BTreeMap::new();
    let amounts = cumulative_floor_split(
        u128::from(distributable),
        &weights
            .iter()
            .map(|(_, weight)| *weight)
            .collect::<Vec<_>>(),
    )?;
    for ((miner, _), amount) in weights.iter().zip(amounts) {
        add(&mut credits, *miner, amount)?;
    }
    let mut retained = fee;
    if fee > 0 && !fee_recipients.is_empty() {
        let assigned: u32 = fee_recipients.iter().map(|(_, share)| share).sum();
        let mut shares: Vec<u128> = fee_recipients
            .iter()
            .map(|(_, share)| u128::from(*share))
            .collect();
        shares.push(u128::from(BASIS_POINTS.saturating_sub(assigned)));
        let amounts = cumulative_floor_split(u128::from(fee), &shares)?;
        for ((miner, _), amount) in fee_recipients.iter().zip(&amounts) {
            add(&mut credits, *miner, *amount)?;
        }
        retained =
            u64::try_from(*amounts.last().unwrap_or(&0)).map_err(|_| SplitError::Overflow)?;
    }
    credits.retain(|_, amount| *amount > 0);
    Ok(Split {
        fee,
        retained,
        credits: credits.into_iter().collect(),
    })
}

fn add(credits: &mut BTreeMap<u32, u64>, miner: u32, amount: u128) -> Result<(), SplitError> {
    let amount = u64::try_from(amount).map_err(|_| SplitError::Overflow)?;
    let entry = credits.entry(miner).or_default();
    *entry = entry.checked_add(amount).ok_or(SplitError::Overflow)?;
    Ok(())
}

/// Shares that telescope to exactly `amount`: share i is floor(amount * W_i / W) - floor(amount * W_{i-1} / W).
pub fn cumulative_floor_split(amount: u128, weights: &[u128]) -> Result<Vec<u128>, SplitError> {
    let total = weights
        .iter()
        .try_fold(0_u128, |sum, weight| sum.checked_add(*weight))
        .ok_or(SplitError::Overflow)?;
    if total == 0 {
        return Err(SplitError::ZeroWeight);
    }
    let mut shares = Vec::with_capacity(weights.len());
    let mut cumulative = 0_u128;
    let mut previous = 0_u128;
    for weight in weights {
        cumulative = cumulative
            .checked_add(*weight)
            .ok_or(SplitError::Overflow)?;
        let boundary = mul_div_floor(amount, cumulative, total).ok_or(SplitError::Overflow)?;
        shares.push(boundary.checked_sub(previous).ok_or(SplitError::Overflow)?);
        previous = boundary;
    }
    Ok(shares)
}

/// `floor(a * b / d)` with a 256-bit product.
pub fn mul_div_floor(a: u128, b: u128, d: u128) -> Option<u128> {
    if d == 0 {
        return None;
    }
    let (high, low) = widening_mul(a, b);
    if high == 0 {
        return Some(low / d);
    }
    divide_256(high, low, d)
}

fn widening_mul(a: u128, b: u128) -> (u128, u128) {
    const MASK: u128 = u64::MAX as u128;
    let (a_low, a_high) = (a & MASK, a >> 64);
    let (b_low, b_high) = (b & MASK, b >> 64);
    let low_low = a_low * b_low;
    let low_high = a_low * b_high;
    let high_low = a_high * b_low;
    let high_high = a_high * b_high;
    let middle = (low_low >> 64) + (low_high & MASK) + (high_low & MASK);
    let low = (low_low & MASK) | (middle << 64);
    let high = high_high + (low_high >> 64) + (high_low >> 64) + (middle >> 64);
    (high, low)
}

fn divide_256(high: u128, low: u128, divisor: u128) -> Option<u128> {
    if high >= divisor {
        return None;
    }
    let mut remainder = high;
    let mut quotient = 0_u128;
    for bit in (0..128).rev() {
        let carry = remainder >> 127;
        remainder = (remainder << 1) | ((low >> bit) & 1);
        if carry == 1 || remainder >= divisor {
            remainder = remainder.wrapping_sub(divisor);
            quotient |= 1 << bit;
        }
    }
    Some(quotient)
}

#[cfg(test)]
#[path = "tests/accounting.rs"]
mod tests;
