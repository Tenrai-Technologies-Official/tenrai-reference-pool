//! The wallet daemon verifies the QC and current UTXO proof; this client checks request bindings.
use super::*;
use crate::chain::{address_text, domain_hash, parse_address};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RewardAnchor {
    pub height: u64,
    pub cut_id: String,
    pub state_root: String,
    pub validator_set_id: String,
}

#[derive(Debug)]
pub(crate) struct WorkReward {
    pub amount: u64,
    pub created: u64,
    pub anchor: RewardAnchor,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    block: String,
    chain_id: String,
    reward_lock: String,
    reward_address: String,
    anchor: RewardAnchor,
    evidence: String,
    reward: Option<Reward>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reward {
    outpoint: Point,
    amount_atoms: String,
    source: String,
    created_finalized_height: u64,
    mature_at_height: u64,
    mature: bool,
    confirmations: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Point {
    transaction_id: String,
    output_index: u32,
}

fn malformed() -> WalletError {
    WalletError::Answer("the daemon's work reward evidence is inconsistent".into())
}

fn checked(
    document: Document,
    block: Hash384,
    chain: Hash384,
    network: Network,
    address: &str,
    minimum: u64,
) -> Result<Option<WorkReward>, WalletError> {
    let lock = parse_address(address, network).map_err(|_| malformed())?;
    let canonical = |text: &str| Hash384::from_hex(text).is_some_and(|hash| hash.to_hex() == text);
    if document.block != block.to_hex()
        || document.chain_id != chain.to_hex()
        || document.reward_lock != hex::encode(lock)
        || document.reward_address != address_text(&lock, network)
        || document.anchor.height < minimum
        || ![
            &document.anchor.cut_id,
            &document.anchor.state_root,
            &document.anchor.validator_set_id,
        ]
        .iter()
        .all(|text| canonical(text))
    {
        return Err(malformed());
    }
    let Some(reward) = document.reward else {
        return if document.evidence == "unknown" {
            Ok(None)
        } else {
            Err(malformed())
        };
    };
    let amount = reward
        .amount_atoms
        .parse::<u64>()
        .map_err(|_| malformed())?;
    let point = domain_hash(b"TRAI_WORK_REWARD_V1", &[&chain.0, &block.0]);
    let confirmations = document
        .anchor
        .height
        .checked_sub(reward.created_finalized_height)
        .and_then(|height| height.checked_add(1))
        .ok_or_else(malformed)?;
    if document.evidence != "unspent"
        || amount == 0
        || amount.to_string() != reward.amount_atoms
        || reward.source != "work_reward"
        || !reward.mature
        || reward.created_finalized_height == 0
        || reward.mature_at_height != reward.created_finalized_height
        || reward.confirmations != confirmations
        || reward.outpoint.transaction_id != point.to_hex()
        || reward.outpoint.output_index != 0
    {
        return Err(malformed());
    }
    Ok(Some(WorkReward {
        amount,
        created: reward.created_finalized_height,
        anchor: document.anchor,
    }))
}

impl Wallet {
    /// Positive-only recovery; absence and every RPC error leave the original intent unresolved.
    pub(crate) async fn work_reward_evidence(
        &self,
        block: Hash384,
        chain: Hash384,
        network: Network,
        address: &str,
        minimum: u64,
    ) -> Result<Option<WorkReward>, WalletError> {
        let lock = parse_address(address, network).map_err(|_| malformed())?;
        let document = self
            .call_as(
                "work_reward_evidence",
                json!({
                    "block":block.to_hex(), "reward_lock":hex::encode(lock),
                    "reward_address":address, "minimum_height":minimum,
                }),
            )
            .await?;
        checked(document, block, chain, network, address, minimum)
    }
}

#[cfg(test)]
mod tests;
