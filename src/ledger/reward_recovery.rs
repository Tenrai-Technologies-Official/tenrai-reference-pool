use super::*;
use crate::{
    chain::{Network, address_text},
    node::SettlementHead,
    pow::template::{HEADER_BYTES, block_id, reward_lock},
    wallet::WorkReward,
};
use rusqlite::OptionalExtension;

pub(super) const MAX_RECOVERIES: usize = 4;

fn coherent(
    before: &SettlementHead,
    after: &SettlementHead,
    reward: &WorkReward,
    chain: Hash384,
) -> bool {
    let anchor = &reward.anchor;
    let matches = |head: &SettlementHead| {
        anchor.cut_id == head.cut_id
            && anchor.state_root == head.state_root
            && anchor.validator_set_id == head.validator_set_id
    };
    before.chain_id == chain.to_hex()
        && after.chain_id == before.chain_id
        && after.height >= before.height
        && (after.height != before.height || after == before)
        && anchor.height >= before.height
        && anchor.height <= after.height
        && (anchor.height != before.height || matches(before))
        && (anchor.height != after.height || matches(after))
}

fn original_address(bytes: &[u8], id: Hash384, chain: Hash384, network: Network) -> Option<String> {
    let header: &[u8; HEADER_BYTES] = bytes.try_into().ok()?;
    if header[..48] != chain.0 || block_id(header) != id {
        return None;
    }
    let lock = reward_lock(header);
    matches!(lock[0], 1 | 2).then(|| address_text(&lock, network))
}

impl Ledger {
    pub(super) async fn recover_reward(&mut self, id: Hash384) {
        let Some(wallet) = self.reward_wallet.as_ref() else {
            return;
        };
        // Look up the retained header using its original payout address.
        let bytes = self
            .hub
            .reader
            .read(move |connection| {
                connection
                    .query_row(
                        "SELECT p.header FROM found_payload p JOIN blocks b ON b.id=p.block_id
                WHERE b.hash=?1 AND b.status<2 AND length(p.header)=312",
                        [id.to_hex()],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
            })
            .await;
        let original = match bytes {
            Ok(Some(bytes)) => original_address(&bytes, id, self.hub.chain_id, self.hub.network),
            _ => None,
        };
        let Some(original) = original else {
            return;
        };
        let before = match self.node.settlement_head().await {
            Ok(head)
                if head.chain_id == self.hub.chain_id.to_hex()
                    && head.height >= *self.heights.borrow() =>
            {
                head
            }
            _ => return,
        };
        let evidence = wallet
            .work_reward_evidence(
                id,
                self.hub.chain_id,
                self.hub.network,
                &original,
                before.height,
            )
            .await;
        let reward = match evidence {
            Ok(Some(reward)) => reward,
            Ok(None) => return,
            Err(error) => {
                tracing::debug!("old work reward {id} remains unresolved: {error}");
                return;
            }
        };
        let after = match self.node.settlement_head().await {
            Ok(head) if head.height >= *self.heights.borrow() => head,
            _ => return,
        };
        if !coherent(&before, &after, &reward, self.hub.chain_id) {
            tracing::warn!("old work reward {id} has inconsistent node/daemon context");
            return;
        }
        // Settle the original work and split once, including on RPC replay.
        self.credit_many(reward.created, &[(id, reward.amount)])
            .await;
    }
}

#[cfg(test)]
mod tests;
