//! A missing work query is negative evidence only while its whole registration window is held.
use super::*;

impl NodeClient {
    /// The configured node is a trusted local/pinned service; these are authenticated service
    /// observations, not a new consensus absence proof. Old positive facts remain useful.
    pub(crate) async fn block_in_registration_window(
        &self,
        id: Hash384,
        anchor: u64,
        window_end: u64,
        chain: Hash384,
    ) -> Result<Option<BlockState>, NodeError> {
        let first = anchor
            .checked_add(1)
            .filter(|first| *first <= window_end)
            .ok_or_else(|| NodeError::Answer("invalid local work registration window".into()))?;
        let (before, floor_before) = tokio::join!(self.settlement_head(), self.history_floor());
        let before = before?;
        if before.chain_id != chain.to_hex() {
            return Err(NodeError::Answer(
                "work settlement belongs to another chain".into(),
            ));
        }
        let reply = self
            .request(
                Method::GET,
                &self.api,
                &format!("v1/blocks/{}", id.to_hex()),
                Vec::new(),
            )
            .await?;
        let block = if reply.status == StatusCode::NOT_FOUND {
            let failure = error_document(&reply.body, reply.status);
            if failure.code.as_deref() != Some("not_found")
                || failure.reason.as_deref() != Some("history_not_held")
            {
                return Err(status_error(reply));
            }
            None
        } else {
            Some(decode(reply)?)
        };
        let (after, floor_after) = tokio::join!(self.settlement_head(), self.history_floor());
        let after = after?;
        if after.chain_id != before.chain_id
            || after.height < before.height
            || (after.height == before.height && after != before)
        {
            return Err(NodeError::Answer(
                "work settlement head changed inconsistently".into(),
            ));
        }
        let unregistered = block.as_ref().is_some_and(|state: &BlockState| {
            state.state == "pooled"
                && state.registration_height.is_none()
                && state.execution_height.is_none()
                && state.reward.is_none()
        });
        if block.is_none() || unregistered {
            // The raw floor is conservative: its complete bodies also retain registration
            // metadata, including the first possible cut. An older gap cannot expire an intent.
            if before.height <= window_end
                || after.height <= window_end
                || floor_before? > first
                || floor_after? > first
            {
                return Err(NodeError::Answer(
                    "the node cannot establish absence across this work registration window".into(),
                ));
            }
        }
        Ok(block)
    }
}

#[cfg(test)]
mod tests;
