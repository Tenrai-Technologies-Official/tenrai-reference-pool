use crate::{
    blocks::Blocks,
    chain::{Hash384, Network},
    config::Config,
    console::Tally,
    db::{
        Reader, Writer,
        accepted::{AcceptanceError, AcceptedReceipt, AcceptedWork},
    },
    pplns::ShareLog,
    rate::CheckBudget,
    registry::Registry,
    stats::Stats,
    stratum::admission::Admission,
    work::WorkSource,
};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, watch};

/// State shared by the sessions, the ledger and the API.
pub struct Hub {
    pub config: Config,
    pub network: Network,
    pub chain_id: Hash384,
    pub pool_address: String,
    pub work: WorkSource,
    pub heights: watch::Receiver<u64>,
    pub verify: Arc<Semaphore>,
    pub budget: CheckBudget,
    pub admission: Arc<Admission>,
    pub stats: Arc<Stats>,
    pub registry: Arc<Registry>,
    pub pplns: Mutex<ShareLog>,
    pub writer: Writer,
    pub reader: Reader,
    pub blocks: Blocks,
    pub tally: Arc<Tally>,
    pub shutdown: watch::Receiver<bool>,
}

impl Hub {
    /// Height at which a block found now is registered.
    pub fn registration_height(&self) -> u64 {
        self.heights.borrow().saturating_add(1)
    }

    /// Monetary accounting precedes optional cache/statistics updates and the Stratum ACK.
    pub async fn record_share(
        &self,
        request: AcceptedWork,
        pending: Arc<crate::db::accepted::pending::WorkLease>,
    ) -> Result<AcceptedReceipt, AcceptanceError> {
        let key = request.key.clone();
        let network = request.found.as_ref().map(|found| found.network_difficulty);
        let receipt = self.writer.accept_claimed(request, pending).await?;
        if receipt.fresh {
            self.registry.remember(&key.address, receipt.miner);
            self.stats
                .accepted(&key, (receipt.miner, receipt.worker), receipt.weight);
            if receipt.mode == crate::config::Mode::Pplns
                && let Ok(mut log) = self.pplns.lock()
                && let Err(error) =
                    log.record(receipt.id, receipt.time, receipt.miner, receipt.weight)
            {
                tracing::warn!(
                    "accepted share is durable; optional cache update failed: {error:#}"
                );
            }
            if let (Some(block), Some(network)) = (&receipt.block, network) {
                self.blocks.committed(self, &key, block, network);
            }
        }
        Ok(receipt)
    }
}
