//! Payout rounds with durable daemon request IDs.

use crate::{
    chain::{Hash384, Network, parse_address},
    cli::FeePayer,
    config::{PayoutSettings, format_trai},
    db::{Op, PayoutStatus, Reader, Writer, now_millis, read},
    node::TransactionStatus,
    wallet::{Wallet, WalletError},
};
use anyhow::{Context, Result};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Mutex,
    time::Instant,
};
use tokio::{
    sync::watch,
    time::{MissedTickBehavior, interval},
};

/// Finalized cuts after expiry before reporting an unresolved payout.
const VALIDITY_GRACE: u64 = 4;
/// Fee reserve before an unsigned quote is available.
const FEE_RESERVE: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Sending {
    Sent,
    NeverSigned,
    Unknown,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Verdict {
    Paid(u64),
    Failed(String),
    /// Expired status with an unresolved original payment.
    Expired,
    Open,
}

/// Bounded review logs and a fair cursor. Keep reservations in SQLite.
#[derive(Default)]
pub struct ReviewState {
    seen: BTreeSet<i64>,
    order: VecDeque<i64>,
    after: i64,
    through: i64,
}

const REVIEW_LOG_IDS: usize = 1024;

impl ReviewState {
    fn insert(&mut self, id: i64) -> bool {
        if self.seen.contains(&id) {
            return false;
        }
        if self.order.len() == REVIEW_LOG_IDS
            && let Some(oldest) = self.order.pop_front()
        {
            self.seen.remove(&oldest);
        }
        self.seen.insert(id);
        self.order.push_back(id);
        true
    }

    fn remove(&mut self, id: &i64) {
        self.seen.remove(id);
        self.order.retain(|known| known != id);
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

pub struct Payouts {
    pub wallet: Wallet,
    pub checked: bool,
    pub node: crate::node::NodeClient,
    pub reader: Reader,
    pub writer: Writer,
    pub settings: PayoutSettings,
    pub network: Network,
    pub chain_id: Hash384,
    pub pool_address: String,
    pub request_prefix: String,
    pub heights: watch::Receiver<u64>,
    pub shutdown: watch::Receiver<bool>,
    /// Suppress repeated review logs for the same payout.
    pub unresolved: Mutex<ReviewState>,
}

impl Payouts {
    pub async fn run(self) {
        let every = self.settings.check_every;
        let mut ticker = interval(every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut compounding = interval(self.settings.compound_every);
        compounding.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut shutdown = self.shutdown.clone();
        let mut next_round = Instant::now() + every;
        let mut checked = self.checked;
        loop {
            let compound = tokio::select! {
                _ = ticker.tick() => false,
                _ = compounding.tick() => true,
                _ = shutdown.changed() => return,
            };
            if compound {
                if checked && self.settings.compound_above > 0 {
                    self.compound().await;
                }
                continue;
            }
            if !checked {
                match self.check_wallet().await {
                    Ok(()) => {
                        tracing::info!(
                            "the wallet daemon answers for the pool address; payouts start"
                        );
                        checked = true;
                    }
                    Err(error) => {
                        tracing::warn!("payouts wait for the wallet daemon: {error:#}");
                        continue;
                    }
                }
            }
            let open = match self.settle().await {
                Ok(open) => open,
                Err(error) => {
                    tracing::warn!("cannot check open payouts: {error:#}");
                    continue;
                }
            };
            if self.settings.enabled && open == 0 && Instant::now() >= next_round {
                next_round = Instant::now() + self.settings.interval;
                if let Err(error) = self.round().await {
                    tracing::warn!("payout round stopped: {error:#}");
                }
            }
        }
    }

    /// Verify the daemon's chain and payout address after a deferred startup check.
    async fn check_wallet(&self) -> Result<()> {
        let address = self
            .wallet
            .checked_address(self.network, self.chain_id)
            .await?;
        anyhow::ensure!(
            address == self.pool_address,
            "the wallet daemon receives at {address}, not at the pool address {}",
            self.pool_address
        );
        Ok(())
    }

    /// Check one page. Report whether any prepared payout remains.
    async fn settle(&self) -> Result<usize> {
        let (after, through) = {
            let state = self
                .unresolved
                .lock()
                .map_err(|_| anyhow::anyhow!("payout review state is unavailable"))?;
            (state.after, state.through)
        };
        let (start, through, open) = self
            .reader
            .read(move |connection| {
                // Freeze the scan bound so new rows cannot starve older payouts.
                let (mut start, mut through) = if through == 0 {
                    (0, read::maximum_open_payout_id(connection)?)
                } else {
                    (after, through)
                };
                let mut open = read::open_payout_page(
                    connection,
                    start,
                    through,
                    read::MAX_OPEN_PAYOUT_CHECKS,
                )?;
                if open.is_empty() && through > 0 {
                    start = 0;
                    through = read::maximum_open_payout_id(connection)?;
                    open = read::open_payout_page(
                        connection,
                        start,
                        through,
                        read::MAX_OPEN_PAYOUT_CHECKS,
                    )?;
                }
                Ok((start, through, open))
            })
            .await?;
        {
            let mut state = self
                .unresolved
                .lock()
                .map_err(|_| anyhow::anyhow!("payout review state is unavailable"))?;
            state.after = start;
            state.through = through;
        }
        let started = Instant::now();
        for payout in open {
            if *self.shutdown.borrow() {
                return Ok(1);
            }
            let id = payout.id;
            match payout.status {
                PayoutStatus::Prepared => {
                    let recipients: Vec<(String, u64)> = payout
                        .items
                        .iter()
                        .map(|(_, address, amount)| (address.clone(), *amount))
                        .collect();
                    let miners: Vec<u32> = payout.items.iter().map(|(miner, ..)| *miner).collect();
                    self.send(payout.id, &payout.request_id, &recipients, &miners)
                        .await;
                }
                PayoutStatus::Sent => {
                    if let Some(transaction) = &payout.transaction_id {
                        self.follow(payout.id, transaction, payout.valid_until)
                            .await;
                    }
                }
                PayoutStatus::Paid | PayoutStatus::Failed => {}
            }
            // Advance after the current operation completes.
            self.unresolved
                .lock()
                .map_err(|_| anyhow::anyhow!("payout review state is unavailable"))?
                .after = id;
            // Yield between RPCs while letting each request finish.
            if started.elapsed() >= std::time::Duration::from_secs(10) {
                break;
            }
        }
        // Any prepared payout blocks a new round, including rows on later pages.
        Ok(usize::from(
            self.reader.read(read::has_prepared_payouts).await?,
        ))
    }

    /// Pay the largest balances within the available output budget.
    async fn round(&self) -> Result<()> {
        let limits = self
            .node
            .transaction_manifest()
            .await
            .context("cannot read the node's transaction limits")?
            .transaction_limits;
        let inputs = limits.max_inputs as usize;
        let size = self
            .settings
            .batch_size
            .min((limits.max_outputs as usize).saturating_sub(1).max(1));
        let subtract = self.settings.fee_payer == FeePayer::Miners;
        let mut reserve = if subtract { 0 } else { FEE_RESERVE };
        for sent in 0..self.settings.batches {
            let payable = self
                .reader
                .read({
                    let (minimum, limit) = (self.settings.minimum, size * self.settings.batches);
                    move |connection| read::payable(connection, minimum, limit)
                })
                .await?;
            let (payable, refused) = payable_to(payable, self.network, &self.pool_address);
            if sent == 0 && !refused.is_empty() {
                tracing::error!(
                    "{} balances have an address this pool cannot pay and stay unpaid: {}",
                    refused.len(),
                    refused.join(", ")
                );
            }
            if payable.is_empty() {
                return Ok(());
            }
            let batch = &payable[..payable.len().min(size)];
            let available = self
                .wallet
                .balance()
                .await
                .context("cannot read the pool wallet")?
                .available;
            let coverable = self
                .wallet
                .largest_outputs(inputs)
                .await
                .unwrap_or(available)
                .min(available);
            let mut items = fit(
                batch,
                coverable.saturating_sub(reserve),
                self.settings.minimum,
            );
            if items.is_empty() {
                tracing::info!(
                    "the pool wallet has {} TRAI available; the next payout waits for more",
                    format_trai(available)
                );
                return Ok(());
            }
            let fee = match self.quote(&items, subtract).await {
                Some(fee) => fee,
                None => return Ok(()),
            };
            if !subtract {
                reserve = fee;
                items = fit(&items, coverable.saturating_sub(fee), self.settings.minimum);
                if items.is_empty() {
                    return Ok(());
                }
            }
            if self.settings.fee_max.is_some_and(|max| fee > max) {
                tracing::warn!(
                    "a payout would cost {} TRAI, above --payout-fee-max; trying again next round",
                    format_trai(fee)
                );
                return Ok(());
            }
            let recipients: Vec<(String, u64)> = items
                .iter()
                .map(|(_, address, amount)| (address.clone(), *amount))
                .collect();
            let miners: Vec<u32> = items.iter().map(|(miner, ..)| *miner).collect();
            let prefix = self.request_prefix.clone();
            let entries: Vec<(u32, u64)> = items
                .iter()
                .map(|(miner, _, amount)| (*miner, *amount))
                .collect();
            let (id, request_id) = self
                .writer
                .call(|done| Op::PayoutCreate {
                    request_prefix: prefix,
                    items: entries,
                    at: now_millis(),
                    done,
                })
                .await?;
            if self.send(id, &request_id, &recipients, &miners).await != Sending::Sent {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Quote the fee before the daemon signs.
    async fn quote(&self, items: &[(u32, String, u64)], subtract: bool) -> Option<u64> {
        let recipients: Vec<(String, u64)> = items
            .iter()
            .map(|(_, address, amount)| (address.clone(), *amount))
            .collect();
        match self
            .wallet
            .quote_send_many(&recipients, &self.settings.fee, subtract)
            .await
        {
            Ok(fee) => Some(fee),
            Err(error) => {
                tracing::warn!("the wallet daemon cannot plan a payout yet: {error}");
                self.compound().await;
                None
            }
        }
    }

    /// Preserve the reservation and request ID after any send error.
    async fn send(
        &self,
        id: i64,
        request_id: &str,
        recipients: &[(String, u64)],
        miners: &[u32],
    ) -> Sending {
        let Some(total) = recipients
            .iter()
            .try_fold(0_u64, |sum, (_, amount)| sum.checked_add(*amount))
        else {
            tracing::warn!("payout {id} remains prepared: its amount total overflows");
            return Sending::Unknown;
        };
        if i64::try_from(total).is_err() {
            tracing::warn!(
                "payout {id} remains prepared: its amount total exceeds exact storage capacity"
            );
            return Sending::Unknown;
        }
        let scope = match self.wallet.payment_scope(self.network, self.chain_id).await {
            Ok(scope) => scope,
            Err(error) => {
                tracing::warn!(
                    "payout {id} remains prepared: cannot bind this attempt to a durable daemon archive: {error}"
                );
                return Sending::Unknown;
            }
        };
        let recovery_only = match self
            .writer
            .call(|done| Op::PayoutAttempt {
                id,
                request_id: request_id.to_owned(),
                scope: scope.clone(),
                done,
            })
            .await
        {
            Ok(recovery_only) => recovery_only,
            Err(error) => {
                tracing::warn!("payout {id} remains prepared for review: {error:#}");
                return Sending::Unknown;
            }
        };
        let subtract = self.settings.fee_payer == FeePayer::Miners;
        match self
            .wallet
            .send_many_bound(
                recipients,
                &self.settings.fee,
                request_id,
                subtract,
                &scope,
                recovery_only,
                self.settings.fee_max,
            )
            .await
        {
            Ok(receipt) => {
                let valid_until = Some(receipt.valid_until);
                let fee_shares = if subtract {
                    fee_shares(receipt.fee, miners)
                } else {
                    Vec::new()
                };
                let recorded = self
                    .writer
                    .call(|done| Op::PayoutSent {
                        id,
                        transaction: receipt.transaction.clone(),
                        fee: receipt.fee,
                        valid_until,
                        fee_shares,
                        at: now_millis(),
                        done,
                    })
                    .await;
                if let Err(error) = recorded {
                    tracing::error!("payout {id} was sent but not recorded: {error:#}");
                    return Sending::Unknown;
                }
                tracing::info!(
                    "payout {id}: {} TRAI to {} miners, fee {} TRAI, transaction {}{}",
                    format_trai(total),
                    recipients.len(),
                    format_trai(receipt.fee),
                    &receipt.transaction[..receipt.transaction.len().min(16)],
                    if receipt.duplicate {
                        " (sent before)"
                    } else {
                        ""
                    }
                );
                Sending::Sent
            }
            Err(WalletError::NeverSigned(receipt)) => {
                let recorded = self
                    .writer
                    .call(|done| Op::PayoutNeverSigned {
                        id,
                        request_id: receipt.request_id().to_owned(),
                        scope: receipt.scope().clone(),
                        at: now_millis(),
                        done,
                    })
                    .await;
                if let Err(error) = recorded {
                    tracing::warn!(
                        "payout {id} remains reserved: budget outcome was not durably recorded: {error:#}"
                    );
                    return Sending::Unknown;
                }
                tracing::info!(
                    "payout {id}: {receipt}; its unpaid amount is available for a later round"
                );
                Sending::NeverSigned
            }
            Err(error) => {
                let state = if error.outcome_unknown() {
                    "may have been sent"
                } else {
                    "was not confirmed"
                };
                tracing::warn!(
                    "payout {id} {state}; asking again under the same request id: {error}"
                );
                Sending::Unknown
            }
        }
    }

    async fn follow(&self, id: i64, transaction: &str, valid_until: Option<u64>) {
        let before = match self.node.settlement_head().await {
            Ok(head) if head.chain_id == self.chain_id.to_hex() => head,
            _ => return,
        };
        let observed = match self.node.payout_status(transaction).await {
            Ok(observed) => observed,
            Err(error) => {
                tracing::debug!("cannot ask the node about payout {id}: {error}");
                self.review(id, transaction);
                return;
            }
        };
        let after = match self.node.settlement_head().await {
            Ok(head) if head.chain_id == before.chain_id => head,
            _ => return,
        };
        let terminal_height = observed
            .status
            .as_ref()
            .and_then(|status| status.finalized_cut.map(|cut| cut.height));
        if after.height < before.height
            || (after.height == before.height && after != before)
            || observed
                .authenticated_height
                .is_some_and(|height| height < before.height)
            || terminal_height.is_some_and(|height| {
                height == 0
                    || height > after.height
                    || observed
                        .authenticated_height
                        .is_some_and(|served| height > served)
            })
        {
            self.review(id, transaction);
            return;
        }
        match verdict(observed.status, after.height, valid_until) {
            Verdict::Paid(cut) => {
                tracing::info!("payout {id} finalized at cut {cut}");
                self.close(id, true, None).await;
            }
            Verdict::Failed(reason) => {
                tracing::warn!("payout {id} failed ({reason}); its amounts return to the balances");
                self.close(id, false, Some(reason)).await;
            }
            Verdict::Expired => self.review(id, transaction),
            Verdict::Open => {}
        }
    }

    /// Keep unresolved sent payouts reserved after unknown status or expiry.
    fn review(&self, id: i64, transaction: &str) {
        if self.unresolved.lock().is_ok_and(|mut seen| seen.insert(id)) {
            tracing::warn!(
                "payout {id} remains reserved for review: retain its original request ID and transaction {transaction}; an unknown or inconsistent status cannot authorize another payment"
            );
        }
    }

    async fn close(&self, id: i64, paid: bool, error: Option<String>) {
        let closed = self
            .writer
            .call(|done| Op::PayoutClosed {
                id,
                paid,
                error,
                at: now_millis(),
                done,
            })
            .await;
        if let Err(error) = closed {
            tracing::error!("cannot close payout {id}: {error:#}");
        } else if let Ok(mut seen) = self.unresolved.lock() {
            seen.remove(&id);
        }
    }

    /// Cap concurrent compound transactions at `--compound-open`.
    async fn compound(&self) {
        let status = match self.wallet.status().await {
            Ok(status) => status,
            Err(error) => {
                tracing::debug!("cannot read the wallet status: {error}");
                return;
            }
        };
        let outputs = status.outputs;
        let open = status.pending_compounds.unwrap_or(status.pending_sends);
        if outputs <= self.settings.compound_above || open >= self.settings.compound_open {
            return;
        }
        match self.wallet.compound(&self.settings.fee).await {
            Ok(sent) => tracing::info!(
                "compounding {outputs} wallet outputs in {sent} more transactions, {open} were open"
            ),
            Err(WalletError::Rpc { message, .. }) => {
                tracing::debug!("the wallet daemon did not compound: {message}");
            }
            Err(error) => tracing::debug!("cannot compound: {error}"),
        }
    }
}

/// Require a terminal record before settling a sent payout.
pub fn verdict(
    status: Option<TransactionStatus>,
    height: u64,
    valid_until: Option<u64>,
) -> Verdict {
    match status {
        Some(status) if status.lifecycle == "finalized" => status
            .finalized_cut
            .filter(|cut| cut.height > 0)
            .map_or(Verdict::Open, |cut| Verdict::Paid(cut.height)),
        Some(status) if status.failed_for_good() => {
            Verdict::Failed(status.failure.unwrap_or_else(|| "failed".into()))
        }
        Some(status) if status.lifecycle != "failed" => Verdict::Open,
        _ if valid_until.is_some_and(|until| height > until.saturating_add(VALIDITY_GRACE)) => {
            Verdict::Expired
        }
        _ => Verdict::Open,
    }
}

pub fn payable_to(
    payable: Vec<(u32, String, u64)>,
    network: Network,
    pool_address: &str,
) -> (Vec<(u32, String, u64)>, Vec<String>) {
    let mut refused = Vec::new();
    let payable = payable
        .into_iter()
        .filter(|(_, address, _)| {
            let valid = address != pool_address && parse_address(address, network).is_ok();
            if !valid {
                refused.push(format!("{address:?}"));
            }
            valid
        })
        .collect();
    (payable, refused)
}

/// Fill the batch in balance order, allowing a partial last payment.
pub fn fit(batch: &[(u32, String, u64)], room: u64, minimum: u64) -> Vec<(u32, String, u64)> {
    let mut left = room;
    let mut items = Vec::new();
    for (miner, address, unpaid) in batch {
        let amount = (*unpaid).min(left);
        if amount < minimum || amount == 0 {
            break;
        }
        left -= amount;
        items.push((*miner, address.clone(), amount));
    }
    items
}

/// Split a deducted fee equally and assign the remainder to the first recipients.
pub fn fee_shares(fee: u64, miners: &[u32]) -> Vec<(u32, u64)> {
    let count = miners.len() as u64;
    if count == 0 {
        return Vec::new();
    }
    let (share, mut remainder) = (fee / count, fee % count);
    miners
        .iter()
        .map(|miner| {
            let extra = u64::from(remainder > 0);
            remainder = remainder.saturating_sub(1);
            (*miner, share + extra)
        })
        .collect()
}

#[cfg(test)]
#[path = "tests/payouts.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/payout_uncertain.rs"]
mod uncertain_tests;
