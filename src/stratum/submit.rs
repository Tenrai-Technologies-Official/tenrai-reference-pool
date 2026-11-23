use super::{
    jobs::Job,
    protocol::{RejectReason, ServerMessage},
    session::Session,
};
use crate::{
    chain::{Hash384, domain_hash},
    db::{
        accepted::{
            AcceptanceError, AcceptedWork, FoundWork, ReplayReceipt,
            pending::{Refused, WorkLease},
        },
        now_millis,
    },
    pow::{
        target::meets,
        template::{HEADER_BYTES, preimage},
    },
};
use anyhow::{Result, bail};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::timeout,
};

const VERIFY_WAIT: Duration = Duration::from_secs(2);

struct Accepted {
    job: Job,
    header: [u8; HEADER_BYTES],
    block: bool,
    pending: Arc<WorkLease>,
}

impl<S> Session<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(super) async fn submit(&mut self, id: Value, job_id: &str, nonce: &str) -> Result<()> {
        let (checked, retargeted) = self.check(job_id, nonce).await;
        let Some(key) = self.miner.as_ref().map(|miner| miner.key.clone()) else {
            return Ok(());
        };
        match checked {
            Ok(accepted) => {
                let template = &accepted.job.work.template;
                let found = accepted.block.then(|| FoundWork {
                    header: accepted.header,
                    body_hex: template.body_hex.clone(),
                    height: template.height(),
                    window_end: template.registration_window_end,
                    network_difficulty: template.network_difficulty(),
                    window_work: self
                        .hub
                        .config
                        .pplns
                        .window_work(template.network_difficulty()),
                    fee_bps: self.hub.config.fees.bps(key.mode),
                });
                let receipt = match self
                    .hub
                    .record_share(
                        AcceptedWork {
                            fingerprint: domain_hash(
                                b"TRAI_POOL_ACCEPTED_WORK_V1",
                                &[&accepted.header],
                            ),
                            header: accepted.header,
                            job_id: accepted.job.id,
                            window_end: template.registration_window_end,
                            key: key.clone(),
                            time: now_millis(),
                            weight: accepted.job.weight,
                            lookback_millis: u64::try_from(
                                self.hub.config.pplns.lookback.as_millis(),
                            )
                            .unwrap_or(u64::MAX),
                            source: None,
                            found,
                        },
                        accepted.pending,
                    )
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(AcceptanceError::Busy) => {
                        let nonce = accepted.header[248..280]
                            .try_into()
                            .expect("fixed work header");
                        self.jobs.release(accepted.job.id, nonce);
                        self.send(ServerMessage::reject(id, RejectReason::RateLimited))
                            .await?;
                        return Ok(());
                    }
                    Err(AcceptanceError::Unknown(error)) => {
                        bail!("share outcome unknown; reconnect and retry the same proof: {error}")
                    }
                };
                // A duplicate proof stays assigned to its original receipt.
                if receipt.fresh
                    && let Some(miner) = &mut self.miner
                {
                    miner.ids = Some((receipt.miner, receipt.worker));
                }
                let mut retired = false;
                if accepted.block {
                    let (template_id, leased) = (template.id, template.transactions > 0);
                    if !leased {
                        self.spent_empty = Some(template_id);
                    }
                    retired = self.retire(template_id);
                }
                self.hub.stats.protocol_share(self.protocol, None);
                self.send(ServerMessage::reply(id, Value::Bool(true)))
                    .await?;
                if retired {
                    self.fill().await?;
                }
            }
            Err(reason) => {
                if matches!(
                    reason,
                    RejectReason::StaleJob | RejectReason::Duplicate | RejectReason::Malformed
                ) && let (Some(job), Ok(bytes)) = (Hash384::from_hex(job_id), hex::decode(nonce))
                    && let Ok(nonce) = <[u8; 32]>::try_from(bytes.as_slice())
                {
                    match self
                        .hub
                        .writer
                        .replay_work(job, nonce, key.address.clone())
                        .await
                    {
                        Ok(ReplayReceipt::Committed(_)) => {
                            self.send(ServerMessage::reply(id, Value::Bool(true)))
                                .await?;
                            return Ok(());
                        }
                        Ok(ReplayReceipt::Absent) => {}
                        Ok(ReplayReceipt::UnknownPending) => bail!(
                            "the original proof is still physically pending; share outcome unknown"
                        ),
                        Err(error) => {
                            bail!("receipt lookup unavailable; share outcome unknown: {error:#}")
                        }
                    }
                }
                let stale = reason == RejectReason::StaleJob;
                self.hub
                    .stats
                    .rejected(Some(&key), self.mode, reason.name(), stale);
                self.hub.stats.protocol_share(self.protocol, Some(stale));
                tracing::debug!(
                    "share from {}.{} rejected: {}",
                    key.address,
                    key.name,
                    reason.message()
                );
                let judged = !matches!(reason, RejectReason::StaleJob | RejectReason::RateLimited);
                if judged
                    && self.hub.admission.bans_enabled()
                    && self.judge.record(false)
                    && self.hub.admission.ban(self.peer.ip())
                {
                    self.send(ServerMessage::reject(id, RejectReason::Banned))
                        .await?;
                    bail!("banned for invalid shares");
                }
                self.send(ServerMessage::reject(id, reason)).await?;
            }
        }
        if retargeted {
            self.reissue().await?;
        }
        Ok(())
    }

    async fn check(&mut self, job_id: &str, nonce: &str) -> (Result<Accepted, RejectReason>, bool) {
        let Some(nonce) = parse_nonce(nonce, &self.prefix) else {
            return (Err(RejectReason::Malformed), false);
        };
        let Some(job_id) = Hash384::from_hex(job_id) else {
            return (Err(RejectReason::StaleJob), false);
        };
        let pending = match self.hub.writer.claim_work(job_id, nonce) {
            Ok(pending) => pending,
            Err(Refused::Busy) => return (Err(RejectReason::RateLimited), false),
            Err(Refused::Pending) => return (Err(RejectReason::Duplicate), false),
        };
        let job = match self
            .jobs
            .claim(job_id, nonce, self.hub.registration_height())
        {
            Ok(job) => job,
            Err(reason) => return (Err(reason), false),
        };
        let retargeted = self.vardiff.record(Instant::now(), job.weight);
        let verified = self.verify(job, nonce, pending).await;
        match &verified {
            Ok(_) if self.hub.admission.bans_enabled() => {
                self.judge.record(true);
            }
            Err(RejectReason::RateLimited) => self.jobs.release(job_id, nonce),
            _ => {}
        }
        (verified, retargeted)
    }

    /// Verify on worker threads and refund budget for valid shares.
    async fn verify(
        &mut self,
        job: Job,
        nonce: [u8; 32],
        pending: Arc<WorkLease>,
    ) -> Result<Accepted, RejectReason> {
        let shared = !self.proven;
        if !self.take_budget(shared) {
            return Err(RejectReason::RateLimited);
        }
        let permit = match timeout(VERIFY_WAIT, Arc::clone(&self.hub.verify).acquire_owned()).await
        {
            Ok(Ok(permit)) => permit,
            _ => {
                self.refund_budget(shared);
                return Err(RejectReason::RateLimited);
            }
        };
        let physical_pending = Arc::clone(&pending);
        let owner = self
            .hub
            .writer
            .process_lease()
            .map_err(|_| RejectReason::RateLimited)?;
        let evaluated = tokio::task::spawn_blocking(move || {
            let _pending = physical_pending;
            let _permit = permit;
            let header = job.work.template.header(&job.extranonce, &nonce);
            let started = Instant::now();
            let value = job.work.hasher.evaluate(&preimage(&header));
            (job, header, value, started.elapsed(), owner)
        })
        .await;
        self.proven = false;
        let Ok((job, header, Some(value), took, _physical_owner)) = evaluated else {
            return Err(RejectReason::Malformed);
        };
        self.hub.budget.checked(took);
        if !meets(&value, &job.share_target) {
            return Err(RejectReason::LowDifficulty);
        }
        self.refund_budget(shared);
        if self.hub.registration_height() > job.work.template.registration_window_end {
            return Err(RejectReason::StaleJob);
        }
        self.proven = true;
        let block = meets(&value, &job.work.template.target);
        Ok(Accepted {
            job,
            header,
            block,
            pending,
        })
    }

    /// Apply the shared budget until the session proves valid work.
    fn take_budget(&mut self, shared: bool) -> bool {
        let ip = self.peer.ip();
        if !self.shares.allow() {
            return false;
        }
        if !shared {
            return true;
        }
        if !self.hub.admission.take_verification(ip) {
            self.shares.refund();
            return false;
        }
        if !self.hub.budget.take() {
            self.shares.refund();
            self.hub.admission.refund_verification(ip);
            return false;
        }
        true
    }

    fn refund_budget(&mut self, shared: bool) {
        self.shares.refund();
        if shared {
            self.hub.admission.refund_verification(self.peer.ip());
            self.hub.budget.refund();
        }
    }
}

/// Decode a 32-byte nonce ending in the session prefix.
fn parse_nonce(text: &str, prefix: &[u8]) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let nonce: [u8; 32] = hex::decode(text).ok()?.try_into().ok()?;
    nonce.ends_with(prefix).then_some(nonce)
}
