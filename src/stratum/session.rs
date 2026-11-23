use super::{
    admission::{Judge, Lease, SESSION_SHARE_BURST},
    jobs::{Job, JobBook},
    protocol::{
        ClientMessage, JobNotice, Protocol, RejectReason, ServerMessage, SubscribeResult,
        printable, requested_difficulty, split_login,
    },
    vardiff::Vardiff,
    wire::{Inbound, Wire},
};
use crate::{
    blocks::short,
    chain::{Hash384, domain_hash, fresh_extranonce, parse_address},
    config::Mode,
    hub::Hub,
    pow::target::{difficulty_of, target_for},
    rate::TokenBucket,
    stats::WorkerKey,
    work::{TEMPLATE_LEASE, Work},
};
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
    time::{Instant as TokioInstant, MissedTickBehavior, interval, sleep_until, timeout},
};

const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const GOODBYE_TIMEOUT: Duration = Duration::from_secs(1);
const WORK_WAIT: Duration = Duration::from_secs(20);
const TICK: Duration = Duration::from_secs(1);
const MAX_AGENT_CHARS: usize = 64;
const MESSAGES_PER_MINUTE: u64 = 6_000;
const MESSAGE_BURST: u32 = 200;
const JOB_DOMAIN: &[u8] = b"TRAI_POOL_JOB_V1";

type PendingWork = (oneshot::Receiver<Result<Arc<Work>, String>>, TokioInstant);

pub(super) struct Session<'a, S> {
    wire: Wire<S>,
    pub(super) protocol: Protocol,
    pub(super) hub: &'a Arc<Hub>,
    pub(super) peer: SocketAddr,
    pub(super) mode: Mode,
    pub(super) prefix: [u8; 2],
    agent: String,
    tag: String,
    subscribed: bool,
    pub(super) miner: Option<Miner>,
    pub(super) jobs: JobBook,
    work: Option<Arc<Work>>,
    work_since: Instant,
    current_job: Option<Hash384>,
    pending_work: Option<PendingWork>,
    /// A session can submit one block per empty template.
    pub(super) spent_empty: Option<Hash384>,
    pub(super) vardiff: Vardiff,
    pub(super) shares: TokenBucket,
    requests: TokenBucket,
    pub(super) judge: Judge,
    /// Valid work exempts the session from the shared verification budget.
    pub(super) proven: bool,
    ended: Option<String>,
}

/// Assign database IDs on the first accepted share.
pub(super) struct Miner {
    pub(super) key: WorkerKey,
    pub(super) ids: Option<(u32, crate::db::WorkerId)>,
    label: String,
}

pub(super) async fn run<S>(
    wire: Wire<S>,
    peer: SocketAddr,
    mode: Mode,
    protocol: Protocol,
    lease: Lease,
    hub: Arc<Hub>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = Session::new(wire, peer, mode, protocol, lease.prefix(), &hub);
    hub.stats.session_opened(protocol);
    tracing::debug!(
        "{peer} connected to the {} port over {}",
        mode.label(),
        protocol.label()
    );
    session.serve().await;
    hub.stats.session_closed(protocol);
    drop(lease);
}

impl<'a, S> Session<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn new(
        wire: Wire<S>,
        peer: SocketAddr,
        mode: Mode,
        protocol: Protocol,
        prefix: [u8; 2],
        hub: &'a Arc<Hub>,
    ) -> Self {
        let now = Instant::now();
        let vardiff = &hub.config.vardiff;
        let shares_per_minute = (60 / vardiff.share_interval).max(1);
        Self {
            wire,
            protocol,
            hub,
            peer,
            mode,
            prefix,
            agent: String::new(),
            tag: String::new(),
            subscribed: false,
            miner: None,
            jobs: JobBook::default(),
            work: None,
            work_since: now,
            current_job: None,
            pending_work: None,
            spent_empty: None,
            vardiff: Vardiff::new(vardiff.clone(), now),
            shares: TokenBucket::per_minute(shares_per_minute, SESSION_SHARE_BURST)
                .expect("a positive share budget"),
            requests: TokenBucket::per_minute(MESSAGES_PER_MINUTE, MESSAGE_BURST)
                .expect("a positive request budget"),
            judge: Judge::default(),
            proven: false,
            ended: None,
        }
    }

    async fn serve(&mut self) {
        let mut tick = interval(TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let opened = TokioInstant::now();
        let mut last_inbound = opened;
        let mut ticked = Instant::now();
        let mut shutdown = self.hub.shutdown.clone();
        loop {
            let deadline = if self.ready() {
                last_inbound + self.hub.config.idle_timeout
            } else {
                opened + AUTH_TIMEOUT
            };
            let step = tokio::select! {
                inbound = self.wire.next() => match inbound {
                    Some(Ok(inbound)) => {
                        last_inbound = TokioInstant::now();
                        self.receive(inbound).await
                    }
                    Some(Err(error)) => Err(error),
                    None => break,
                },
                work = next_work(&mut self.pending_work) => self.assign(work).await,
                _ = tick.tick() => {
                    // Drain queued lines after a delayed timer tick.
                    let now = Instant::now();
                    self.requests.stalled(now.duration_since(ticked).saturating_sub(TICK));
                    ticked = now;
                    self.tick().await
                }
                _ = sleep_until(deadline) => {
                    self.ended = Some(if self.ready() { "idle" } else { "no login in time" }.into());
                    break;
                }
                _ = shutdown.changed() => {
                    let _ = timeout(GOODBYE_TIMEOUT, self.wire.goodbye()).await;
                    self.ended = Some("the pool stops".into());
                    break;
                }
            };
            if let Err(error) = step {
                self.ended = Some(format!("{error:#}"));
                break;
            }
        }
    }

    async fn receive(&mut self, inbound: Inbound) -> Result<()> {
        let message = match inbound {
            Inbound::Junk(reason) => return Err(self.junk(&reason)),
            _ if !self.requests.allow() => return Err(self.junk("sent requests too fast")),
            Inbound::Control => return Ok(()),
            Inbound::Line(line) if line.trim().is_empty() => return Ok(()),
            Inbound::Line(line) => match ClientMessage::parse(&line) {
                Ok(message) => message,
                Err(_) => return Err(self.junk("sent a malformed request")),
            },
            Inbound::Message(message) => message,
        };
        match message {
            ClientMessage::Subscribe {
                id,
                agent,
                tag,
                pow_algorithm_id,
                spec_revision,
            } => {
                self.subscribe(id, &agent, tag, pow_algorithm_id, spec_revision)
                    .await
            }
            ClientMessage::Authorize {
                id,
                login,
                password,
            } => self.authorize(id, &login, &password).await,
            ClientMessage::Submit { id, job_id, nonce } if self.ready() => {
                self.submit(id, &job_id, &nonce).await
            }
            ClientMessage::Submit { id, .. } => {
                self.send(ServerMessage::reject(id, RejectReason::Unauthorized))
                    .await
            }
            ClientMessage::Unknown { id, method } => {
                tracing::debug!(
                    "{} sent unsupported method {}",
                    self.peer,
                    printable(&method, 64)
                );
                self.send(ServerMessage::reject(id, RejectReason::Unsupported))
                    .await
            }
        }
    }

    async fn subscribe(
        &mut self,
        id: Value,
        agent: &str,
        tag: String,
        pow_algorithm_id: u8,
        spec_revision: u32,
    ) -> Result<()> {
        if pow_algorithm_id != crate::pow::tenq::ALGORITHM_ID
            || spec_revision != crate::pow::tenq::SPEC_REVISION
        {
            return self
                .send(ServerMessage::reject(id, RejectReason::Unsupported))
                .await;
        }
        if crate::chain::check_tag(&tag).is_err() {
            self.strike();
            return self
                .send(ServerMessage::reject(id, RejectReason::InvalidTag))
                .await;
        }
        if !self.subscribed {
            self.subscribed = true;
            self.tag = tag;
            self.agent = printable(agent, MAX_AGENT_CHARS);
        }
        let result = SubscribeResult {
            protocol: self.protocol.id().to_owned(),
            nonce_prefix: hex::encode(self.prefix),
            pool_address: self.hub.pool_address.clone(),
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            chain_id: self.hub.chain_id.to_hex(),
        };
        self.send(ServerMessage::reply(id, serde_json::to_value(result)?))
            .await?;
        self.request_work();
        Ok(())
    }

    async fn authorize(&mut self, id: Value, login: &str, password: &str) -> Result<()> {
        if !self.subscribed || self.miner.is_some() {
            return self
                .send(ServerMessage::reject(id, RejectReason::Unauthorized))
                .await;
        }
        let (address, worker) = split_login(login.trim());
        if parse_address(address, self.hub.network).is_err() {
            self.strike();
            tracing::debug!(
                "{} logged in without a valid {} address",
                self.peer,
                self.hub.network.name()
            );
            return self
                .send(ServerMessage::reject(id, RejectReason::InvalidAddress))
                .await;
        }
        if address == self.hub.pool_address {
            tracing::debug!("{} logged in with the pool's own address", self.peer);
            return self
                .send(ServerMessage::reject(id, RejectReason::InvalidAddress))
                .await;
        }
        if let Some(difficulty) = requested_difficulty(password) {
            self.vardiff.fix(difficulty);
        }
        let key = WorkerKey {
            address: address.to_owned(),
            name: worker,
            mode: self.mode,
        };
        let label = format!(
            "{}.{} ({})",
            short(&key.address),
            key.name,
            self.mode.label()
        );
        let agent = match self.agent.as_str() {
            "" => String::new(),
            agent => format!(" ({agent})"),
        };
        tracing::info!(
            "worker {label} connected from {} over {}{agent}",
            self.peer.ip(),
            self.protocol.label()
        );
        self.hub
            .stats
            .connected(&key, &self.agent, self.peer.ip(), self.protocol);
        self.hub
            .stats
            .difficulty(&key, self.vardiff.difficulty(), self.vardiff.is_fixed());
        self.miner = Some(Miner {
            key,
            ids: None,
            label,
        });
        self.send(ServerMessage::reply(id, Value::Bool(true)))
            .await?;
        self.request_work();
        Ok(())
    }

    pub(super) const fn ready(&self) -> bool {
        self.subscribed && self.miner.is_some()
    }

    /// Refresh work on a new cut or an expired template lease.
    pub(super) fn request_work(&mut self) {
        if !self.ready() || self.pending_work.is_some() {
            return;
        }
        if let Some(work) = &self.work {
            let refresh = if work.template.transactions > 0 {
                self.work_since.elapsed() >= TEMPLATE_LEASE
            } else {
                *self.hub.heights.borrow() > work.template.height()
            };
            if !refresh {
                return;
            }
        }
        if let Some(answer) = self.hub.work.request(self.spent_empty) {
            self.pending_work = Some((answer, TokioInstant::now() + WORK_WAIT));
        }
    }

    async fn assign(&mut self, work: Result<Arc<Work>, String>) -> Result<()> {
        self.pending_work = None;
        let work = match work {
            Ok(work) => work,
            Err(error) => {
                tracing::debug!("{} is waiting for work: {error}", self.peer);
                return Ok(());
            }
        };
        if self.spent_empty == Some(work.template.id) {
            return Ok(());
        }
        if self
            .work
            .as_ref()
            .is_some_and(|current| current.template.id == work.template.id)
        {
            self.work_since = Instant::now();
            return Ok(());
        }
        self.issue(&work).await?;
        self.work = Some(work);
        self.work_since = Instant::now();
        Ok(())
    }

    async fn tick(&mut self) -> Result<()> {
        if !self.ready() {
            return Ok(());
        }
        self.jobs.prune(self.hub.registration_height());
        if self.current_job.is_some_and(|id| !self.jobs.contains(id)) {
            self.current_job = None;
            self.work = None;
        }
        if self.vardiff.retarget(Instant::now()) {
            self.reissue().await?;
        }
        self.request_work();
        Ok(())
    }

    async fn issue(&mut self, work: &Arc<Work>) -> Result<()> {
        let Some(miner) = &self.miner else {
            return Ok(());
        };
        let template = &work.template;
        self.vardiff.set_ceiling(template.network_difficulty());
        let tag = self.hub.config.tag.as_deref().unwrap_or(&self.tag);
        let extranonce = fresh_extranonce(tag);
        let header = template.header(&extranonce, &[0; 32]);
        let issue = self.jobs.next_issue().to_be_bytes();
        let header_id = crate::pow::template::block_id(&header);
        let id = domain_hash(JOB_DOMAIN, &[&template.id.0, &header_id.0, &issue]);
        let share_target = target_for(u128::from(self.vardiff.difficulty())).max(template.target);
        let weight = u64::try_from(difficulty_of(&share_target)).unwrap_or(u64::MAX);
        self.hub
            .stats
            .difficulty(&miner.key, weight, self.vardiff.is_fixed());
        let notice = JobNotice {
            job_id: id.to_hex(),
            header: hex::encode(header),
            share_target: hex::encode(share_target),
            target: hex::encode(template.target),
            registration_height: template.registration_height,
            network: self.hub.network.name().to_owned(),
            clean: true,
            // Consume the template after this session finds a block.
            single_block: true,
        };
        self.jobs.insert(Job {
            id,
            work: Arc::clone(work),
            extranonce,
            share_target,
            weight,
        });
        self.current_job = Some(id);
        self.send(ServerMessage::SetTarget(hex::encode(share_target)))
            .await?;
        self.send(ServerMessage::Notify(notice)).await
    }

    pub(super) async fn reissue(&mut self) -> Result<()> {
        if let Some(miner) = &self.miner {
            tracing::debug!(
                "worker {} now mines at difficulty {}",
                miner.label,
                self.vardiff.difficulty()
            );
        }
        if let Some(work) = self.work.clone() {
            self.issue(&work).await?;
        }
        Ok(())
    }

    /// Close jobs whose body was included in a block.
    pub(super) fn retire(&mut self, template: Hash384) -> bool {
        self.jobs.retire_template(template);
        let current = self
            .work
            .as_ref()
            .is_some_and(|work| work.template.id == template);
        if current {
            self.work = None;
            self.current_job = None;
            self.request_work();
        }
        current
    }

    /// Use shared empty work until a leased template arrives.
    /// Skip it if this session already used that template.
    pub(super) async fn fill(&mut self) -> Result<()> {
        let Some(filler) = self.hub.work.filler() else {
            return Ok(());
        };
        if self.work.is_some()
            || filler.template.registration_window_end < self.hub.registration_height()
            || self.spent_empty == Some(filler.template.id)
        {
            return Ok(());
        }
        self.issue(&filler).await?;
        self.work = Some(filler);
        self.work_since = Instant::now();
        Ok(())
    }

    pub(super) fn strike(&self) {
        if self.hub.admission.strike(self.peer.ip()) {
            tracing::info!("{} is banned for repeated bad requests", self.peer.ip());
        }
    }

    pub(super) fn junk(&self, reason: &str) -> anyhow::Error {
        self.strike();
        anyhow!("{reason}")
    }

    pub(super) async fn send(&mut self, message: ServerMessage) -> Result<()> {
        timeout(WRITE_TIMEOUT, self.wire.send(message))
            .await
            .map_err(|_| anyhow!("write timed out"))??;
        Ok(())
    }
}

impl<S> Drop for Session<'_, S> {
    fn drop(&mut self) {
        let reason = self
            .ended
            .as_deref()
            .map(|reason| format!(": {reason}"))
            .unwrap_or_default();
        match &self.miner {
            Some(miner) => {
                self.hub.stats.disconnected(&miner.key);
                tracing::info!("worker {} disconnected{reason}", miner.label);
            }
            None => tracing::debug!("{} disconnected{reason}", self.peer),
        }
    }
}

async fn next_work(pending: &mut Option<PendingWork>) -> Result<Arc<Work>, String> {
    match pending {
        Some((answer, deadline)) => match tokio::time::timeout_at(*deadline, answer).await {
            Ok(Ok(work)) => work,
            Ok(Err(_)) => Err("the work source stopped".into()),
            Err(_) => Err("no work from the node in time".into()),
        },
        None => std::future::pending().await,
    }
}
