#![deny(unsafe_code)]

pub mod accounting;
pub mod address_words;
pub mod api;
pub mod blocks;
pub mod chain;
pub mod cli;
pub mod config;
pub mod console;
pub mod day;
pub mod db;
#[cfg(test)]
#[path = "tests/flow.rs"]
mod flow;
pub mod home;
pub mod hub;
pub mod ledger;
mod lifecycle;
pub mod net;
pub mod node;
pub mod outage;
pub mod payouts;
pub mod pow;
pub mod pplns;
pub mod rate;
pub mod registry;
#[cfg(test)]
#[path = "tests/start.rs"]
mod start;
pub mod stats;
pub mod stratum;
mod tasks;
#[cfg(test)]
mod test_support;
pub mod tls;
pub mod wallet;
pub mod work;

use crate::{
    blocks::Blocks,
    chain::{Hash384, Network, parse_address},
    config::{Config, format_trai},
    console::Tally,
    day::{Day, SavedBlocks},
    db::{read, setting},
    hub::Hub,
    node::{NetworkMining, NodeClient, NodeError, NodeInfo},
    outage::{NodeOutage, Outage, WAIT_REMINDER, waited},
    pplns::ShareLog,
    rate::CheckBudget,
    registry::Registry,
    stats::{BlockCounts, Stats},
    stratum::{admission::Admission, protocol::Protocol},
    wallet::{CheckError, Wallet, WalletError},
    work::{Hashers, WorkSource},
};
use anyhow::{Context, Result, bail};
use rand_core::{OsRng, RngCore};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc, watch},
};
use tokio_rustls::TlsAcceptor;

const WAIT_INTERVAL: Duration = Duration::from_secs(5);
const BUSY_RETRY: Duration = Duration::from_secs(1);
const WALLET_ATTEMPTS: u32 = 3;
const WALLET_RETRY: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(5)
};
const CANDIDATE_WORKERS: usize = 8;

pub async fn run(config: Config) -> Result<()> {
    serve(config, wait_for_stop()).await
}

/// Run until stop and drain the database on shutdown or failure.
pub async fn serve(config: Config, stop: impl Future<Output = ()>) -> Result<()> {
    tracing::info!("tenrai-pool {}", env!("CARGO_PKG_VERSION"));
    let (writer, reader, database) = db::open(&config.db)?;
    let served = serve_open(config, stop, writer.clone(), reader).await;
    let closed = writer.shutdown().await;
    drop(writer);
    let joined = tokio::task::spawn_blocking(move || {
        database
            .join()
            .map_err(|_| anyhow::anyhow!("database writer panicked"))
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|joined| joined);
    served?;
    closed?;
    joined
}

async fn serve_open(
    config: Config,
    stop: impl Future<Output = ()>,
    writer: db::Writer,
    reader: db::Reader,
) -> Result<()> {
    let stored = reader
        .read(|connection| {
            Ok([
                setting(connection, "instance")?,
                setting(connection, "chain_id")?,
                setting(connection, "pool_address")?,
                setting(connection, "cut_height")?,
                setting(connection, "round_work")?,
                setting(connection, "blocks")?,
            ])
        })
        .await?;
    let [
        instance,
        stored_chain,
        stored_address,
        cut_height,
        round_work,
        blocks,
    ] = stored;
    let saved = blocks
        .as_deref()
        .and_then(|text| serde_json::from_str::<SavedBlocks>(text).ok());

    let mut node = NodeClient::new(config.node.clone(), config.node_api.clone())?;
    if let Some(identity) = config.node_identity {
        wait_for_node_trust(&mut node, config.chain_id, identity)
            .await
            .with_context(|| format!("cannot verify the node identity at {}", config.node_api))?;
    }
    let info = wait_for_node(&node).await?;
    let network = Network::parse(&info.network)
        .with_context(|| format!("the node serves an unknown network {}", info.network))?;
    let chain_id =
        Hash384::from_hex(&info.chain_id).context("the node sent an invalid chain ID")?;
    for (pinned, origin) in [
        (Some(config.chain_id), "--chain-id"),
        (
            stored_chain.as_deref().and_then(Hash384::from_hex),
            "the database",
        ),
    ] {
        if let Some(pinned) = pinned
            && pinned != chain_id
        {
            bail!("the node serves chain {chain_id}, but {origin} holds chain {pinned}");
        }
    }
    let rates = wait_for_mining_rates(&node)
        .await
        .with_context(|| format!("cannot read /v1/network/mining from {}", config.node_api))?;
    tracing::info!(
        "node {} {} serves the {} chain {} at height {}, {} work blocks a second",
        config.node,
        info.node_version,
        network.name(),
        &chain_id.to_hex()[..16],
        info.finalized.height,
        rates.work_blocks_per_second
    );

    let wallet = config
        .wallet_token
        .clone()
        .map(|token| Wallet::new(config.wallet.clone(), token))
        .transpose()?;
    let reached = match &wallet {
        Some(wallet) => reach_wallet(&config, wallet, network, chain_id).await?,
        None => None,
    };
    let pool_address = choose_pool_address(
        config.pool_address.as_deref(),
        reached.as_deref(),
        stored_address.as_deref(),
    )?;
    let pool_lock = parse_address(&pool_address, network)
        .context("the pool address is not valid on this network")?;
    let instance = instance.unwrap_or_else(|| {
        let mut bytes = [0_u8; 8];
        OsRng.fill_bytes(&mut bytes);
        hex::encode(bytes)
    });
    for (key, value) in [
        ("instance", instance.clone()),
        ("chain_id", chain_id.to_hex()),
        ("network", network.name().to_owned()),
        ("pool_address", pool_address.clone()),
    ] {
        writer.send(db::Op::Setting { key, value });
    }
    match (&wallet, reached.is_some()) {
        (Some(wallet), true) => {
            let balance = wallet.balance().await.ok();
            tracing::info!(
                "wallet daemon {}: pool address {pool_address}, {} TRAI available in {} outputs",
                config.wallet,
                balance
                    .as_ref()
                    .map_or("?".into(), |balance| format_trai(balance.available)),
                balance.as_ref().map_or(0, |balance| balance.outputs)
            );
        }
        (Some(_), false) => tracing::warn!(
            "pool address {pool_address}; the wallet daemon {} does not answer yet, payouts wait for it",
            config.wallet
        ),
        (None, _) => {
            tracing::info!("pool address {pool_address}; no wallet daemon, payouts are off")
        }
    }

    let day_start = Day::start(db::now_millis());
    let scan = saved.is_none();
    let (open, next_share, scanned) = reader
        .read(move |connection| {
            Ok((
                read::block_counts(connection)?,
                read::next_share_id(connection)?,
                if scan {
                    Some((
                        read::block_counts(connection)?,
                        read::day(connection, day_start)?,
                    ))
                } else {
                    None
                },
            ))
        })
        .await?;
    let registry = Arc::new(Registry::load(reader.clone(), writer.clone()).await?);
    let mut fee_ids = Vec::new();
    for (address, share) in &config.fees.recipients {
        parse_address(address, network).with_context(|| {
            format!("--fee-recipient {address} is not an address of this network")
        })?;
        if *address == pool_address {
            bail!("--fee-recipient {address} is the pool's own address");
        }
        fee_ids.push((registry.miner(address).await?, *share));
    }
    let log = ShareLog::new(next_share);
    tracing::info!(
        "database {}: {} addresses, {} shares in the PPLNS log, {} blocks waiting for their reward",
        config.db.display(),
        registry.miners(),
        log.len(),
        open.iter().map(|entry| entry.1).sum::<u64>()
    );

    let (block_counts, day) = match saved {
        Some(saved) => {
            let mut counts = [BlockCounts::default(); 2];
            for (entry, stored) in counts.iter_mut().zip(open) {
                entry.open = stored.1;
            }
            for (entry, (confirmed, lost)) in counts.iter_mut().zip(saved.closed) {
                entry.confirmed = confirmed;
                entry.lost = lost;
                entry.found = entry.open + confirmed + lost;
            }
            (counts, saved.day)
        }
        None => {
            let (counts, day) = scanned.unwrap_or_default();
            let counts = counts.map(|(found, open, confirmed, lost)| BlockCounts {
                found,
                open,
                confirmed,
                lost,
            });
            (counts, day)
        }
    };
    let stats = Arc::new(Stats::new(
        block_counts,
        parse_round_work(round_work.as_deref()),
        Day::load(day, db::now_millis()),
    ));
    stats.set_network(|known| {
        known.height = info.finalized.height;
        known.block_rate = rates.work_blocks_per_second;
        known.cut_interval_ms = rates.target_cut_interval_millis;
    });
    let hashers = Arc::new(Hashers::new(
        pow::profile_for(network),
        chain_id,
        config.tenq_mode,
        config.verify_threads,
        config.tenq_prefetch,
        writer.process_lease()?,
    ));

    let (heights, height) = watch::channel(info.finalized.height);
    let work = WorkSource::start(
        node.clone(),
        (pool_address.clone(), pool_lock),
        chain_id,
        Arc::clone(&hashers),
        heights.clone(),
        rates.work_blocks_per_second,
        Arc::clone(&stats),
    );
    let (stopping, shutdown) = watch::channel(false);
    let shares_per_minute = (60 / config.vardiff.share_interval).max(1);
    let budget = CheckBudget::new(
        shares_per_minute.saturating_mul(u64::from(config.max_clients)),
        config.verify_threads.saturating_mul(2).clamp(8, 65_536) as u32,
        config.verify_threads,
    )
    .context("the share budget does not fit")?;
    let hub = Arc::new(Hub {
        network,
        chain_id,
        pool_address: pool_address.clone(),
        work,
        heights: height.clone(),
        verify: Arc::new(Semaphore::new(config.verify_threads)),
        budget,
        admission: Admission::new(
            config.max_clients,
            config.max_clients_per_ip,
            shares_per_minute,
            config.ban,
        ),
        stats: Arc::clone(&stats),
        registry,
        pplns: Mutex::new(log),
        writer: writer.clone(),
        blocks: Blocks::new(reader.clone()),
        reader: reader.clone(),
        tally: Arc::new(Tally::default()),
        shutdown: shutdown.clone(),
        config,
    });
    let mut background = lifecycle::Group::default();
    background.extend(blocks::start(
        Arc::clone(&hub),
        node.clone(),
        CANDIDATE_WORKERS,
    ));
    let resume = cut_height.and_then(|value| value.parse::<u64>().ok());
    let (events, receiver) = mpsc::channel(1_024);
    background.push(lifecycle::spawn(
        shutdown.clone(),
        node::events::follow(hub.config.node.clone(), resume, events),
    ));
    let ledger = ledger::Ledger::new(
        Arc::clone(&hub),
        node.clone(),
        heights,
        fee_ids,
        resume.unwrap_or(info.finalized.height),
        wallet.clone(),
    );
    background.push(lifecycle::spawn(shutdown.clone(), ledger.run(receiver)));
    if let Some(wallet) = wallet {
        background.push(lifecycle::spawn(
            shutdown.clone(),
            payouts::Payouts {
                wallet,
                checked: reached.is_some(),
                node: node.clone(),
                reader: reader.clone(),
                writer: writer.clone(),
                settings: hub.config.payouts.clone(),
                network,
                chain_id,
                pool_address: pool_address.clone(),
                request_prefix: format!("pool-{}-", &instance[..instance.len().min(8)]),
                heights: height,
                shutdown: shutdown.clone(),
                unresolved: Default::default(),
            }
            .run(),
        ));
    }
    background.extend(tasks::start(Arc::clone(&hub), node.clone()));

    if let Some(bind) = hub.config.web {
        let api = api::Api::new(Arc::clone(&hub), reader.clone(), Arc::clone(&hashers));
        let (address, handle) = api::serve_tracked(bind, api)
            .await
            .with_context(|| format!("cannot serve the website on {bind}"))?;
        background.push(handle);
        tracing::info!("website and API on http://{address}/");
    }
    listen(&hub, &mut background).await?;
    log_settings(&hub);

    stop.await;
    tracing::info!("stopping");
    let _ = stopping.send(true);
    background.join().await;
    hub.work.close().await?;
    let _verifiers = Arc::clone(&hub.verify)
        .acquire_many_owned(u32::try_from(hub.config.verify_threads)?)
        .await?;
    reader.drain().await?;
    tasks::save(&hub);
    hub.tally.log();
    Ok(())
}

async fn listen(hub: &Arc<Hub>, background: &mut lifecycle::Group) -> Result<()> {
    let config = &hub.config;
    if let (Some((cert, key)), Some(names)) = (&config.tls, &config.own_certificate)
        && tls::create_self_signed(cert, key, names)?
    {
        tracing::info!(
            "created the certificate {} for {}; miners trust it with --tls-ca",
            cert.display(),
            names.join(", ")
        );
    }
    let acceptor = |protocol: Protocol| -> Result<Option<TlsAcceptor>> {
        let Some((cert, key)) = &config.tls else {
            return Ok(None);
        };
        if !config
            .listeners
            .iter()
            .any(|listener| listener.tls && listener.protocol == protocol)
        {
            return Ok(None);
        }
        tls::acceptor(cert, key, protocol.alpn()).map(Some)
    };
    let (v1, v2) = (acceptor(Protocol::V1)?, acceptor(Protocol::V2)?);
    for listener in &config.listeners {
        let socket = TcpListener::bind(listener.address)
            .await
            .with_context(|| format!("cannot listen on {}", listener.address))?;
        let (protocol, encrypted) = (listener.protocol, listener.tls);
        let acceptor = match (encrypted, protocol) {
            (false, _) => None,
            (true, Protocol::V1) => v1.clone(),
            (true, Protocol::V2) => v2.clone(),
        };
        let (transport, scheme, security) = match acceptor {
            _ if listener.web => (
                stratum::Transport::Web(config.web_origins.clone()),
                "ws",
                ", for web pages".to_owned(),
            ),
            Some(acceptor) => (
                stratum::Transport::Tls(acceptor),
                protocol.scheme(true),
                ", TLS 1.3 hybrid X25519MLKEM768 key exchange".to_owned(),
            ),
            None => (
                stratum::Transport::Plain,
                protocol.scheme(false),
                String::new(),
            ),
        };
        tracing::info!(
            "{} miners on {scheme}://{} ({}{security})",
            listener.mode.label(),
            socket.local_addr()?,
            protocol.label()
        );
        background.push(tokio::spawn(stratum::serve(
            socket,
            listener.mode,
            protocol,
            transport,
            Arc::clone(hub),
        )));
    }
    Ok(())
}

fn log_settings(hub: &Hub) {
    let config = &hub.config;
    let fees = &config.fees;
    tracing::info!(
        "fees: PPLNS {}%, SOLO {}%{}; PPLNS window {}x the network difficulty, lookback {} min",
        config::decimal_text(fees.pplns_bps.into(), 100),
        config::decimal_text(fees.solo_bps.into(), 100),
        if fees.recipients.is_empty() {
            String::new()
        } else {
            format!(", shared with {} fee recipients", fees.recipients.len())
        },
        config.pplns.factor_text(),
        config.pplns.lookback.as_secs() / 60
    );
    let vardiff = &config.vardiff;
    tracing::info!(
        "share difficulty starts at {}, at least {}, aiming at one share every {} s per worker",
        vardiff.start,
        vardiff.min,
        vardiff.share_interval
    );
    let payouts = &config.payouts;
    if payouts.enabled {
        tracing::info!(
            "payouts every {} min from {} TRAI, fee {} paid by the {}",
            payouts.interval.as_secs() / 60,
            format_trai(payouts.minimum),
            payouts.fee,
            match payouts.fee_payer {
                cli::FeePayer::Pool => "pool",
                cli::FeePayer::Miners => "miners",
            }
        );
    } else {
        tracing::info!("payouts are off; balances are only credited");
    }
}

async fn wait_for_node(node: &NodeClient) -> Result<NodeInfo> {
    let mut outage = NodeOutage::new(Duration::ZERO, WAIT_REMINDER);
    loop {
        match node.info().await {
            Ok(info) => {
                if let Some(failed) = outage.recovered() {
                    tracing::info!(
                        "the node at {} answers after {}",
                        node.mining_url(),
                        waited(failed)
                    );
                }
                return Ok(info);
            }
            Err(NodeError::Trust(error)) => return Err(error.into()),
            Err(error) => {
                report_wait(
                    &mut outage,
                    &format!("the node at {}", node.mining_url()),
                    &error,
                );
                tokio::time::sleep(WAIT_INTERVAL).await;
            }
        }
    }
}

/// Retry temporary node outages and busy responses.
async fn wait_for_mining_rates(node: &NodeClient) -> Result<NetworkMining, NodeError> {
    let mut outage = NodeOutage::new(Duration::ZERO, WAIT_REMINDER);
    loop {
        match node.network_mining().await {
            Err(error) if passing(&error) => {
                report_wait(
                    &mut outage,
                    &format!("the node at {}", node.api_url()),
                    &error,
                );
                tokio::time::sleep(BUSY_RETRY).await;
            }
            answer => return answer,
        }
    }
}

fn passing(error: &NodeError) -> bool {
    match error {
        NodeError::Transport(_) | NodeError::Busy => true,
        NodeError::Status { status, .. } => {
            *status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
        }
        _ => false,
    }
}

async fn wait_for_node_trust(
    node: &mut NodeClient,
    chain_id: Hash384,
    identity: [u8; 32],
) -> Result<()> {
    let mut outage = NodeOutage::new(Duration::ZERO, WAIT_REMINDER);
    loop {
        match node.establish_trust(chain_id, identity).await {
            Ok(()) => return Ok(()),
            Err(error @ NodeError::Transport(_)) => {
                report_wait(
                    &mut outage,
                    &format!("the node identity at {}", node.api_url()),
                    &error,
                );
            }
            Err(NodeError::Status { status, .. })
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
            {
                report_wait(
                    &mut outage,
                    &format!("the node identity at {}", node.api_url()),
                    &status,
                );
            }
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

/// Log the first failure, then limit repeats to once a minute.
fn report_wait(outage: &mut NodeOutage, what: &str, error: &dyn std::fmt::Display) {
    match outage.failed() {
        Outage::Paused => tracing::warn!("waiting for {what}: {error}"),
        Outage::Lasting(failed) => {
            tracing::info!("still waiting for {what} after {}: {error}", waited(failed));
        }
        Outage::Brief | Outage::Quiet => tracing::debug!("waiting for {what}: {error}"),
    }
}

/// Read the daemon's payout address when startup requires one.
async fn reach_wallet(
    config: &Config,
    wallet: &Wallet,
    network: Network,
    chain_id: Hash384,
) -> Result<Option<String>> {
    let mut attempt = 1;
    loop {
        match wallet.checked_address(network, chain_id).await {
            Ok(address) => return Ok(Some(address)),
            Err(
                error @ (CheckError::OtherChain { .. }
                | CheckError::Wallet(WalletError::Rpc { .. })),
            ) => return Err(error.into()),
            Err(error) if attempt < WALLET_ATTEMPTS => {
                tracing::warn!("waiting for the wallet daemon: {error}");
                attempt += 1;
                tokio::time::sleep(WALLET_RETRY).await;
            }
            Err(error) if config.wallet_check && config.payouts.enabled => bail!(
                "the wallet daemon {} does not answer: {error}. Payouts need it: start tenrai-wallet daemon first, or start without it with --no-wallet-check or --no-payouts",
                config.wallet
            ),
            Err(error) => {
                tracing::warn!(
                    "the wallet daemon {} does not answer: {error}",
                    config.wallet
                );
                return Ok(None);
            }
        }
    }
}

/// The configured reward address and database must match the daemon's address.
fn choose_pool_address(
    given: Option<&str>,
    reached: Option<&str>,
    stored: Option<&str>,
) -> Result<String> {
    if let (Some(reached), Some(given)) = (reached, given)
        && reached != given
    {
        bail!("--pool-address {given} is not the wallet daemon's receive address {reached}");
    }
    let address = reached.or(given).or(stored).context(
        "the pool address comes from the wallet daemon; give --pool-address to start without it",
    )?;
    if let Some(stored) = stored
        && stored != address
    {
        bail!("this database belongs to pool address {stored}, but this start uses {address}");
    }
    Ok(address.to_owned())
}

fn parse_round_work(text: Option<&str>) -> [u128; 2] {
    let mut work = [0, 0];
    if let Some((pplns, solo)) = text.and_then(|text| text.split_once(',')) {
        work = [pplns.parse().unwrap_or(0), solo.parse().unwrap_or(0)];
    }
    work
}

async fn wait_for_stop() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
