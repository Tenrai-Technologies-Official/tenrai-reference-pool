use crate::{
    chain::{Hash384, Network},
    cli::{Cli, FeePayer, TenQExecution},
    net::{Ports, bind_address, http_origin},
    pow::TenQMode,
    stratum::{WebOrigins, protocol::Protocol},
};
use anyhow::{Context, Result, bail};
use std::{net::IpAddr, net::SocketAddr, path::Path, path::PathBuf, time::Duration};
use url::Url;

pub const ATOMS_PER_TRAI: u64 = 100_000_000;
pub const BASIS_POINTS: u32 = 10_000;
const MAX_CLIENT_LIMIT: u32 = 65_536;
const OWN_CERTIFICATE: &str = "stratum.crt";
const OWN_KEY: &str = "stratum.key";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Mode {
    Pplns,
    Solo,
}

impl Mode {
    pub const ALL: [Self; 2] = [Self::Pplns, Self::Solo];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Pplns => "pplns",
            Self::Solo => "solo",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Pplns => "PPLNS",
            Self::Solo => "SOLO",
        }
    }

    pub const fn code(self) -> i64 {
        match self {
            Self::Pplns => 0,
            Self::Solo => 1,
        }
    }

    pub const fn from_code(code: i64) -> Self {
        match code {
            0 => Self::Pplns,
            _ => Self::Solo,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Listener {
    pub address: SocketAddr,
    pub mode: Mode,
    pub tls: bool,
    pub protocol: Protocol,
    /// V1 over WebSocket for browser miners.
    pub web: bool,
}

#[derive(Clone, Debug)]
pub struct VardiffSettings {
    pub start: u64,
    pub min: u64,
    pub max: Option<u64>,
    pub shares_per_block: u64,
    pub share_interval: u64,
    pub tolerance_percent: u32,
    pub max_step: u32,
    pub history_shares: u32,
}

impl VardiffSettings {
    /// At very low network difficulty every share is already a block, so effort cannot be measured.
    pub fn measures_effort(&self, network_difficulty: u128) -> bool {
        network_difficulty > u128::from(self.min)
    }
}

#[derive(Clone, Debug)]
pub struct PplnsSettings {
    pub numerator: u128,
    pub denominator: u128,
    pub lookback: Duration,
}

impl PplnsSettings {
    /// Share work covered by the PPLNS window at this network difficulty.
    pub fn window_work(&self, network_difficulty: u128) -> u128 {
        (network_difficulty.saturating_mul(self.numerator) / self.denominator).max(1)
    }

    pub fn factor_text(&self) -> String {
        decimal_text(self.numerator, self.denominator)
    }
}

#[derive(Clone, Debug)]
pub struct FeeSettings {
    pub pplns_bps: u32,
    pub solo_bps: u32,
    /// Address and share of the fee in basis points.
    pub recipients: Vec<(String, u32)>,
}

impl FeeSettings {
    pub const fn bps(&self, mode: Mode) -> u32 {
        match mode {
            Mode::Pplns => self.pplns_bps,
            Mode::Solo => self.solo_bps,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PayoutSettings {
    pub enabled: bool,
    /// How often open payouts are checked and a new round may start.
    pub check_every: Duration,
    pub interval: Duration,
    pub minimum: u64,
    pub fee: String,
    pub fee_max: Option<u64>,
    pub fee_payer: FeePayer,
    pub batch_size: usize,
    pub batches: usize,
    pub compound_above: u64,
    pub compound_open: u64,
    pub compound_every: Duration,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub node: Url,
    pub node_api: Url,
    pub chain_id: Hash384,
    pub node_identity: Option<[u8; 32]>,
    pub wallet: Url,
    /// `None` only with payouts off; then no wallet daemon is needed.
    pub wallet_token: Option<String>,
    /// Whether the wallet daemon must answer at start.
    pub wallet_check: bool,
    pub pool_address: Option<String>,
    pub listeners: Vec<Listener>,
    pub web_origins: WebOrigins,
    pub tls: Option<(PathBuf, PathBuf)>,
    /// Names for the self-signed certificate created when no certificate files exist.
    pub own_certificate: Option<Vec<String>>,
    pub max_clients: u32,
    pub max_clients_per_ip: u32,
    pub idle_timeout: Duration,
    pub tag: Option<String>,
    pub tenq_mode: TenQMode,
    pub tenq_prefetch: bool,
    pub verify_threads: usize,
    pub ban: Option<Duration>,
    pub vardiff: VardiffSettings,
    pub pplns: PplnsSettings,
    pub fees: FeeSettings,
    pub payouts: PayoutSettings,
    pub db: PathBuf,
    /// How long a credited block keeps its PPLNS split; zero drops it at the next prune.
    pub keep_block_splits: Duration,
    pub keep_blocks: Option<Duration>,
    pub keep_blocks_max: Option<u64>,
    pub web: Option<SocketAddr>,
    pub web_rate_limit: u64,
    pub metrics: bool,
    pub pool_name: String,
    pub public_host: Option<String>,
    pub stratum_urls: Vec<(Mode, String)>,
    pub explorer_url: Option<String>,
    pub stats_interval: Duration,
}

impl Config {
    pub fn from_cli(cli: &Cli) -> Result<Self> {
        Self::build(cli, crate::home::wallet_token_file(cli.network))
    }

    /// `usual_token` is the token file of a wallet daemon running with default options.
    pub(crate) fn build(cli: &Cli, usual_token: Option<PathBuf>) -> Result<Self> {
        let wallet_token = match (&cli.wallet_token_file, cli.no_payouts) {
            (Some(token_file), _) => Some(read_token(token_file)?),
            (None, true) => None,
            (None, false) => match usual_token {
                Some(token_file) if token_file.is_file() => Some(read_token(&token_file)?),
                usual => bail!(
                    "the pool pays miners through a wallet daemon and found no token for one{}. Start the pool wallet's daemon on this machine first (tenrai-wallet daemon writes the token), name the token with --wallet-token-file, or run without payouts with --no-payouts",
                    usual.map_or_else(String::new, |file| format!(
                        ": {} does not exist",
                        file.display()
                    ))
                ),
            },
        };
        let ports = cli.network.ports();
        let listeners = listeners(cli, ports)?;
        let stratum_urls = stratum_urls(cli)?;
        let (tls, own_certificate) = match (&cli.tls_cert, &cli.tls_key) {
            (Some(cert), Some(key)) => (Some((cert.clone(), key.clone())), None),
            _ if listeners.iter().any(|listener| listener.tls) => {
                let folder = cli.db.parent().unwrap_or(Path::new(""));
                (
                    Some((folder.join(OWN_CERTIFICATE), folder.join(OWN_KEY))),
                    Some(certificate_names(cli, &listeners, &stratum_urls)),
                )
            }
            _ => (None, None),
        };
        if cli.max_clients == 0 || cli.max_clients > MAX_CLIENT_LIMIT {
            bail!("--max-clients must be between 1 and {MAX_CLIENT_LIMIT}");
        }
        if cli.max_clients_per_ip == 0 || cli.max_clients_per_ip > cli.max_clients {
            bail!("--max-clients-per-ip must be between 1 and --max-clients");
        }
        if cli.idle_timeout < 30 {
            bail!("--idle-timeout must be at least 30 seconds");
        }
        let text = match (cli.chain_id.as_deref(), cli.network) {
            (Some(text), _) => text,
            (None, Network::Mainnet) => crate::chain::MAINNET_CHAIN_ID,
            (None, Network::Development) => crate::chain::DEVELOPMENT_CHAIN_ID,
            (None, network) => bail!(
                "a {} has no built-in chain ID: pass --chain-id with the chain the testnet's node names when it starts",
                network.name()
            ),
        };
        let chain_id = Hash384::from_hex(text)
            .filter(|chain_id| chain_id.to_hex() == text)
            .context("--chain-id must be 96 lowercase hexadecimal characters")?;
        let node_identity = cli
            .node_identity
            .as_deref()
            .map(crate::node::trust::parse_identity)
            .transpose()
            .map_err(|_| {
                anyhow::anyhow!("--node-identity must be 64 lowercase hexadecimal characters")
            })?;
        let node = http_url(&origin(&cli.node, ports.node_mining), "--node")?;
        let node_api = http_url(&origin(&cli.node_api, ports.node_api), "--node-api")?;
        if (!is_loopback_url(&node) || !is_loopback_url(&node_api)) && node_identity.is_none() {
            bail!("a remote node requires --node-identity");
        }
        let pool_name = cli.pool_name.trim().to_owned();
        if pool_name.is_empty() || pool_name.chars().count() > 40 {
            bail!("--pool-name must have 1 to 40 characters");
        }
        Ok(Self {
            node,
            node_api,
            chain_id,
            node_identity,
            wallet: http_url(&origin(&cli.wallet, ports.wallet), "--wallet")?,
            wallet_token,
            wallet_check: !cli.no_wallet_check,
            pool_address: cli
                .pool_address
                .as_deref()
                .map(str::trim)
                .map(str::to_owned),
            listeners,
            web_origins: WebOrigins::parse(&cli.ws_origin)?,
            tls,
            own_certificate,
            max_clients: cli.max_clients,
            max_clients_per_ip: cli.max_clients_per_ip,
            idle_timeout: Duration::from_secs(cli.idle_timeout),
            tag: cli.tag.clone().filter(|tag| !tag.is_empty()),
            tenq_mode: match cli.tenq_mode {
                TenQExecution::Full => TenQMode::Full,
                TenQExecution::Light => TenQMode::Light,
            },
            tenq_prefetch: !cli.no_tenq_prefetch,
            verify_threads: threads(cli.verify_threads),
            ban: cli
                .autoban
                .then(|| Duration::from_secs(cli.ban_time.max(1))),
            vardiff: vardiff(cli)?,
            pplns: pplns(cli)?,
            fees: fees(cli)?,
            payouts: payouts(cli)?,
            db: cli.db.clone(),
            keep_block_splits: days(cli.keep_block_splits),
            keep_blocks: (cli.keep_blocks > 0).then(|| days(cli.keep_blocks)),
            keep_blocks_max: (cli.keep_blocks_max > 0).then_some(cli.keep_blocks_max),
            web: match (&cli.web_bind, cli.no_web) {
                (_, true) => None,
                (Some(value), false) => Some(bind(value, ports.web, "--web-bind")?),
                (None, false) => Some(SocketAddr::from(([127, 0, 0, 1], ports.web))),
            },
            web_rate_limit: cli.web_rate_limit,
            metrics: !cli.no_metrics,
            pool_name,
            public_host: cli
                .public_host
                .clone()
                .filter(|host| !host.trim().is_empty()),
            stratum_urls,
            explorer_url: explorer_url(&cli.explorer_url)?,
            stats_interval: Duration::from_secs(cli.stats_interval.max(10)),
        })
    }
}

fn read_token(token_file: &std::path::Path) -> Result<String> {
    let token = std::fs::read_to_string(token_file)
        .with_context(|| format!("cannot read {}", token_file.display()))?
        .trim()
        .to_owned();
    if token.is_empty() {
        bail!("{} is empty", token_file.display());
    }
    Ok(token)
}

fn origin(value: &Option<String>, default_port: u16) -> String {
    http_origin(value.as_deref().unwrap_or("127.0.0.1"), default_port)
}

fn bind(value: &str, default_port: u16, option: &str) -> Result<SocketAddr> {
    bind_address(value, default_port)
        .with_context(|| format!("{option} {value} is neither an IP nor IP:PORT"))
}

fn listeners(cli: &Cli, ports: Ports) -> Result<Vec<Listener>> {
    let pplns_v1 = !cli.pplns_listen.is_empty() || !cli.pplns_listen_tls.is_empty();
    let solo_v1 = !cli.solo_listen.is_empty() || !cli.solo_listen_tls.is_empty();
    if cli.no_v1 && (pplns_v1 || solo_v1) {
        bail!("--no-v1 cannot be combined with V1 listeners");
    }
    if cli.no_v1 && cli.pplns_listen_v2_tls.is_empty() && cli.solo_listen_v2_tls.is_empty() {
        bail!("--no-v1 needs --pplns-listen-v2-tls or --solo-listen-v2-tls");
    }
    let mut all = Vec::new();
    let groups = [
        (
            &cli.pplns_listen,
            "--pplns-listen",
            ports.pplns,
            Mode::Pplns,
            false,
            Protocol::V1,
            false,
        ),
        (
            &cli.solo_listen,
            "--solo-listen",
            ports.solo,
            Mode::Solo,
            false,
            Protocol::V1,
            false,
        ),
        (
            &cli.pplns_listen_tls,
            "--pplns-listen-tls",
            ports.pplns_tls,
            Mode::Pplns,
            true,
            Protocol::V1,
            false,
        ),
        (
            &cli.solo_listen_tls,
            "--solo-listen-tls",
            ports.solo_tls,
            Mode::Solo,
            true,
            Protocol::V1,
            false,
        ),
        (
            &cli.pplns_listen_v2_tls,
            "--pplns-listen-v2-tls",
            ports.pplns_v2,
            Mode::Pplns,
            true,
            Protocol::V2,
            false,
        ),
        (
            &cli.solo_listen_v2_tls,
            "--solo-listen-v2-tls",
            ports.solo_v2,
            Mode::Solo,
            true,
            Protocol::V2,
            false,
        ),
        (
            &cli.pplns_listen_ws,
            "--pplns-listen-ws",
            ports.pplns_ws,
            Mode::Pplns,
            false,
            Protocol::V1,
            true,
        ),
        (
            &cli.solo_listen_ws,
            "--solo-listen-ws",
            ports.solo_ws,
            Mode::Solo,
            false,
            Protocol::V1,
            true,
        ),
    ];
    for (values, option, port, mode, tls, protocol, web) in groups {
        for value in values {
            all.push(Listener {
                address: bind(value, port, option)?,
                mode,
                tls,
                protocol,
                web,
            });
        }
    }
    if !cli.no_v1 && !pplns_v1 {
        all.push(default_listener(ports.pplns, Mode::Pplns));
    }
    if !cli.no_v1 && !solo_v1 {
        all.push(default_listener(ports.solo, Mode::Solo));
    }
    for listener in &all {
        if !listener.tls && !listener.address.ip().is_loopback() {
            bail!(
                "plain listener on {} must use loopback; use TLS or put a TLS proxy in front of a loopback listener",
                listener.address
            );
        }
    }
    for (index, listener) in all.iter().enumerate() {
        if all[..index]
            .iter()
            .any(|other| other.address == listener.address)
        {
            bail!("{} is listed twice", listener.address);
        }
    }
    Ok(all)
}

fn default_listener(port: u16, mode: Mode) -> Listener {
    Listener {
        address: SocketAddr::from(([127, 0, 0, 1], port)),
        mode,
        tls: false,
        protocol: Protocol::V1,
        web: false,
    }
}

fn vardiff(cli: &Cli) -> Result<VardiffSettings> {
    if cli.min_difficulty == 0 || cli.start_difficulty < cli.min_difficulty {
        bail!("--start-difficulty must not be below --min-difficulty, and both at least 1");
    }
    if cli
        .max_difficulty
        .is_some_and(|max| max < cli.start_difficulty)
    {
        bail!("--max-difficulty must not be below --start-difficulty");
    }
    if !(1..=3_600).contains(&cli.share_interval) {
        bail!("--share-interval must be between 1 and 3600");
    }
    if !(1..=4_096).contains(&cli.shares_per_block) {
        bail!("--shares-per-block must be between 1 and 4096");
    }
    if cli.vardiff_tolerance > 90 {
        bail!("--vardiff-tolerance must be at most 90 percent");
    }
    if !(2..=64).contains(&cli.vardiff_max_step) {
        bail!("--vardiff-max-step must be between 2 and 64");
    }
    if !(8..=4_096).contains(&cli.vardiff_history) {
        bail!("--vardiff-history must be between 8 and 4096 shares");
    }
    Ok(VardiffSettings {
        start: cli.start_difficulty,
        min: cli.min_difficulty,
        max: cli.max_difficulty,
        shares_per_block: cli.shares_per_block,
        share_interval: cli.share_interval,
        tolerance_percent: cli.vardiff_tolerance,
        max_step: cli.vardiff_max_step,
        history_shares: cli.vardiff_history,
    })
}

fn days(count: u64) -> Duration {
    Duration::from_secs(count.saturating_mul(86_400))
}

const MAX_URL_CHARS: usize = 200;

/// Host names for the self-signed certificate.
fn certificate_names(cli: &Cli, listeners: &[Listener], urls: &[(Mode, String)]) -> Vec<String> {
    let mut names = vec![
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ];
    names.extend(
        listeners
            .iter()
            .filter(|listener| listener.tls && !listener.address.ip().is_unspecified())
            .map(|listener| listener.address.ip().to_string()),
    );
    names.extend(cli.public_host.iter().map(|host| host.trim().to_owned()));
    names.extend(urls.iter().filter_map(|(_, url)| {
        let rest = url.split_once("://")?.1;
        let host = rest.split(['/', '?', '#']).next()?;
        let host = match host.strip_prefix('[') {
            Some(bracketed) => bracketed.split(']').next()?,
            None => host.rsplit_once(':').map_or(host, |(name, _)| name),
        };
        Some(host.to_owned())
    }));
    let mut kept = Vec::new();
    for name in names {
        if !name.is_empty() && !kept.contains(&name) {
            kept.push(name);
        }
    }
    kept
}

/// Stratum URLs shown on the website instead of the ones derived from the listeners.
fn stratum_urls(cli: &Cli) -> Result<Vec<(Mode, String)>> {
    let mut urls = Vec::new();
    for (mode, given, flag) in [
        (Mode::Pplns, &cli.pplns_url, "--pplns-url"),
        (Mode::Solo, &cli.solo_url, "--solo-url"),
    ] {
        for url in given {
            let url = url.trim();
            let scheme = ["stratum+tcp://", "stratum+tls://", "stratum2+tls://"]
                .iter()
                .any(|scheme| url.starts_with(scheme));
            if !scheme || url.len() > MAX_URL_CHARS || !url.chars().all(|c| c.is_ascii_graphic()) {
                bail!(
                    "{flag} takes a stratum+tcp://, stratum+tls:// or stratum2+tls:// URL of at most {MAX_URL_CHARS} characters"
                );
            }
            let parsed = Url::parse(url).with_context(|| format!("{flag} is not a URL"))?;
            if parsed.scheme() == "stratum+tcp" && !is_loopback_url(&parsed) {
                bail!("{flag} allows stratum+tcp:// only for localhost or a loopback IP");
            }
            urls.push((mode, url.to_owned()));
        }
    }
    Ok(urls)
}

fn explorer_url(text: &str) -> Result<Option<String>> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    let url = Url::parse(text).context("--explorer-url must be an http or https URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > MAX_URL_CHARS
    {
        bail!("--explorer-url must be an http or https URL without a query or fragment");
    }
    let mut base = url.to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    Ok(Some(base))
}

fn pplns(cli: &Cli) -> Result<PplnsSettings> {
    let (numerator, denominator) =
        decimal(&cli.pplns_window, 4).context("--pplns-window must be a number like 2 or 1.5")?;
    if numerator == 0 || numerator > 100 * denominator {
        bail!("--pplns-window must be above 0 and at most 100");
    }
    if cli.pplns_lookback == 0 || cli.pplns_lookback > 7 * 24 * 60 {
        bail!("--pplns-lookback must be between 1 minute and 7 days");
    }
    Ok(PplnsSettings {
        numerator,
        denominator,
        lookback: Duration::from_secs(cli.pplns_lookback * 60),
    })
}

fn fees(cli: &Cli) -> Result<FeeSettings> {
    let pplns_bps = percent_bps(&cli.pplns_fee).context("--pplns-fee")?;
    let solo_bps = percent_bps(&cli.solo_fee).context("--solo-fee")?;
    let mut recipients = Vec::new();
    for entry in &cli.fee_recipient {
        let (address, share) = entry
            .split_once('=')
            .context("--fee-recipient takes ADDRESS=PERCENT")?;
        let share = percent_bps(share).context("--fee-recipient")?;
        if share == 0 {
            bail!("--fee-recipient {address} has a share of 0");
        }
        if recipients.iter().any(|(known, _)| known == address) {
            bail!("--fee-recipient {address} is listed twice");
        }
        recipients.push((address.trim().to_owned(), share));
    }
    if recipients.iter().map(|(_, share)| share).sum::<u32>() > BASIS_POINTS {
        bail!("the --fee-recipient shares add up to more than 100 percent");
    }
    Ok(FeeSettings {
        pplns_bps,
        solo_bps,
        recipients,
    })
}

fn payouts(cli: &Cli) -> Result<PayoutSettings> {
    if cli.payout_interval == 0 {
        bail!("--payout-interval must be at least 1 minute");
    }
    let minimum = trai_atoms(&cli.min_payout).context("--min-payout")?;
    if minimum > i64::MAX as u64 {
        bail!("payout minimum exceeds current balance storage");
    }
    if minimum == 0 {
        bail!("--min-payout must be above 0");
    }
    match cli.payout_fee.trim() {
        "minimum" | "normal" | "priority" => {}
        total => {
            trai_atoms(total).context("--payout-fee must be minimum, normal, priority or TRAI")?;
        }
    }
    let fee_max = cli
        .payout_fee_max
        .as_deref()
        .map(|text| trai_atoms(text).context("--payout-fee-max"))
        .transpose()?;
    if !(1..=64).contains(&cli.payout_batch_size) {
        bail!("--payout-batch-size must be between 1 and 64");
    }
    if !(1..=500).contains(&cli.payout_batches) {
        bail!("--payout-batches must be between 1 and 500");
    }
    if cli.compound_above == 1 {
        bail!("--compound-above needs at least 2 outputs, or 0 for off");
    }
    if !(1..=10_000).contains(&cli.compound_open) {
        bail!("--compound-open must be between 1 and 10000");
    }
    Ok(PayoutSettings {
        enabled: !cli.no_payouts,
        check_every: Duration::from_secs(20),
        interval: Duration::from_secs(cli.payout_interval * 60),
        minimum,
        fee: cli.payout_fee.trim().to_owned(),
        fee_max,
        fee_payer: cli.payout_fee_payer,
        batch_size: cli.payout_batch_size,
        batches: cli.payout_batches,
        compound_above: cli.compound_above,
        compound_open: cli.compound_open,
        compound_every: Duration::from_secs(5),
    })
}

fn threads(requested: usize) -> usize {
    if requested > 0 {
        return requested;
    }
    std::thread::available_parallelism().map_or(1, |count| count.get())
}

/// An HTTP(S) origin; plain HTTP only to this machine.
pub fn http_url(text: &str, option: &str) -> Result<Url> {
    let url = Url::parse(text.trim()).with_context(|| format!("{option} is not a URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.path() != "/"
    {
        bail!("{option} must be an http:// or https:// origin without path or credentials");
    }
    let local = is_loopback_url(&url);
    if url.scheme() == "http" && !local {
        bail!("{option} must use https:// for a host other than this machine");
    }
    Ok(url)
}

fn is_loopback_url(url: &Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Basis points from a percentage (at most two decimals, at most 100).
pub fn percent_bps(text: &str) -> Result<u32> {
    let (value, scale) =
        decimal(text.trim(), 2).context("a percentage with at most two decimals")?;
    let bps = value * 100 / scale;
    if bps > u128::from(BASIS_POINTS) {
        bail!("a percentage of at most 100");
    }
    Ok(u32::try_from(bps)?)
}

/// Atoms from a TRAI amount with at most eight decimals.
pub fn trai_atoms(text: &str) -> Result<u64> {
    let (value, scale) =
        decimal(text.trim(), 8).context("a TRAI amount with at most eight decimals")?;
    let atoms = value * u128::from(ATOMS_PER_TRAI) / scale;
    u64::try_from(atoms).context("the amount is too large")
}

/// A non-negative decimal as (value, 10^decimals) with at most `places` decimals.
fn decimal(text: &str, places: usize) -> Option<(u128, u128)> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if (whole.is_empty() && fraction.is_empty())
        || fraction.len() > places
        || whole.len() > 18
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let scale = 10_u128.pow(fraction.len() as u32);
    let whole: u128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let fraction: u128 = if fraction.is_empty() {
        0
    } else {
        fraction.parse().ok()?
    };
    Some((whole * scale + fraction, scale))
}

pub fn decimal_text(value: u128, scale: u128) -> String {
    let whole = value / scale;
    let mut fraction = value % scale;
    if fraction == 0 {
        return whole.to_string();
    }
    let mut digits = scale.ilog10() as usize;
    while fraction.is_multiple_of(10) {
        fraction /= 10;
        digits -= 1;
    }
    format!("{whole}.{fraction:0digits$}")
}

pub fn format_trai(atoms: u64) -> String {
    format_trai_wide(u128::from(atoms))
}

pub fn format_trai_wide(atoms: u128) -> String {
    let unit = u128::from(ATOMS_PER_TRAI);
    format!("{}.{:08}", atoms / unit, atoms % unit)
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
