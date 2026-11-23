use crate::chain::Network;
use clap::{CommandFactory, Parser, ValueEnum};
use serde_json::{Map, Value};
use std::{
    ffi::OsString,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Parser, Debug)]
#[command(
    name = "tenrai-pool",
    version,
    about = "Tenrai reference mining pool: PPLNS and SOLO over tenrai-mining-v1, payouts through the wallet daemon",
    args_override_self = true
)]
pub struct Cli {
    #[arg(
        long,
        value_name = "FILE",
        help = "JSON file with the same options; the command line wins"
    )]
    pub config: Option<PathBuf>,

    #[arg(
        long,
        value_name = "NETWORK",
        default_value = "mainnet",
        value_parser = network,
        help = "mainnet, testnet, testnet100 or development; every network has its own default ports"
    )]
    pub network: Network,
    #[arg(
        long,
        value_name = "HOST[:PORT]",
        help = "Loopback mining endpoint of the node, or its URL [default: 127.0.0.1 on the network's port]"
    )]
    pub node: Option<String>,
    #[arg(
        long,
        value_name = "HOST[:PORT]",
        help = "HTTP API of the same node, for block states and network figures, or its URL [default: 127.0.0.1 on the network's port]"
    )]
    pub node_api: Option<String>,
    #[arg(
        long,
        value_name = "HEX",
        help = "Expected chain ID; required on a testnet, built in for mainnet and development"
    )]
    pub chain_id: Option<String>,
    #[arg(
        long,
        value_name = "HEX",
        help = "Expected ML-DSA-65 identity of the node; required for a remote node"
    )]
    pub node_identity: Option<String>,

    #[arg(
        long,
        value_name = "HOST[:PORT]",
        help = "Wallet daemon that holds the pool wallet, or its URL [default: 127.0.0.1 on the network's port]"
    )]
    pub wallet: Option<String>,
    #[arg(
        long,
        value_name = "FILE",
        help = "Bearer token file of the wallet daemon, <wallet file>.token [default: the token of this machine's wallet for the network]"
    )]
    pub wallet_token_file: Option<PathBuf>,
    #[arg(
        long,
        help = "Start even when the wallet daemon does not answer; payouts begin once it does"
    )]
    pub no_wallet_check: bool,
    #[arg(
        long,
        value_name = "PER_MINUTE",
        default_value_t = 300,
        help = "Requests a minute one client may send to the website and its API; 0 lifts the per-client limit"
    )]
    pub web_rate_limit: u64,
    #[arg(
        long,
        value_name = "ADDRESS",
        help = "Pool address for a start without the wallet daemon; must be the daemon's receive address"
    )]
    pub pool_address: Option<String>,

    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "PPLNS V1 listener for this machine, repeatable [default: 127.0.0.1 on the network's PPLNS port]"
    )]
    pub pplns_listen: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "SOLO V1 listener for this machine, repeatable [default: 127.0.0.1 on the network's SOLO port]"
    )]
    pub solo_listen: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "PPLNS V1 TLS listener; without --tls-cert and --tls-key the pool creates a certificate"
    )]
    pub pplns_listen_tls: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "SOLO V1 TLS listener; without --tls-cert and --tls-key the pool creates a certificate"
    )]
    pub solo_listen_tls: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "PPLNS V2 listener, always TLS"
    )]
    pub pplns_listen_v2_tls: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "SOLO V2 listener, always TLS"
    )]
    pub solo_listen_v2_tls: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "PPLNS V1 over WebSocket on loopback for a TLS proxy"
    )]
    pub pplns_listen_ws: Vec<String>,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        value_delimiter = ',',
        help = "SOLO V1 over WebSocket on loopback for a TLS proxy"
    )]
    pub solo_listen_ws: Vec<String>,
    #[arg(
        long,
        value_name = "ORIGIN",
        value_delimiter = ',',
        help = "Only pages of these origins may mine over WebSocket, e.g. https://miner.example [default: any page]"
    )]
    pub ws_origin: Vec<String>,
    #[arg(long, help = "Open no V1 listener, not even the default ones")]
    pub no_v1: bool,
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    #[arg(long, value_name = "COUNT", default_value_t = 8_192)]
    pub max_clients: u32,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 256,
        help = "Connections from one IPv4 address or IPv6 /64"
    )]
    pub max_clients_per_ip: u32,
    #[arg(long, value_name = "SECONDS", default_value_t = 600)]
    pub idle_timeout: u64,
    #[arg(
        long,
        value_parser = parse_tag,
        help = "Write this tag, e.g. the pool name, instead of each miner's own"
    )]
    pub tag: Option<String>,
    #[arg(long, value_enum, default_value_t = TenQExecution::Full)]
    pub tenq_mode: TenQExecution,
    #[arg(long, help = "Do not build the next epoch's dataset ahead of time")]
    pub no_tenq_prefetch: bool,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 0,
        help = "Concurrent TenQ share checks; 0 uses the host CPU count"
    )]
    pub verify_threads: usize,
    #[arg(long, help = "Ban addresses that send invalid shares or bad requests")]
    pub autoban: bool,
    #[arg(long, value_name = "SECONDS", default_value_t = 600)]
    pub ban_time: u64,

    #[arg(long, value_name = "DIFFICULTY", default_value_t = 1_048_576)]
    pub start_difficulty: u64,
    #[arg(long, value_name = "DIFFICULTY", default_value_t = 50)]
    pub min_difficulty: u64,
    #[arg(
        long,
        value_name = "DIFFICULTY",
        help = "Highest share difficulty [default: the network difficulty]"
    )]
    pub max_difficulty: Option<u64>,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 16,
        help = "Keep the share difficulty at most the network difficulty divided by this, so every block spreads over at least this many shares; 1 lets one share be a whole block"
    )]
    pub shares_per_block: u64,
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 20,
        help = "Time between two shares of one worker that vardiff aims for"
    )]
    pub share_interval: u64,
    #[arg(
        long,
        value_name = "PERCENT",
        default_value_t = 25,
        help = "Keep the difficulty while the share rate stays this close to the aim"
    )]
    pub vardiff_tolerance: u32,
    #[arg(
        long,
        value_name = "FACTOR",
        default_value_t = 4,
        help = "Largest factor one change may move the difficulty by"
    )]
    pub vardiff_max_step: u32,
    #[arg(
        long,
        value_name = "SHARES",
        default_value_t = 64,
        help = "Shares the hashrate estimate keeps at full weight; older ones count half"
    )]
    pub vardiff_history: u32,

    #[arg(
        long,
        value_name = "FACTOR",
        default_value = "2",
        help = "PPLNS window as a multiple of the block's network difficulty"
    )]
    pub pplns_window: String,
    #[arg(
        long,
        value_name = "MINUTES",
        default_value_t = 60,
        help = "Oldest share a PPLNS window may still count"
    )]
    pub pplns_lookback: u64,

    #[arg(long, value_name = "PERCENT", default_value = "2.5")]
    pub pplns_fee: String,
    #[arg(long, value_name = "PERCENT", default_value = "3")]
    pub solo_fee: String,
    #[arg(
        long,
        value_name = "ADDRESS=PERCENT",
        help = "Share of the collected fees paid to an address, repeatable; the rest stays in the pool wallet"
    )]
    pub fee_recipient: Vec<String>,

    #[arg(
        long,
        help = "Credit balances but send no payouts; the wallet daemon becomes optional"
    )]
    pub no_payouts: bool,
    #[arg(long, value_name = "MINUTES", default_value_t = 15)]
    pub payout_interval: u64,
    #[arg(long, value_name = "TRAI", default_value = "1")]
    pub min_payout: String,
    #[arg(
        long,
        value_name = "FEE",
        default_value = "normal",
        help = "minimum, normal, priority or a total in TRAI per transaction"
    )]
    pub payout_fee: String,
    #[arg(
        long,
        value_name = "TRAI",
        help = "Skip a payout round while a transaction would cost more"
    )]
    pub payout_fee_max: Option<String>,
    #[arg(long, value_enum, default_value_t = FeePayer::Pool)]
    pub payout_fee_payer: FeePayer,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 7,
        help = "Recipients per transaction"
    )]
    pub payout_batch_size: usize,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 20,
        help = "Transactions per payout round"
    )]
    pub payout_batches: usize,
    #[arg(
        long,
        value_name = "OUTPUTS",
        default_value_t = 64,
        help = "Compound the pool wallet above this many outputs, 0 leaves it to the daemon"
    )]
    pub compound_above: u64,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 128,
        help = "Compound transactions open at once before the next wave waits"
    )]
    pub compound_open: u64,

    #[arg(long, value_name = "FILE", default_value = "data/pool.sqlite")]
    pub db: PathBuf,
    #[arg(
        long,
        value_name = "DAYS",
        default_value_t = 0,
        help = "Keep the PPLNS split of a block this many days after its reward is credited, for the block page; 0 drops it then"
    )]
    pub keep_block_splits: u64,
    #[arg(
        long,
        value_name = "DAYS",
        default_value_t = 2,
        help = "Keep closed blocks this many days for the block lists, 0 forever; open blocks stay until they close"
    )]
    pub keep_blocks: u64,
    #[arg(
        long,
        value_name = "COUNT",
        default_value_t = 10_000,
        help = "Keep at most this many closed blocks, the newest, 0 no limit"
    )]
    pub keep_blocks_max: u64,
    #[arg(
        long,
        value_name = "IP[:PORT]",
        help = "Website, API and metrics [default: 127.0.0.1 on the network's port]"
    )]
    pub web_bind: Option<String>,
    #[arg(long, help = "Serve neither the website nor the API")]
    pub no_web: bool,
    #[arg(long, help = "Do not serve /metrics")]
    pub no_metrics: bool,
    #[arg(long, default_value = "Tenrai Pool")]
    pub pool_name: String,
    #[arg(
        long,
        value_name = "HOST",
        help = "Host name shown in the connect instructions [default: the page's host]"
    )]
    pub public_host: Option<String>,
    #[arg(
        long,
        value_name = "URL",
        help = "PPLNS stratum URL shown on the website, e.g. stratum+tls://pool.example:28741, repeatable [default: the listeners on the public host]"
    )]
    pub pplns_url: Vec<String>,
    #[arg(
        long,
        value_name = "URL",
        help = "SOLO stratum URL shown on the website, repeatable [default: the listeners on the public host]"
    )]
    pub solo_url: Vec<String>,
    #[arg(
        long,
        value_name = "URL",
        default_value = "https://explorer.tenrai-technologies.org/",
        help = "Block explorer the website links transactions, blocks and cuts to; empty turns the links off"
    )]
    pub explorer_url: String,
    #[arg(long, value_name = "SECONDS", default_value_t = 60)]
    pub stats_interval: u64,
    #[arg(short = 'd', long)]
    pub debug: bool,
    #[arg(long)]
    pub log_file: Option<PathBuf>,
    #[arg(long)]
    pub no_color: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum TenQExecution {
    Full,
    Light,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum FeePayer {
    Pool,
    Miners,
}

fn network(value: &str) -> Result<Network, String> {
    Network::parse(value)
        .ok_or_else(|| "expected mainnet, testnet, testnet100 or development".into())
}

pub fn parse(args: Vec<OsString>) -> anyhow::Result<Cli> {
    let cli = Cli::try_parse_from(&args)?;
    let path = match &cli.config {
        Some(path) => path.clone(),
        None => match default_config() {
            Some(path) => path,
            None => return Ok(cli),
        },
    };
    let mut merged = args[..args.len().min(1)].to_vec();
    merged.extend(file_args(&path)?);
    merged.extend(args.into_iter().skip(1));
    Ok(Cli::try_parse_from(merged)?)
}

fn default_config() -> Option<PathBuf> {
    // Test binaries share an output directory. Skip the per-binary config.
    if cfg!(test) {
        return None;
    }
    let path = std::env::current_exe()
        .ok()?
        .parent()?
        .join("tenrai-pool.json");
    match create_default(&path, &Cli::command()) {
        Ok(true) => eprintln!("created {} with the default settings", path.display()),
        Ok(false) => {}
        Err(_) if path.is_file() => {}
        Err(_) => return None,
    }
    Some(path)
}

fn create_default(path: &Path, command: &clap::Command) -> std::io::Result<bool> {
    let mut entries = Map::new();
    for arg in command.get_arguments() {
        let Some(long) = arg.get_long() else {
            continue;
        };
        if arg.is_hide_set() || matches!(long, "config" | "help" | "version") {
            continue;
        }
        let value = if !arg.get_action().takes_values() {
            Value::Bool(false)
        } else {
            let mut defaults: Vec<Value> = arg
                .get_default_values()
                .iter()
                .map(|value| Value::String(value.to_string_lossy().into_owned()))
                .collect();
            match defaults.len() {
                0 => Value::Null,
                1 => defaults.remove(0),
                _ => Value::Array(defaults),
            }
        };
        entries.insert(long.to_owned(), value);
    }
    if path.exists() {
        return Ok(false);
    }
    let text = serde_json::to_string_pretty(&Value::Object(entries))? + "\n";
    // Publish the config atomically for concurrent starts.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut staging = path.as_os_str().to_owned();
    staging.push(format!(".{}-{serial}.tmp", std::process::id()));
    let staging = PathBuf::from(staging);
    let mut file = std::fs::File::create(&staging)?;
    let linked = file
        .write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::hard_link(&staging, path));
    drop(file);
    let _ = std::fs::remove_file(&staging);
    match linked {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error),
    }
}

/// Convert JSON options to CLI arguments. `true` emits a bare flag.
fn file_args(path: &std::path::Path) -> anyhow::Result<Vec<OsString>> {
    let label = path.display();
    let text = std::fs::read_to_string(path)
        .map_err(|error| anyhow::anyhow!("cannot read config {label}: {error}"))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("config {label} is not valid JSON: {error}"))?;
    let Value::Object(entries) = value else {
        anyhow::bail!("config {label} must be a JSON object");
    };
    let mut args = Vec::new();
    for (key, value) in entries {
        let flag = OsString::from(format!("--{key}"));
        let values = match value {
            Value::Bool(true) => Vec::new(),
            Value::Bool(false) | Value::Null => continue,
            Value::Array(items) => items,
            other => vec![other],
        };
        if values.is_empty() {
            args.push(flag.clone());
        }
        for value in values {
            let text = match value {
                Value::String(text) => text,
                Value::Number(number) => number.to_string(),
                _ => anyhow::bail!("config option {key} must be text, a number or a list of them"),
            };
            args.extend([flag.clone(), text.into()]);
        }
    }
    Ok(args)
}

fn parse_tag(value: &str) -> Result<String, String> {
    crate::chain::check_tag(value).map(|()| value.to_owned())
}

#[cfg(test)]
#[path = "tests/cli.rs"]
mod tests;
