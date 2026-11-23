use crate::chain::Network;
use std::net::{IpAddr, SocketAddr};

/// Default ports per network, as used by the node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ports {
    pub node_api: u16,
    pub node_mining: u16,
    pub wallet: u16,
    pub pplns: u16,
    pub pplns_tls: u16,
    pub pplns_v2: u16,
    pub pplns_ws: u16,
    pub solo: u16,
    pub solo_tls: u16,
    pub solo_v2: u16,
    pub solo_ws: u16,
    pub web: u16,
}

impl Network {
    /// Testnet100 uses the testnet ports.
    pub const fn ports(self) -> Ports {
        let network = match self {
            Self::Mainnet => 0,
            Self::Testnet | Self::Testnet100 => 1,
            Self::Development => 2,
        };
        Ports {
            node_api: 28_701 + 5 * network,
            node_mining: 28_702 + 5 * network,
            wallet: 28_703 + 5 * network,
            pplns: 28_740 + 8 * network,
            pplns_tls: 28_741 + 8 * network,
            pplns_v2: 28_742 + 8 * network,
            pplns_ws: 28_743 + 8 * network,
            solo: 28_744 + 8 * network,
            solo_tls: 28_745 + 8 * network,
            solo_v2: 28_746 + 8 * network,
            solo_ws: 28_747 + 8 * network,
            web: 28_772 + 3 * network,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Host<'a> {
    Ip(IpAddr),
    Name(&'a str),
}

pub fn bind_address(value: &str, default_port: u16) -> Option<SocketAddr> {
    match host_port(value, default_port)? {
        (Host::Ip(ip), port) => Some(SocketAddr::new(ip, port)),
        (Host::Name(_), _) => None,
    }
}

/// IPv6 with a port needs brackets.
pub fn host_port(value: &str, default_port: u16) -> Option<(Host<'_>, u16)> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        return (address.port() != 0).then_some((Host::Ip(address.ip()), address.port()));
    }
    let bare = value
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(value);
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return Some((Host::Ip(ip), default_port));
    }
    let (name, port) = match value.rsplit_once(':') {
        Some((name, port)) => (name, port.parse::<u16>().ok().filter(|port| *port != 0)?),
        None => (value, default_port),
    };
    is_host_name(name).then_some((Host::Name(name), port))
}

pub fn http_origin(value: &str, default_port: u16) -> String {
    let value = value.trim();
    if value.contains("://") {
        return value.to_owned();
    }
    match host_port(value, default_port) {
        Some((Host::Ip(IpAddr::V6(ip)), port)) => format!("http://[{ip}]:{port}"),
        Some((Host::Ip(ip), port)) => format!("http://{ip}:{port}"),
        Some((Host::Name(name), port)) => format!("http://{name}:{port}"),
        None => value.to_owned(),
    }
}

fn is_host_name(name: &str) -> bool {
    let labels = || name.split('.');
    !name.is_empty()
        && name.len() <= 253
        && labels().all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        && labels()
            .next_back()
            .is_some_and(|label| label.bytes().any(|byte| byte.is_ascii_alphabetic()))
}

#[cfg(test)]
#[path = "tests/net.rs"]
mod tests;
