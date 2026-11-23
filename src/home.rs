use crate::chain::Network;
use std::path::{Path, PathBuf};

/// Token file of a wallet daemon started with default options.
pub fn wallet_token_file(network: Network) -> Option<PathBuf> {
    Some(wallet_token_file_in(&user_directory()?, network))
}

pub(crate) fn wallet_token_file_in(user_directory: &Path, network: Network) -> PathBuf {
    user_directory
        .join("wallet")
        .join(network.name())
        .join("wallet.vault.token")
}

#[cfg(windows)]
fn user_directory() -> Option<PathBuf> {
    std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(|path| Path::new(&path).join("Tenrai"))
}

#[cfg(not(windows))]
fn user_directory() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(|path| Path::new(&path).join(".tenrai"))
}
