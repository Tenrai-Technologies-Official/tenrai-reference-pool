//! TLS 1.3 pinned to the X25519MLKEM768 hybrid key exchange.

use anyhow::{Context, Result, anyhow, bail};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    client::Resumption,
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, server::TlsStream};

const TLS13_ONLY: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
const MAX_PEM_BYTES: u64 = 1 << 20;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn provider() -> CryptoProvider {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider.kx_groups = vec![rustls::crypto::aws_lc_rs::kx_group::X25519MLKEM768];
    provider
}

pub fn client_config(alpn: Vec<Vec<u8>>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = ClientConfig::builder_with_provider(Arc::new(provider()))
        .with_protocol_versions(TLS13_ONLY)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn;
    config.resumption = Resumption::disabled();
    config.enable_early_data = false;
    Ok(config)
}

pub fn http_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .use_preconfigured_tls(client_config(vec![b"http/1.1".to_vec()])?)
        .connect_timeout(Duration::from_secs(5))
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

pub fn acceptor(cert: &Path, key: &Path, alpn: &[u8]) -> Result<TlsAcceptor> {
    bounded(cert)?;
    bounded(key)?;
    let certificates = CertificateDer::pem_file_iter(cert)
        .with_context(|| format!("cannot read {}", cert.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("{} is not a PEM certificate chain", cert.display()))?;
    if certificates.is_empty() {
        bail!("{} contains no certificate", cert.display());
    }
    let key = PrivateKeyDer::from_pem_file(key)
        .with_context(|| format!("{} is not a PEM private key", key.display()))?;
    let mut config = ServerConfig::builder_with_provider(Arc::new(provider()))
        .with_protocol_versions(TLS13_ONLY)?
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    config.alpn_protocols = vec![alpn.to_vec()];
    config.max_early_data_size = 0;
    config.send_tls13_tickets = 0;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn accept(
    acceptor: &TlsAcceptor,
    socket: TcpStream,
    alpn: &[u8],
) -> Result<TlsStream<TcpStream>> {
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(socket))
        .await
        .map_err(|_| anyhow!("TLS handshake timed out"))?
        .context("TLS handshake failed")?;
    if stream.get_ref().1.alpn_protocol() != Some(alpn) {
        bail!("the client did not negotiate the stratum ALPN protocol");
    }
    Ok(stream)
}

/// Writes a self-signed certificate and key unless one exists; true when it did.
pub fn create_self_signed(cert: &Path, key: &Path, names: &[String]) -> Result<bool> {
    match (cert.exists(), key.exists()) {
        (true, true) => return Ok(false),
        (false, false) => {}
        (true, false) => bail!(
            "{} exists without its key {}",
            cert.display(),
            key.display()
        ),
        (false, true) => bail!(
            "{} exists without its certificate {}",
            key.display(),
            cert.display()
        ),
    }
    let made = rcgen::generate_simple_self_signed(names.to_vec())
        .context("cannot create a certificate")?;
    if let Some(folder) = cert
        .parent()
        .filter(|folder| !folder.as_os_str().is_empty())
    {
        std::fs::create_dir_all(folder)
            .with_context(|| format!("cannot create {}", folder.display()))?;
    }
    let mut private = std::fs::OpenOptions::new();
    private.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut private, 0o600);
    let written = private
        .open(key)
        .and_then(|mut file| {
            std::io::Write::write_all(&mut file, made.key_pair.serialize_pem().as_bytes())
        })
        .with_context(|| format!("cannot write {}", key.display()))
        .and_then(|()| {
            std::fs::write(cert, made.cert.pem())
                .with_context(|| format!("cannot write {}", cert.display()))
        });
    if written.is_err() {
        let _ = std::fs::remove_file(key);
    }
    written.map(|()| true)
}

fn bounded(path: &Path) -> Result<()> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("cannot read {}", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_PEM_BYTES {
        bail!("{} must be a regular PEM file", path.display());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
