# Tenrai Reference Pool

PPLNS and SOLO mining pool for Tenrai. Miners connect over `tenrai-mining-v1` or `tenrai-mining-v2`. The pool credits rewards from finalized cuts and settles payouts through `tenrai-wallet daemon`.

## Build

Install Rust using `rust-toolchain.toml` and a C toolchain for AWS-LC and SQLite.

```sh
cargo build --release --locked
```

## Run

Run a synced `tenraid` node and a wallet daemon for the pool wallet. Store the wallet's recovery words and keep its token file accessible only to the pool user. Use a direct or threshold receive address for mining rewards.

Copy `pool.example.json` to `pool.json`. Set the wallet token and data paths, public host, listeners and payout policy for the deployment. Start the pool with the resulting configuration.

```sh
tenrai-pool --config pool.json
```

Command-line options override JSON settings. Run `tenrai-pool --help` for the full option list. Mainnet is the default. Testnet requires an explicit `--chain-id`. Use HTTPS and `--node-identity` for a remote node.

| Listener | Mainnet | Testnet | Development |
| --- | --- | --- | --- |
| Node API, node mining, wallet daemon | 28701, 28702, 28703 | 28706, 28707, 28708 | 28711, 28712, 28713 |
| PPLNS V1, V1 TLS, V2 TLS, WebSocket | 28740–28743 | 28748–28751 | 28756–28759 |
| SOLO V1, V1 TLS, V2 TLS, WebSocket | 28744–28747 | 28752–28755 | 28760–28763 |
| Website and API | 28772 | 28775 | 28778 |

## Connect miners

```sh
tenrai-miner -a <address> --stratum stratum+tls://pool.example.org:28741 -w rig1 --pool
tenrai-miner -a <address> --stratum stratum2+tls://pool.example.org:28742 -w rig1 --pool
```

The second command requires a V2 listener enabled with `--pplns-listen-v2-tls`. Both versions require TenQ algorithm `3` and specification revision `9`. Use `<address>` or `<address>.<worker>` for the login. Set `d=<n>` to request a fixed share difficulty within the configured bounds.

Use a certificate trusted by miners. For the pool's generated self-signed certificate, give miners `stratum.crt` with `--tls-ca`.

## Rewards and payouts

PPLNS credits shares within the configured work window and lookback. SOLO credits the finder. Both deduct the configured pool fee and credit rewards after finalization. Set `fee-recipient` entries only when distributing a share of those fees.

Payouts require a daemon that reports `durable_request_ids: true`. The pool records each payout and its daemon binding before sending. For an unresolved payment, preserve its original request, daemon archive and fee settings. Recover through the matching daemon journal so the same request ID and signed transaction can be reconciled.

Stop the pool and wallet daemon before copying or restoring their state. Keep the pool database with the matching wallet vault, `.pending.json`, `.pending/`, `.requests.json` and `.requests.json.sqlite3` files. Include any SQLite journal files and preserve owner-only access.

## Website and metrics

Bind the website and WebSocket listeners to loopback behind a TLS reverse proxy. Set `X-Real-IP` from the client connection, configure `--ws-origin` for browser miners and keep `/metrics` private. See the [nginx example](deploy/nginx/pool.tenrai-technologies.org.conf).

The website shows pool statistics, blocks, miner balances and payouts. The native API is under `/api/v1/` and the Miningcore-compatible pool list is at `/api/pools`.

## Checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace
```

## License

Apache-2.0.
