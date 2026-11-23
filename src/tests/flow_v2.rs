//! A V2 miner over post-quantum TLS on the same pool and fake node as the V1 flow.

use super::*;
use crate::stratum::{
    protocol::{ClientMessage, JobNotice, ServerMessage},
    v2::{self, ClientCodec, ClientEvent},
};
use futures_util::{SinkExt, StreamExt};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        ClientConfig, RootCertStore,
        crypto::aws_lc_rs::{default_provider, kx_group::X25519MLKEM768},
        pki_types::ServerName,
        version::TLS13,
    },
};
use tokio_util::codec::Framed;

struct V2Miner {
    wire: Framed<TlsStream<TcpStream>, ClientCodec>,
    prefix: Vec<u8>,
    job: Option<JobNotice>,
    next_id: u64,
}

impl V2Miner {
    async fn connect(port: u16, roots: RootCertStore) -> Self {
        let mut provider = default_provider();
        provider.kx_groups = vec![X25519MLKEM768];
        let mut config = ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![v2::ALPN.to_vec()];
        let connector = TlsConnector::from(Arc::new(config));
        let started = Instant::now();
        let stream = loop {
            let attempt = async {
                let socket = TcpStream::connect(("127.0.0.1", port)).await?;
                connector
                    .connect(ServerName::try_from("localhost").unwrap(), socket)
                    .await
            };
            match attempt.await {
                Ok(stream) => break stream,
                Err(_) if started.elapsed() < WAIT => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("no V2 handshake on {port}: {error}"),
            }
        };
        Self {
            wire: Framed::new(stream, ClientCodec::default()),
            prefix: Vec::new(),
            job: None,
            next_id: 3,
        }
    }

    async fn event(&mut self) -> ClientEvent {
        let event = tokio::time::timeout(WAIT, self.wire.next())
            .await
            .expect("the pool answers")
            .expect("the pool keeps the connection")
            .unwrap();
        if let ClientEvent::Message(ServerMessage::Notify(job)) = &event {
            self.job = Some(job.clone());
        }
        event
    }

    async fn answer(&mut self, id: u64) -> Result<Value, (i64, String)> {
        loop {
            if let ClientEvent::Message(ServerMessage::Response {
                id: answered,
                outcome,
            }) = self.event().await
                && answered == json!(id)
            {
                return outcome;
            }
        }
    }

    async fn login(&mut self, login: &str) -> Value {
        let requests = [
            ClientMessage::Subscribe {
                pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
                spec_revision: crate::pow::tenq::SPEC_REVISION,
                id: json!(1),
                agent: "flow-v2/1".into(),
                tag: "flow".into(),
            },
            ClientMessage::Authorize {
                id: json!(2),
                login: login.into(),
                password: "x".into(),
            },
        ];
        for request in requests {
            self.wire.send(request).await.unwrap();
        }
        let subscribed = self.answer(1).await.unwrap();
        assert_eq!(
            subscribed["pow_algorithm_id"],
            crate::pow::tenq::ALGORITHM_ID
        );
        assert_eq!(subscribed["spec_revision"], crate::pow::tenq::SPEC_REVISION);
        assert_eq!(subscribed["chain_id"], CHAIN.to_hex());
        self.prefix = hex::decode(subscribed["nonce_prefix"].as_str().unwrap()).unwrap();
        assert_eq!(self.answer(2).await, Ok(json!(true)));
        while self.job.is_none() {
            self.event().await;
        }
        subscribed
    }

    fn nonce(&self, counter: u64) -> [u8; 32] {
        let mut nonce = [0_u8; 32];
        nonce[..8].copy_from_slice(&counter.to_le_bytes());
        nonce[32 - self.prefix.len()..].copy_from_slice(&self.prefix);
        nonce
    }

    async fn mine_block(&mut self) -> Hash384 {
        let hasher = hasher();
        for counter in 0.. {
            let job = self.job.clone().unwrap();
            let mut header: [u8; HEADER_BYTES] =
                hex::decode(&job.header).unwrap().try_into().unwrap();
            let nonce = self.nonce(counter);
            header[248..280].copy_from_slice(&nonce);
            let value = hasher.evaluate(&preimage(&header)).unwrap();
            let target: [u8; 32] = hex::decode(&job.target).unwrap().try_into().unwrap();
            let share: [u8; 32] = hex::decode(&job.share_target).unwrap().try_into().unwrap();
            if !meets(&value, &share) {
                continue;
            }
            let id = self.next_id;
            self.next_id += 1;
            let submit = ClientMessage::Submit {
                id: json!(id),
                job_id: job.job_id.clone(),
                nonce: hex::encode(nonce),
            };
            self.wire.send(submit).await.unwrap();
            assert_eq!(self.answer(id).await, Ok(json!(true)));
            if meets(&value, &target) {
                return block_id(&header);
            }
        }
        unreachable!()
    }
}

async fn text(url: String) -> Option<String> {
    let client = crate::tls::http_client(Duration::from_secs(5)).unwrap();
    client.get(url).send().await.ok()?.text().await.ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_v2_miner_mines_over_hybrid_tls_and_is_asked_back_on_stop() {
    let dir = temp_path("v2-tls");
    std::fs::create_dir_all(&dir).unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let (cert, key) = (dir.join("pool.crt"), dir.join("pool.key"));
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.key_pair.serialize_pem()).unwrap();
    let v2_port = free_port();
    let rig = rig_with(&[
        "--pplns-listen-v2-tls".into(),
        format!("127.0.0.1:{v2_port}"),
        "--tls-cert".into(),
        cert.to_str().unwrap().into(),
        "--tls-key".into(),
        key.to_str().unwrap().into(),
    ])
    .await;
    let (stop, pool) = start(&rig.config);
    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let miner = address(7);

    let mut v2 = V2Miner::connect(v2_port, roots).await;
    let subscribed = v2.login(&format!("{miner}.rig2")).await;
    assert_eq!(subscribed["protocol"], v2::PROTOCOL);
    assert_eq!(subscribed["pool_address"], address(POOL));
    until("the event stream", async || {
        (rig.node.listeners() > 0).then_some(())
    })
    .await;
    let block = v2.mine_block().await;
    until("the candidate", async || {
        rig.node.candidates().contains(&block).then_some(())
    })
    .await;

    let web = rig.config.web.unwrap();
    let view = until("the V2 worker", async || {
        let view = miner_view(&rig.config, &miner).await?;
        (view["workers"][0]["protocol"] == "v2").then_some(view)
    })
    .await;
    assert_eq!(view["workers"][0]["name"], "rig2");
    let summary = get_json(format!("http://{web}/api/v1/pool")).await.unwrap();
    let ports = summary["ports"].as_array().unwrap();
    assert!(
        ports
            .iter()
            .any(|port| port["protocol"] == "v2" && port["port"] == v2_port)
    );
    assert_eq!(summary["protocols"][1]["protocol"], "v2");
    assert_eq!(summary["protocols"][1]["connections"], 1);
    let metrics = text(format!("http://{web}/metrics")).await.unwrap();
    assert!(
        metrics.contains("tenrai_pool_connections{protocol=\"v2\"} 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("tenrai_pool_connections{protocol=\"v1\"} 0"),
        "{metrics}"
    );
    assert!(
        !metrics
            .contains("tenrai_pool_protocol_shares_total{protocol=\"v2\",result=\"accepted\"} 0")
    );

    let _ = stop.send(());
    let reconnect = loop {
        match v2.event().await {
            ClientEvent::Reconnect { host, port } => break (host, port),
            ClientEvent::Message(_) => {}
        }
    };
    assert_eq!(reconnect, (String::new(), 0));
    pool.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(dir);
    for suffix in ["", "-wal", "-shm"] {
        let mut name = rig.database.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}
