use super::*;
use crate::test_support::CHAIN;
use serde_json::json;

fn head(height: u64) -> SettlementHead {
    serde_json::from_value(json!({"chain_id":CHAIN.to_hex(),"height":height,"cut_id":"11".repeat(48),
        "state_root":"22".repeat(48),"validator_set_id":"33".repeat(48),"params_hash":"44".repeat(48)})).unwrap()
}
fn reward(height: u64) -> WorkReward {
    WorkReward {
        amount: 1_000_000,
        created: 20,
        anchor: serde_json::from_value(json!({"height":height,
        "cut_id":"11".repeat(48),"state_root":"22".repeat(48),"validator_set_id":"33".repeat(48)}))
        .unwrap(),
    }
}

#[test]
fn verified_positive_rewards_require_same_chain_nonrollback_and_exact_equal_height_context() {
    assert!(coherent(&head(100), &head(100), &reward(100), CHAIN));
    assert!(coherent(&head(99), &head(101), &reward(100), CHAIN));
    assert!(!coherent(&head(101), &head(100), &reward(100), CHAIN));
    assert!(!coherent(&head(100), &head(100), &reward(99), CHAIN));
    assert!(!coherent(&head(100), &head(100), &reward(101), CHAIN));
    assert!(!coherent(
        &head(100),
        &head(100),
        &reward(100),
        Hash384([99; 48])
    ));
    let mut after = head(100);
    after.state_root = "55".repeat(48);
    assert!(!coherent(&head(100), &after, &reward(100), CHAIN));
    let mut bad = reward(100);
    bad.anchor.validator_set_id = "55".repeat(48);
    assert!(!coherent(&head(100), &head(101), &bad, CHAIN));
}

#[test]
fn recovery_uses_the_original_header_lock_even_if_the_receive_address_changed() {
    let mut header = [0; HEADER_BYTES];
    header[..48].copy_from_slice(&CHAIN.0);
    header[207] = 1;
    header[208..248].fill(7);
    let id = block_id(&header);
    let original = original_address(&header, id, CHAIN, Network::Development).unwrap();
    assert_ne!(original, crate::test_support::address(1));
    assert_eq!(
        crate::chain::parse_address(&original, Network::Development).unwrap(),
        reward_lock(&header)
    );
    assert!(original_address(&header, Hash384([0; 48]), CHAIN, Network::Development).is_none());
    assert!(original_address(&header, id, Hash384([0; 48]), Network::Development).is_none());
    header[207] = 3;
    assert!(original_address(&header, block_id(&header), CHAIN, Network::Development).is_none());
}

#[tokio::test]
async fn replaying_a_recovered_old_reward_books_the_original_block_only_once_after_reopen() {
    use crate::{
        config::Mode,
        db::accepted::{FoundWork, tests::request},
        db::{self, Op},
        test_support::temp_path,
    };
    let path = temp_path("pool-old-work-credit").with_extension("sqlite");
    let (writer, reader, physical) = db::open(&path).unwrap();
    let mut work = request(1, Mode::Pplns, "original-miner");
    work.found = Some(FoundWork {
        header: work.header,
        body_hex: "00".into(),
        height: 10,
        window_end: 100,
        network_difficulty: 1_000,
        window_work: 1_000,
        fee_bps: 0,
    });
    let receipt = writer.accept(work).await.unwrap();
    let block = receipt.block.unwrap();
    let evidence = reward(100);
    assert!(coherent(&head(100), &head(100), &evidence, CHAIN));
    let credit = |done| Op::Credit {
        cut: evidence.created,
        blocks: vec![crate::db::BlockCredit {
            id: block.id,
            reward: evidence.amount,
            fee: 0,
            credits: vec![(receipt.miner, evidence.amount)],
        }],
        at: 1_000,
        done,
    };
    assert_eq!(writer.call(credit).await.unwrap(), 1);
    assert_eq!(writer.call(credit).await.unwrap(), 0);
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || physical.join().unwrap())
        .await
        .unwrap();
    let (writer, reader, physical) = db::open(&path).unwrap();
    assert_eq!(writer.call(credit).await.unwrap(), 0);
    let miner = receipt.miner;
    let amount = reader
        .read(move |connection| {
            connection.query_row(
                "SELECT unpaid FROM balances WHERE miner_id=?1",
                [miner],
                |row| row.get::<_, u64>(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(amount, evidence.amount);
    let payloads = reader
        .read(|connection| {
            connection.query_row("SELECT COUNT(*) FROM found_payload", [], |row| {
                row.get::<_, u64>(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        payloads, 0,
        "only positive conditional credit retires the original outbox payload"
    );
    let receipts = reader
        .read(|connection| {
            connection.query_row("SELECT COUNT(*) FROM accepted_work", [], |row| {
                row.get::<_, u64>(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        receipts, 1,
        "positive reward settlement does not erase canonical accepted-share replay protection"
    );
    writer.shutdown().await.unwrap();
    drop(reader);
    drop(writer);
    tokio::task::spawn_blocking(move || physical.join().unwrap())
        .await
        .unwrap();
    for suffix in ["", "-wal", "-shm", ".pool-owner.lock"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(name));
    }
}
