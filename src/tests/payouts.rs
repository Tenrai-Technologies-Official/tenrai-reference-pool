use super::*;

#[test]
fn a_subtracted_fee_is_shared_like_the_wallet_shares_it() {
    assert_eq!(fee_shares(10, &[4, 5, 6]), vec![(4, 4), (5, 3), (6, 3)]);
    assert_eq!(fee_shares(9, &[4, 5, 6]), vec![(4, 3), (5, 3), (6, 3)]);
    assert_eq!(fee_shares(1, &[4, 5]), vec![(4, 1), (5, 0)]);
    assert!(fee_shares(5, &[]).is_empty());
}

#[test]
fn a_round_pays_what_the_wallet_can_cover() {
    let batch = vec![
        (1, "a".to_owned(), 500),
        (2, "b".to_owned(), 300),
        (3, "c".to_owned(), 200),
    ];
    assert_eq!(fit(&batch, 2_000, 100).len(), 3);
    assert_eq!(
        fit(&batch, 650, 100),
        vec![(1, "a".to_owned(), 500), (2, "b".to_owned(), 150)]
    );
    assert_eq!(fit(&batch, 550, 100), vec![(1, "a".to_owned(), 500)]);
    assert!(fit(&batch, 50, 100).is_empty());
}

#[test]
fn payouts_skip_addresses_the_pool_cannot_pay() {
    use crate::{
        chain::{Network, address_text},
        test_support::{address, lock},
    };
    let pool = address(1);
    let miner = address(2);
    let foreign = address_text(&lock(3), Network::Mainnet);
    let payable = vec![
        (1, miner.clone(), 500),
        (2, "dtrai1qqqq".to_owned(), 400),
        (3, pool.clone(), 300),
        (4, foreign, 200),
        (5, "dtrai1\u{1b}[31m".to_owned(), 100),
    ];
    let (paid, refused) = payable_to(payable, Network::Development, &pool);
    assert_eq!(paid, vec![(1, miner, 500)]);
    assert_eq!(refused.len(), 4);
    assert!(refused.iter().all(|shown| !shown.contains('\u{1b}')));
}

fn status(lifecycle: &str, cut: Option<u64>, failure: Option<&str>) -> Option<TransactionStatus> {
    Some(TransactionStatus {
        lifecycle: lifecycle.to_owned(),
        finalized_cut: cut.map(|height| crate::node::CutRef { height }),
        failure: failure.map(str::to_owned),
    })
}

#[test]
fn only_the_node_finalizing_or_failing_it_for_good_closes_a_payout() {
    assert_eq!(
        verdict(status("finalized", Some(70), None), 80, Some(100)),
        Verdict::Paid(70)
    );
    assert_eq!(
        verdict(status("failed", Some(70), Some("conflict")), 80, Some(100)),
        Verdict::Failed("conflict".into())
    );
    assert_eq!(
        verdict(status("failed", None, Some("dropped")), 80, Some(100)),
        Verdict::Open
    );
    assert_eq!(
        verdict(status("failed", Some(70), Some("dropped")), 80, Some(100)),
        Verdict::Open
    );
    assert_eq!(verdict(None, 104, Some(100)), Verdict::Open);
}

#[test]
fn an_unknown_or_dropped_payout_expires_only_after_its_validity() {
    assert_eq!(verdict(None, 105, Some(100)), Verdict::Expired);
    assert_eq!(
        verdict(status("failed", None, Some("dropped")), 105, Some(100)),
        Verdict::Expired
    );
    assert_eq!(verdict(None, 1_000, None), Verdict::Open);
    for pending in ["processing", "receiving", "registered"] {
        assert_eq!(
            verdict(status(pending, None, None), 1_000, Some(100)),
            Verdict::Open,
            "{pending}"
        );
    }
}
