use super::*;

fn reward(state: &str, fees: &str, subsidy: Option<&str>) -> BlockReward {
    BlockReward {
        state: state.to_owned(),
        fees: fees.to_owned(),
        subsidy: subsidy.map(str::to_owned),
        payable_from_height: Some(12),
    }
}

#[test]
fn a_block_reward_counts_only_once_paid() {
    assert_eq!(reward("pending", "5", None).paid().unwrap(), None);
    assert_eq!(reward("queued", "5", Some("100")).paid().unwrap(), None);
    assert_eq!(reward("paid", "5", Some("100")).paid().unwrap(), Some(105));
    assert_eq!(reward("paid", "0", Some("0")).paid().unwrap(), Some(0));
    assert!(reward("paid", "5", Some("1.5")).paid().is_err());
    assert!(
        reward("paid", "1", Some(&u64::MAX.to_string()))
            .paid()
            .is_err()
    );
}

#[test]
fn a_block_view_without_a_reward_still_reads() {
    let view: BlockState = serde_json::from_value(serde_json::json!({
        "state": "registered", "registration_height": 6, "execution_height": null, "block_id": "ab"
    }))
    .unwrap();
    assert!(view.reward.is_none());
    assert_eq!(view.registration_height, Some(6));
}
