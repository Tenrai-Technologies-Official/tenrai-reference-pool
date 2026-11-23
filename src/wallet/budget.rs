//! A capability is issued only for the exact owned daemon scope, request and numeric signing cap.
use super::*;
#[derive(Debug, Error)]
#[error(
    "request {request} is durably closed without signing: fee {planned} exceeds cap {maximum} atoms"
)]
pub struct NeverSignedBudget {
    request: String,
    scope: RequestScope,
    planned: u128,
    maximum: u64,
}
impl NeverSignedBudget {
    pub(crate) fn request_id(&self) -> &str {
        &self.request
    }
    pub(crate) fn scope(&self) -> &RequestScope {
        &self.scope
    }
}
fn number(value: &Value, field: &str) -> Result<u128, WalletError> {
    let raw = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| WalletError::Answer(format!("budget receipt lacks {field}")))?;
    let number = raw
        .parse::<u128>()
        .map_err(|_| WalletError::Answer("invalid budget atoms".into()))?;
    if number.to_string() != raw {
        return Err(WalletError::Answer("noncanonical budget atoms".into()));
    }
    Ok(number)
}
pub(super) fn validate(
    value: &Value,
    request: &str,
    scope: &RequestScope,
    maximum: Option<u64>,
) -> Result<NeverSignedBudget, WalletError> {
    const FIELDS: &[&str] = &[
        "outcome",
        "reason",
        "request_id",
        "request_archive_id",
        "account",
        "chain_id",
        "fingerprint",
        "maximum_fee_atoms",
        "planned_fee_atoms",
    ];
    let object = value
        .as_object()
        .ok_or_else(|| WalletError::Answer("budget receipt is not an object".into()))?;
    if object.len() != FIELDS.len() || object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(WalletError::Answer(
            "unexpected or missing budget receipt field".into(),
        ));
    }
    let cap = maximum.ok_or_else(|| {
        WalletError::Answer("uncapped attempt cannot accept a budget rejection".into())
    })?;
    let fingerprint = value
        .get("fingerprint")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if value["outcome"] != "never_signed"
        || value["reason"] != "fee_limit_exceeded"
        || value["request_id"] != request
        || value["request_archive_id"] != scope.archive_id
        || value["account"].as_u64() != Some(u64::from(scope.account))
        || value["chain_id"] != scope.chain_id
        || Hash384::from_hex(fingerprint).is_none_or(|id| id.to_hex() != fingerprint)
        || value.get("transaction").is_some()
        || value.get("wire_hex").is_some()
        || number(value, "maximum_fee_atoms")? != u128::from(cap)
    {
        return Err(WalletError::Answer(
            "never-signed budget receipt scope or cap changed".into(),
        ));
    }
    let planned = number(value, "planned_fee_atoms")?;
    if planned <= u128::from(cap) {
        return Err(WalletError::Answer("budget was not exceeded".into()));
    }
    Ok(NeverSignedBudget {
        request: request.into(),
        scope: scope.clone(),
        planned,
        maximum: cap,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Value, RequestScope) {
        let scope = RequestScope {
            archive_id: "69".repeat(32),
            account: 0,
            chain_id: crate::test_support::CHAIN.to_hex(),
        };
        (
            json!({"outcome":"never_signed","reason":"fee_limit_exceeded","request_id":"budget-1",
            "request_archive_id":scope.archive_id,"account":scope.account,"chain_id":scope.chain_id,
            "fingerprint":"ab".repeat(48),"maximum_fee_atoms":"20","planned_fee_atoms":"21"}),
            scope,
        )
    }
    #[test]
    fn only_the_exact_owned_scope_cap_and_request_can_authorize_a_never_signed_result() {
        let (value, scope) = fixture();
        let receipt = validate(&value, "budget-1", &scope, Some(20)).unwrap();
        assert_eq!(receipt.request_id(), "budget-1");
        assert_eq!(receipt.scope(), &scope);
        assert!(validate(&value, "other-request", &scope, Some(20)).is_err());
        assert!(validate(&value, "budget-1", &scope, Some(21)).is_err());
        assert!(validate(&value, "budget-1", &scope, None).is_err());
        for (field, poison) in [
            ("account", json!(1)),
            ("chain_id", json!("cd".repeat(48))),
            ("request_archive_id", json!("cd".repeat(32))),
            ("fingerprint", json!("AB".repeat(48))),
            ("maximum_fee_atoms", json!("020")),
            ("planned_fee_atoms", json!("20")),
            ("reason", json!("not_found")),
            ("transaction", json!("ab".repeat(48))),
            ("wire_hex", json!("00")),
            ("transactions", json!([])),
            ("transaction_id", json!("ab".repeat(48))),
        ] {
            let mut changed = value.clone();
            changed[field] = poison;
            assert!(
                validate(&changed, "budget-1", &scope, Some(20)).is_err(),
                "{field}"
            );
        }
    }
}
