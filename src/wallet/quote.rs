//! Fee planning is unsigned; dry_run exports remain executable reserved transactions.
use super::*;
impl Wallet {
    pub async fn quote_send_many(
        &self,
        recipients: &[(String, u64)],
        fee: &str,
        subtract_fee: bool,
    ) -> Result<u64, WalletError> {
        let recipients: Vec<Value> = recipients
            .iter()
            .map(|(address, amount)| json!({"address":address,"amount":format_trai(*amount)}))
            .collect();
        let value = self
            .call(
                "quote_send_many",
                json!({"recipients":recipients,
            "fee":fee,"subtract_fee":subtract_fee}),
            )
            .await?;
        if value.get("unsigned").and_then(Value::as_bool) != Some(true)
            || value.get("submitted").and_then(Value::as_bool) != Some(false)
            || value.get("transaction").is_some()
            || value.get("wire_hex").is_some()
        {
            return Err(WalletError::Answer(
                "the daemon did not return an unsigned fee quote".into(),
            ));
        }
        let fee = value
            .get("fee")
            .and_then(Value::as_str)
            .ok_or_else(|| WalletError::Answer("the fee quote is missing its fee".into()))?;
        atoms(fee)
    }
}
