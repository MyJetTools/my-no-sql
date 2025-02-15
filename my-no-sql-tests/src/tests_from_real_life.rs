use my_no_sql_macros::my_no_sql_entity;
use serde::*;

#[my_no_sql_entity("payout-withdrawal-settings")]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PayoutWithdrawalSettingsMyNoSqlEntity {
    pub value: f64,
    pub currency: String,
}

#[cfg(test)]
mod tests {

    use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;

    use super::PayoutWithdrawalSettingsMyNoSqlEntity;

    #[test]
    fn test() {
        let src = "{\"PartitionKey\":\"bank-transfer\",\"RowKey\":\"max\",\"TimeStamp\":\"2024-11-29T14:59:15.6145\",\"Value\":15000.0,\"Currency\":\"USD\"}";

        let entity =
            PayoutWithdrawalSettingsMyNoSqlEntity::deserialize_entity(src.as_bytes()).unwrap();

        assert_eq!(entity.value, 15000.0);
        assert_eq!(entity.currency, "USD");
    }
}
