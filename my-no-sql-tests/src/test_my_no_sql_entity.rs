use my_no_sql_macros::*;
use serde::*;

#[my_no_sql_entity("instrument-mapping")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InstrumentMappingEntity {
    #[serde(rename = "LpId")]
    pub liquidity_provider_id: String,
}

#[cfg(test)]
mod test {
    use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;

    use super::InstrumentMappingEntity;

    #[test]
    fn test_serialize_deserialize() {
        let entity = InstrumentMappingEntity {
            partition_key: "Pk".to_string(),
            row_key: "Rk".to_string(),
            time_stamp: "".to_string(),
            liquidity_provider_id: "lp".to_string(),
        };

        let serialized = entity.serialize_entity();

        let res = InstrumentMappingEntity::deserialize_entity(&serialized).unwrap();

        assert_eq!(res.partition_key, entity.partition_key);
        assert_eq!(res.row_key, entity.row_key);
        assert_eq!(res.liquidity_provider_id, entity.liquidity_provider_id);
    }
}
