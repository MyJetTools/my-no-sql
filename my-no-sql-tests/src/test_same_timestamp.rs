use my_no_sql_macros::my_no_sql_entity;
use my_no_sql_sdk::abstractions::Timestamp;
use serde::*;

#[my_no_sql_entity("test-table")]
#[derive(Debug, Serialize, Deserialize)]
pub struct MyEntity {
    pub expires: Timestamp,
    pub ts: String,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use my_no_sql_sdk::{
        abstractions::MyNoSqlEntitySerializer,
        core::rust_extensions::date_time::DateTimeAsMicroseconds,
    };

    use super::MyEntity;

    #[test]
    fn test() {
        let entity = MyEntity {
            partition_key: "test".to_string(),
            row_key: "test".to_string(),
            time_stamp: Default::default(),
            expires: DateTimeAsMicroseconds::now()
                .add(Duration::from_secs(5))
                .into(),
            ts: "str".to_string(),
        };

        let result = entity.serialize_entity();

        let result = MyEntity::deserialize_entity(&result).unwrap();

        assert_eq!(entity.partition_key.as_str(), result.partition_key.as_str());
        assert_eq!(entity.row_key.as_str(), result.row_key.as_str());
        assert_eq!(entity.time_stamp, result.time_stamp);
        assert_eq!(entity.ts, result.ts);
    }
}
