use my_no_sql_abstractions::MyNoSqlEntity;
use serde::{de::DeserializeOwned, Serialize};

use crate::db_json_entity::DbJsonEntity;

pub fn serialize<TMyNoSqlEntity>(entity: &TMyNoSqlEntity) -> Vec<u8>
where
    TMyNoSqlEntity: MyNoSqlEntity + Serialize,
{
    serde_json::to_vec(&entity).unwrap()
}

pub fn deserialize<TMyNoSqlEntity>(data: &[u8]) -> TMyNoSqlEntity
where
    TMyNoSqlEntity: MyNoSqlEntity + DeserializeOwned,
{
    let parse_result: Result<TMyNoSqlEntity, _> = serde_json::from_slice(&data);

    match parse_result {
        Ok(el) => return el,
        Err(err) => {
            let db_entity = DbJsonEntity::parse(data);

            match db_entity {
                Ok(db_entity) => {
                    panic!(
                        "Table: {}. Can not parse entity with PartitionKey: [{}] and RowKey: [{}]. Err: {:?}",
                         TMyNoSqlEntity::TABLE_NAME, db_entity.partition_key, db_entity.row_key, err
                    );
                }
                Err(err) => {
                    panic!(
                        "Table: {}. Can not extract partitionKey and rowKey. Looks like entity broken at all. Err: {:?}",
                        TMyNoSqlEntity::TABLE_NAME, err
                    )
                }
            }
        }
    }
}

pub fn inject_partition_key_and_row_key(
    src: Vec<u8>,
    partition_key: &str,
    row_key: &str,
) -> Vec<u8> {
    let found_object_index = src.iter().position(|&x| x == b'{');

    if found_object_index.is_none() {
        panic!(
            "Can not find object start while injecting partitionKey:{partition_key} and rowKey:{row_key}"
        );
    }

    let found_object_index = found_object_index.unwrap();

    let to_insert = format!(
        "\"PartitionKey\":\"{}\",\"RowKey\":\"{}\",",
        my_json::EscapedJsonString::new(partition_key).as_str(),
        my_json::EscapedJsonString::new(row_key).as_str()
    )
    .into_bytes();

    let mut result = Vec::with_capacity(src.len() + partition_key.len());

    result.extend_from_slice(&src[..found_object_index + 1]);

    result.extend_from_slice(to_insert.as_slice());

    result.extend_from_slice(&src[found_object_index + 1..]);

    result
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_injection() {
        let src = r#"{"TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#;

        let injected = super::inject_partition_key_and_row_key(src.as_bytes().to_vec(), "PK", "RK");

        let dest = String::from_utf8(injected).unwrap();

        assert_eq!(
            r#"{"PartitionKey":"PK","RowKey":"RK","TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#,
            dest
        );
    }
}
