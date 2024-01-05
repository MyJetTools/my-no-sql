/*
pub fn compile_data_with_new_expires(
    db_row: &DbRow,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    if let Some(expires_position) = &db_row.expires_json_position {
        replace_expires(db_row, expires_position, value)
    } else {
        inject_expires_at_the_end_of_content(db_row, value)
    }
}

pub fn remove_expiration_time(db_row: &DbRow) -> Vec<u8> {
    let db_row_content = db_row.as_slice();

    if let Some(expires_position) = &db_row.expires_json_position {
        let mut result = Vec::new();

        result.extend_from_slice(&db_row_content[..expires_position.key.start - 1]);

        let mut comma_pos = None;
        for i in expires_position.value.end..db_row.content_len() {
            if db_row_content[i] == b',' {
                comma_pos = Some(i);
                break;
            }
        }

        if let Some(comma_pos) = comma_pos {
            result.extend_from_slice(&db_row_content[comma_pos + 1..]);
        } else {
            result.extend_from_slice(&db_row_content[expires_position.value.end + 1..]);
        }

        result
    } else {
        db_row_content.to_vec()
    }
}

fn replace_expires(
    db_row: &DbRow,
    expires_position: &JsonKeyValuePosition,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    let mut result = Vec::new();

    let mut json_key_value_position = expires_position.clone();

    let db_row_content = db_row.as_slice();

    result.extend_from_slice(&db_row_content[..expires_position.key.start + 1]);
    result.extend_from_slice("Expires\":\"".as_bytes());

    json_key_value_position.value.start = result.len() - 1;
    result.extend_from_slice(value.as_bytes());
    json_key_value_position.value.end = result.len() + 1;
    result.extend_from_slice(&db_row_content[expires_position.value.end - 1..]);
    (result, json_key_value_position)
}

fn inject_expires_at_the_end_of_content(
    db_row: &DbRow,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    let mut json_key_value_position = JsonKeyValuePosition {
        key: KeyValueContentPosition { start: 0, end: 0 },
        value: KeyValueContentPosition { start: 0, end: 0 },
    };

    let db_row_content = db_row.as_slice();

    let mut i = 0;
    for b in db_row_content {
        if *b == b'{' {
            break;
        }

        i += 1;
    }

    i += 1;

    let mut result = Vec::new();

    result.extend_from_slice(&db_row_content[..i]);
    json_key_value_position.key.start = result.len();
    result.extend_from_slice("\"Expires\"".as_bytes());
    json_key_value_position.key.end = result.len();
    result.extend_from_slice(":\"".as_bytes());

    json_key_value_position.value.start = result.len() - 1;

    result.extend_from_slice(value.as_bytes());

    json_key_value_position.value.end = result.len() + 1;
    result.extend_from_slice("\",".as_bytes());

    result.extend_from_slice(&db_row_content[i..]);
    (result, json_key_value_position)
}
 */

#[cfg(test)]
mod test {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    #[test]
    fn test_replace_existing_expires() {
        let test_json = r#"{
            "PartitionKey": "TestPk",
            "RowKey": "TestRk",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        let new_expires = DateTimeAsMicroseconds::from_str("2020-01-02T01:02:03").unwrap();

        db_row.update_expires(new_expires.into());

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "TestPk");
        assert_eq!(result_entity.get_row_key(&result_json), "TestRk");

        assert_eq!(
            result_entity.get_expires(&result_json).unwrap(),
            &new_expires.to_rfc3339()[..19]
        );
    }

    #[test]
    fn test_injecting_expires() {
        let test_json = r#"{
            "PartitionKey": "Pk",
            "RowKey": "Rk"
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        let new_expires = DateTimeAsMicroseconds::from_str("2020-01-02T01:02:03").unwrap();

        db_row.update_expires(new_expires.into());

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "Pk");
        assert_eq!(result_entity.get_row_key(&result_json), "Rk");

        assert_eq!(
            result_entity.get_expires(&result_json).unwrap(),
            &new_expires.to_rfc3339()[..19]
        );
    }

    #[test]
    fn test_remove_expiration_time() {
        let test_json =
            r#"{"PartitionKey": "Pk","RowKey": "Rk", "Expires": "2019-01-01T00:00:00"}"#
                .as_bytes()
                .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        db_row.update_expires(None);

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        println!("Result: {}", std::str::from_utf8(&result_json).unwrap());

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "Pk");
        assert_eq!(result_entity.get_row_key(&result_json), "Rk");

        assert!(result_entity.get_expires(&result_json).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_begin() {
        let test_json = r#"{"Expires": "2019-01-01T00:00:00","PartitionKey":"Pk","RowKey":"Rk"}"#
            .as_bytes()
            .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        db_row.update_expires(None);

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        println!("Result: {}", std::str::from_utf8(&result_json).unwrap());

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "Pk");
        assert_eq!(result_entity.get_row_key(&result_json), "Rk");

        assert!(result_entity.get_expires(&result_json).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_begin_and_space_after_expire() {
        let test_json = r#"{"Expires": "2019-01-01T00:00:00",  "PartitionKey":"Pk","RowKey":"Rk"}"#
            .as_bytes()
            .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        db_row.update_expires(None);

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        println!("Result: {}", std::str::from_utf8(&result_json).unwrap());

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "Pk");
        assert_eq!(result_entity.get_row_key(&result_json), "Rk");

        assert!(result_entity.get_expires(&result_json).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_the_middle() {
        let test_json = r#"{"PartitionKey": "Pk",
            "Expires": "2019-01-01T00:00:00",
            "RowKey": "Rk"}"#
            .as_bytes()
            .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.into_db_row(test_json, &inject_time_stamp);

        db_row.update_expires(None);

        let mut result_json = Vec::new();

        db_row.compile_json(&mut result_json);

        println!("Result: {}", std::str::from_utf8(&result_json).unwrap());

        let result_entity = DbJsonEntity::parse(&result_json).unwrap();

        assert_eq!(result_entity.get_partition_key(&result_json), "Pk");
        assert_eq!(result_entity.get_row_key(&result_json), "Rk");

        assert!(result_entity.get_expires(&result_json).is_none());
    }
}

//todo!("Create Test where we have partitionKey and RowKey after Timestamp");
