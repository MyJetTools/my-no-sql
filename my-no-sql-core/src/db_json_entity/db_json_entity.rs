use crate::db::DbRow;

use my_json::json_reader::array_parser::ArrayToJsonObjectsSplitter;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use std::{collections::BTreeMap, sync::Arc};

use super::DbEntityParseFail;
use super::JsonKeyValuePosition;
use super::JsonTimeStamp;
use super::KeyValueContentPosition;
use my_json::json_reader::JsonFirstLineReader;

pub struct DbJsonEntity {
    pub partition_key: JsonKeyValuePosition,
    pub row_key: JsonKeyValuePosition,
    pub expires: Option<JsonKeyValuePosition>,
    pub time_stamp: Option<JsonKeyValuePosition>,
    pub expires_value: Option<DateTimeAsMicroseconds>,
}

impl DbJsonEntity {
    pub fn parse(raw: &[u8]) -> Result<Self, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;

        let mut expires_value = None;

        for line in JsonFirstLineReader::new(&raw) {
            let line = line?;

            let name = line.get_name()?;
            match name {
                super::consts::PARTITION_KEY => {
                    partition_key = Some(JsonKeyValuePosition::new(&line));
                }

                super::consts::ROW_KEY => {
                    row_key = Some(JsonKeyValuePosition::new(&line));
                }
                super::consts::EXPIRES => {
                    expires_value = line.get_value()?.as_date_time();
                    expires = Some(JsonKeyValuePosition::new(&line))
                }
                super::consts::TIME_STAMP => {
                    time_stamp = Some(JsonKeyValuePosition::new(&line));
                }
                _ => {
                    if rust_extensions::str_utils::compare_strings_case_insensitive(
                        name,
                        super::consts::TIME_STAMP_LOWER_CASE,
                    ) {
                        time_stamp = Some(JsonKeyValuePosition::new(&line));
                    }
                }
            }
        }

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyIsRequired);
        }

        let partition_key = partition_key.unwrap();

        if partition_key.key.len() > 255 {
            return Err(DbEntityParseFail::PartitionKeyIsTooLong);
        }

        if partition_key.value.is_null(raw) {
            return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let row_key = row_key.unwrap();

        if row_key.value.is_null(raw) {
            return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
        }

        let result = Self {
            partition_key,
            row_key,
            expires,
            time_stamp,
            expires_value,
        };

        return Ok(result);
    }

    pub fn get_partition_key<'s>(&self, raw: &'s [u8]) -> &'s str {
        self.partition_key.value.get_str_value(raw)
    }

    pub fn get_row_key<'s>(&self, raw: &'s [u8]) -> &'s str {
        self.row_key.value.get_str_value(raw)
    }

    pub fn get_expires<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(expires) = &self.expires {
            return Some(expires.value.get_str_value(raw));
        }

        None
    }

    pub fn get_time_stamp<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(time_stamp) = &self.time_stamp {
            return Some(time_stamp.value.get_str_value(raw));
        }
        None
    }

    pub fn into_db_row(mut self, mut raw: Vec<u8>, inject_time_stamp: &JsonTimeStamp) -> DbRow {
        self.inject_time_stamp_if_requires(&mut raw, &inject_time_stamp);

        return DbRow::new(
            self,
            raw,
            #[cfg(feature = "master-node")]
            inject_time_stamp,
        );
    }

    pub fn restore_db_row(self, raw: Vec<u8>) -> DbRow {
        #[cfg(feature = "master-node")]
        let time_stamp = if let Some(time_stamp) = &self.time_stamp {
            JsonTimeStamp::parse_or_now(time_stamp.value.get_str_value(&raw))
        } else {
            JsonTimeStamp::now()
        };

        return DbRow::new(
            self,
            raw,
            #[cfg(feature = "master-node")]
            &time_stamp,
        );
    }

    pub fn parse_as_vec(
        src: &[u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        for json in src.split_array_json_to_objects() {
            let json = json?;
            let db_entity = DbJsonEntity::parse(json)?;
            let db_row = db_entity.into_db_row(json.to_vec(), inject_time_stamp);

            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn restore_as_vec(src: &[u8]) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        for json in src.split_array_json_to_objects() {
            let json = json?;
            let db_entity = DbJsonEntity::parse(json)?;
            let db_row = db_entity.restore_db_row(json.to_vec());

            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn parse_as_btreemap(
        src: &[u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<BTreeMap<String, Vec<Arc<DbRow>>>, DbEntityParseFail> {
        let mut result = BTreeMap::new();

        for json in src.split_array_json_to_objects() {
            let json = json?;
            let db_entity = DbJsonEntity::parse(json)?;
            let db_row = db_entity.into_db_row(json.to_vec(), inject_time_stamp);

            let partition_key = db_row.get_partition_key();
            if !result.contains_key(partition_key) {
                result.insert(partition_key.to_string(), Vec::new());
            }

            result
                .get_mut(partition_key)
                .unwrap()
                .push(Arc::new(db_row));
        }

        return Ok(result);
    }

    pub fn restore_as_btreemap(
        src: &[u8],
    ) -> Result<BTreeMap<String, Vec<Arc<DbRow>>>, DbEntityParseFail> {
        let mut result = BTreeMap::new();

        for json in src.split_array_json_to_objects() {
            let json = json?;
            let db_entity = DbJsonEntity::parse(json)?;
            let db_row = db_entity.restore_db_row(json.to_vec());

            let partition_key = db_row.get_partition_key();

            if !result.contains_key(partition_key) {
                result.insert(partition_key.to_string(), Vec::new());
            }

            result
                .get_mut(partition_key)
                .unwrap()
                .push(Arc::new(db_row));
        }

        return Ok(result);
    }

    fn inject_time_stamp_if_requires(&mut self, raw: &mut Vec<u8>, time_stamp: &JsonTimeStamp) {
        if self.time_stamp.is_some() {
            self.replace_timestamp_value(raw, time_stamp);
        } else {
            self.inject_at_the_end_of_json(raw, time_stamp);
        }
    }

    pub fn replace_timestamp_value(&mut self, raw: &mut Vec<u8>, json_time_stamp: &JsonTimeStamp) {
        let timestamp_value = format!("{dq}{val}{dq}", dq = '"', val = json_time_stamp.as_str());

        let timestamp_value = timestamp_value.as_bytes();

        let ts_as_bytes = super::consts::TIME_STAMP.as_bytes();

        let time_stamp_position = self.time_stamp.as_ref().unwrap();

        for i in 0..ts_as_bytes.len() {
            raw[time_stamp_position.key.start + 1 + i] = ts_as_bytes[i];
        }

        let content_timestamp_len = time_stamp_position.value.len();

        if content_timestamp_len < timestamp_value.len() {
            replace_timestamp(raw, time_stamp_position, json_time_stamp);
            return;
        }

        let mut no = 0;
        for i in time_stamp_position.value.start..time_stamp_position.value.end {
            if no < timestamp_value.len() {
                raw[i] = timestamp_value[no];
            } else {
                raw[i] = b' ';
            }

            no += 1;
        }
    }

    pub fn inject_at_the_end_of_json(&mut self, raw: &mut Vec<u8>, time_stamp: &JsonTimeStamp) {
        let end_of_json = get_the_end_of_the_json(raw);

        raw.truncate(end_of_json);

        raw.push(b',');
        self.time_stamp = inject_time_stamp_key_value(raw, time_stamp).into();
        raw.push(b'}');
    }
}

fn replace_timestamp(
    raw: &mut Vec<u8>,
    time_stamp_position: &JsonKeyValuePosition,
    time_stamp: &JsonTimeStamp,
) {
    let temp_buffer_len = raw.len() - time_stamp_position.value.end;
    let mut temp_buffer = Vec::with_capacity(temp_buffer_len);

    temp_buffer.extend_from_slice(raw.as_slice()[time_stamp_position.value.end..].as_ref());

    raw.truncate(time_stamp_position.key.start);

    inject_time_stamp_key_value(raw, time_stamp);

    raw.extend_from_slice(temp_buffer.as_slice());
}

fn inject_time_stamp_key_value(
    raw: &mut Vec<u8>,
    time_stamp: &JsonTimeStamp,
) -> JsonKeyValuePosition {
    let mut key = KeyValueContentPosition {
        start: raw.len(),
        end: 0,
    };

    raw.push(b'"');
    raw.extend_from_slice(super::consts::TIME_STAMP.as_bytes());
    raw.push(b'"');

    key.end = raw.len();

    raw.push(b':');

    let mut value = KeyValueContentPosition {
        start: raw.len(),
        end: 0,
    };

    raw.push(b'"');
    raw.extend_from_slice(time_stamp.as_slice());
    raw.push(b'"');

    value.end = raw.len();

    JsonKeyValuePosition { key, value }
}

pub fn get_the_end_of_the_json(data: &[u8]) -> usize {
    for i in (0..data.len()).rev() {
        if data[i] == my_json::json_reader::consts::CLOSE_BRACKET {
            return i;
        }
    }

    panic!("Invalid Json. Can not find the end of json");
}
#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbEntityParseFail, JsonTimeStamp};

    use super::DbJsonEntity;

    #[test]
    pub fn parse_expires_with_z() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
            "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let entity = DbJsonEntity::parse(src_json.as_bytes()).unwrap();

        let expires = entity.expires_value.as_ref().unwrap();

        assert_eq!("2022-03-17T13:28:29.653747", &expires.to_rfc3339()[..26]);

        let expires_value_position = entity.expires.unwrap();

        let expires_key =
            &src_json.as_bytes()[expires_value_position.key.start..expires_value_position.key.end];

        assert_eq!("\"Expires\"", std::str::from_utf8(expires_key).unwrap());

        let expires_value = &src_json.as_bytes()
            [expires_value_position.value.start..expires_value_position.value.end];

        assert_eq!(
            "\"2022-03-17T13:28:29.6537478Z\"",
            std::str::from_utf8(expires_value).unwrap()
        );
    }

    #[test]
    pub fn parse_with_partition_key_is_null() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": null,
            "RowKey": "test",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let result = DbJsonEntity::parse(src_json.as_bytes());

        if let Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull) = result {
        } else {
            panic!("Should not be here")
        }
    }
    #[test]
    pub fn parse_some_case_from_real_life() {
        let src_json = r#"{"value":{"is_enabled":true,"fee_percent":5.0,"min_balance_usd":100.0,"fee_period_days":30,"inactivity_period_days":90},"PartitionKey":"*","RowKey":"*"}"#;

        let result = DbJsonEntity::parse(src_json.as_bytes()).unwrap();

        let time_stamp = JsonTimeStamp::now();
        let db_row = result.into_db_row(src_json.as_bytes().to_vec(), &time_stamp);

        println!("{:?}", std::str::from_utf8(db_row.as_slice()).unwrap());
    }

    #[test]
    fn test_timestamp_injection_at_the_end_of_json() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let mut json = r#"{"PartitionKey":"PK", "RowKey":"RK"}     "#.as_bytes().to_vec();

        let mut db_json_entity = DbJsonEntity::parse(json.as_slice()).unwrap();

        db_json_entity.inject_at_the_end_of_json(&mut json, &json_ts);

        assert_eq!(db_json_entity.get_partition_key(&json), "PK");
        assert_eq!(db_json_entity.get_row_key(&json), "RK");

        assert_eq!(
            db_json_entity.get_time_stamp(&json).unwrap(),
            json_ts.as_str()
        );

        assert_eq!(
            std::str::from_utf8(json.as_slice()).unwrap(),
            format!(
                r#"{{"PartitionKey":"PK", "RowKey":"RK","TimeStamp":"{}"}}"#,
                json_ts.as_str()
            )
        );
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_which_has_less_size() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let mut json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":null}"#
            .as_bytes()
            .to_vec();

        let mut db_json_entity = DbJsonEntity::parse(json.as_slice()).unwrap();

        let dest_json_result = format!(
            "{}\"PartitionKey\":\"Pk\", \"RowKey\":\"Rk\", \"TimeStamp\":\"{}\"{}",
            '{',
            json_ts.as_str(),
            '}'
        );

        db_json_entity.inject_time_stamp_if_requires(&mut json, &json_ts);

        let dest_json = std::str::from_utf8(json.as_slice()).unwrap();

        assert_eq!(db_json_entity.get_partition_key(&json), "Pk",);
        assert_eq!(db_json_entity.get_row_key(&json), "Rk",);

        assert_eq!(dest_json_result, dest_json);
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_which_has_bigger_size() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let mut json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":"12345678901234567890123456789012345678901234567890"}"#
            .as_bytes()
            .to_vec();

        let mut db_json_entity = DbJsonEntity::parse(json.as_slice()).unwrap();

        let dest_json_result = format!(
            "{}\"PartitionKey\":\"Pk\", \"RowKey\":\"Rk\", \"TimeStamp\":\"{}\"                          {}",
            '{',
            json_ts.as_str(),
            '}'
        );

        db_json_entity.inject_time_stamp_if_requires(&mut json, &json_ts);

        let dest_json = std::str::from_utf8(json.as_slice()).unwrap();

        assert_eq!(db_json_entity.get_partition_key(&json), "Pk",);
        assert_eq!(db_json_entity.get_row_key(&json), "Rk",);

        assert_eq!(dest_json_result, dest_json);
    }
}
