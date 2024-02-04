use crate::db::DbRow;

use my_json::json_reader::array_iterator::JsonArrayIterator;
use rust_extensions::array_of_bytes_iterator::SliceIterator;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use std::sync::Arc;

use super::DbEntityParseFail;
use super::DbJsonEntityWithContent;
use super::DbRowContentCompiler;
use super::JsonKeyValuePosition;
use super::JsonTimeStamp;
use super::KeyValueContentPosition;
use my_json::json_reader::JsonFirstLineReader;

pub struct DbJsonEntity {
    pub partition_key: JsonKeyValuePosition,
    pub row_key: JsonKeyValuePosition,
    pub time_stamp: Option<JsonKeyValuePosition>,
    pub expires: Option<JsonKeyValuePosition>,
    pub expires_value: Option<DateTimeAsMicroseconds>,
}

impl DbJsonEntity {
    pub fn from_slice(src: &[u8]) -> Result<Self, DbEntityParseFail> {
        let slice_iterator = SliceIterator::new(src);
        Self::new(JsonFirstLineReader::new(slice_iterator))
    }
    pub fn new(
        mut json_first_line_reader: JsonFirstLineReader<SliceIterator>,
    ) -> Result<Self, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;

        let mut expires_value = None;

        while let Some(line) = json_first_line_reader.get_next() {
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
        let raw = json_first_line_reader.get_src_slice();

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

        Ok(result)
    }

    pub fn parse<'s>(
        raw: &'s [u8],
        time_stamp_to_inject: &'s JsonTimeStamp,
    ) -> Result<DbJsonEntityWithContent<'s>, DbEntityParseFail> {
        let slice_iterator = SliceIterator::new(raw);
        let entity = Self::new(JsonFirstLineReader::new(slice_iterator))?;

        return Ok(DbJsonEntityWithContent::new(
            raw,
            time_stamp_to_inject,
            entity,
        ));
    }

    pub fn parse_into_db_row(
        mut json_first_line_reader: JsonFirstLineReader<SliceIterator>,
        now: &JsonTimeStamp,
    ) -> Result<DbRow, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;
        let mut expires_value = None;

        let mut raw = DbRowContentCompiler::new(json_first_line_reader.get_src_slice().len());

        while let Some(line) = json_first_line_reader.get_next() {
            let line = line?;

            let name = line.get_name()?;
            match name {
                super::consts::PARTITION_KEY => {
                    partition_key = Some(raw.append(&line));
                }

                super::consts::ROW_KEY => {
                    row_key = Some(raw.append(&line));
                    time_stamp = raw
                        .append_str_value(super::consts::TIME_STAMP, now.as_str())
                        .into();
                }
                super::consts::EXPIRES => {
                    expires_value = line.get_value()?.as_date_time();
                    expires = Some(raw.append(&line));
                }
                super::consts::TIME_STAMP => {}
                _ => {
                    if rust_extensions::str_utils::compare_strings_case_insensitive(
                        name,
                        super::consts::TIME_STAMP_LOWER_CASE,
                    ) {
                    } else {
                        raw.append(&line);
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

        if partition_key
            .value
            .is_null(json_first_line_reader.get_src_slice())
        {
            return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let row_key = row_key.unwrap();

        if row_key
            .value
            .is_null(json_first_line_reader.get_src_slice())
        {
            return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
        }

        let db_json_entity = Self {
            partition_key,
            row_key,
            expires,
            time_stamp,
            expires_value,
        };

        let result = DbRow::new(db_json_entity, raw.into_vec());

        Ok(result)
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

    pub fn restore_into_db_row(
        json_first_line_reader: JsonFirstLineReader<SliceIterator>,
    ) -> Result<DbRow, DbEntityParseFail> {
        let raw = json_first_line_reader.get_src_slice().to_vec();
        let db_row = Self::new(json_first_line_reader)?;
        let result = DbRow::new(db_row, raw);
        Ok(result)
    }

    pub fn parse_as_vec(
        src: &[u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        let slice_iterator = SliceIterator::new(src);

        let mut json_array_iterator = JsonArrayIterator::new(slice_iterator);

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::parse_into_db_row(
                json.unwrap_as_object().unwrap(),
                inject_time_stamp,
            )?;
            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn restore_as_vec(src: &[u8]) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        let slice_iterator = SliceIterator::new(src);

        let mut json_array_iterator = JsonArrayIterator::new(slice_iterator);

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_entity = DbJsonEntity::restore_into_db_row(json.unwrap_as_object().unwrap())?;
            result.push(Arc::new(db_entity));
        }
        return Ok(result);
    }

    pub fn parse_grouped_by_partition_key<'s>(
        src: &'s [u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<(String, Vec<Arc<DbRow>>)>, DbEntityParseFail> {
        let mut result = Vec::new();

        let slice_iterator = SliceIterator::new(src);

        let mut json_array_iterator = JsonArrayIterator::new(slice_iterator);

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::parse_into_db_row(
                json.unwrap_as_object().unwrap(),
                inject_time_stamp,
            )?;

            let partition_key = db_row.get_partition_key();

            match result.binary_search_by(|itm: &(String, Vec<Arc<DbRow>>)| {
                itm.0.as_str().cmp(partition_key)
            }) {
                Ok(index) => {
                    result[index].1.push(Arc::new(db_row));
                }
                Err(index) => {
                    result.insert(index, (partition_key.to_string(), vec![Arc::new(db_row)]));
                }
            }
        }

        Ok(result)
    }

    pub fn restore_grouped_by_partition_key(
        src: &[u8],
    ) -> Result<Vec<(String, Vec<Arc<DbRow>>)>, DbEntityParseFail> {
        let mut result = Vec::new();

        let slice_iterator = SliceIterator::new(src);
        let mut json_array_iterator = JsonArrayIterator::new(slice_iterator);

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::restore_into_db_row(json.unwrap_as_object().unwrap())?;

            let partition_key = db_row.get_partition_key();

            match result.binary_search_by(|itm: &(String, Vec<Arc<DbRow>>)| {
                itm.0.as_str().cmp(partition_key)
            }) {
                Ok(index) => {
                    result[index].1.push(Arc::new(db_row));
                }
                Err(index) => {
                    result.insert(index, (partition_key.to_string(), vec![Arc::new(db_row)]));
                }
            }
        }

        return Ok(result);
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

    use my_json::json_reader::JsonFirstLineReader;
    use rust_extensions::{
        array_of_bytes_iterator::SliceIterator, date_time::DateTimeAsMicroseconds,
    };

    use crate::db_json_entity::{DbEntityParseFail, JsonTimeStamp};

    use super::DbJsonEntity;

    #[test]
    pub fn test_partition_key_and_row_key_and_time_stamp_are_ok() {
        let src_json = r#"{"TwoFaMethods": {},
        "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
        "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
        "TimeStamp": "2022-03-17T09:28:27.5923",
        "Expires": "2022-03-17T13:28:29.6537478Z"
      }"#;

        let json_first_line_reader: JsonFirstLineReader<SliceIterator> = src_json.into();

        let json_time = JsonTimeStamp::now();

        let entity = DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_time).unwrap();

        let json_first_line_reader: JsonFirstLineReader<SliceIterator> =
            entity.get_src_as_slice().into();

        let dest_entity =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_time).unwrap();

        assert_eq!(
            "ff95cdae9f7e4f1a847f6b83ad68b495",
            dest_entity.get_partition_key()
        );

        assert_eq!(
            "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            dest_entity.get_row_key()
        );
    }

    #[test]
    pub fn parse_expires_with_z() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
            "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let json_first_line_reader: JsonFirstLineReader<SliceIterator<'_>> = src_json.into();

        let entity = DbJsonEntity::new(json_first_line_reader).unwrap();

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

        let slice_iterator = SliceIterator::new(src_json.as_bytes());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);

        let result = DbJsonEntity::new(json_first_line_reader);

        if let Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull) = result {
        } else {
            panic!("Should not be here")
        }
    }
    #[test]
    pub fn parse_some_case_from_real_life() {
        let src_json = r#"{"value":{"is_enabled":true,"fee_percent":5.0,"min_balance_usd":100.0,"fee_period_days":30,"inactivity_period_days":90},"PartitionKey":"*","RowKey":"*"}"#;

        let time_stamp = JsonTimeStamp::now();
        let slice_iterator = SliceIterator::new(src_json.as_bytes());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);
        let db_row = DbJsonEntity::parse_into_db_row(json_first_line_reader, &time_stamp).unwrap();

        println!(
            "{:?}",
            std::str::from_utf8(db_row.get_src_as_slice()).unwrap()
        );
    }

    #[test]
    fn test_timestamp_injection_at_the_end_of_json() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let mut json = r#"{"PartitionKey":"PK", "RowKey":"RK"}     "#.as_bytes().to_vec();

        let slice_iterator = SliceIterator::new(json.as_slice());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);

        let mut db_json_entity = DbJsonEntity::new(json_first_line_reader).unwrap();

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

        let json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":null}"#;

        let slice_iterator = SliceIterator::new(json.as_bytes());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);

        let db_row = DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_ts).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk",);
        assert_eq!(db_row.get_row_key(), "Rk",);
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_which_has_bigger_size() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":"12345678901234567890123456789012345678901234567890"}"#;

        let slice_iterator = SliceIterator::new(json.as_bytes());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);

        let db_json_entity =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_ts).unwrap();

        assert_eq!(db_json_entity.get_partition_key(), "Pk",);
        assert_eq!(db_json_entity.get_row_key(), "Rk",);

        assert_eq!(db_json_entity.get_row_key(), "Rk",);
    }

    #[test]
    fn test_we_have_timestamp_before_partition_key() {
        let test_json = r#"{
            "Timestamp":"",
            "PartitionKey": "Pk",
            "Expires": "2019-01-01T00:00:00",
            "RowKey": "Rk"}"#;

        let inject_time_stamp = JsonTimeStamp::now();

        let slice_iterator = SliceIterator::new(test_json.as_bytes());
        let json_first_line_reader = JsonFirstLineReader::new(slice_iterator);

        let db_row =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &inject_time_stamp).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk");
        assert_eq!(db_row.get_row_key(), "Rk");

        #[cfg(feature = "master-node")]
        assert_eq!(
            db_row.get_expires().unwrap().unix_microseconds,
            DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00")
                .unwrap()
                .unix_microseconds
        );
    }
}
