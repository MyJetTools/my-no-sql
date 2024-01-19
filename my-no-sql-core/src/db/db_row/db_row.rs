use std::sync::Arc;

use my_json::json_writer::JsonObject;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::sorted_vec::EntityWithStrKey;

use crate::db::PartitionKeyParameter;
use crate::db_json_entity::DbJsonEntity;

use super::RowKeyParameter;

pub struct DbRow {
    partition_key: crate::db_json_entity::KeyValueContentPosition,
    row_key: crate::db_json_entity::KeyValueContentPosition,
    raw: Vec<u8>,
    #[cfg(feature = "master-node")]
    expires_value: AtomicDateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    expires: Option<crate::db_json_entity::JsonKeyValuePosition>,
    #[cfg(feature = "master-node")]
    pub time_stamp: crate::db_json_entity::KeyValueContentPosition,
    #[cfg(feature = "master-node")]
    last_read_access: AtomicDateTimeAsMicroseconds,
}

impl DbRow {
    pub fn new(db_json_entity: DbJsonEntity, raw: Vec<u8>) -> Self {
        #[cfg(feature = "debug_db_row")]
        println!(
            "Created DbRow: PK:{}. RK:{}. Expires{:?}",
            db_json_entity.get_partition_key(raw.as_slice()),
            db_json_entity.get_row_key(raw.as_slice()),
            db_json_entity.expires
        );

        #[cfg(feature = "master-node")]
        let time_stamp = db_json_entity.time_stamp.unwrap();
        #[cfg(feature = "master-node")]
        let time_stamp_value =
            DateTimeAsMicroseconds::from_str(time_stamp.value.get_str_value(&raw)).unwrap();

        Self {
            raw,
            partition_key: db_json_entity.partition_key.value,
            row_key: db_json_entity.row_key.value,
            #[cfg(feature = "master-node")]
            time_stamp: time_stamp.value,
            #[cfg(feature = "master-node")]
            expires_value: if let Some(expires_value) = db_json_entity.expires_value {
                AtomicDateTimeAsMicroseconds::new(expires_value.unix_microseconds)
            } else {
                AtomicDateTimeAsMicroseconds::new(0)
            },
            #[cfg(feature = "master-node")]
            expires: db_json_entity.expires,
            #[cfg(feature = "master-node")]
            last_read_access: AtomicDateTimeAsMicroseconds::new(time_stamp_value.unix_microseconds),
        }
    }

    pub fn get_partition_key(&self) -> &str {
        self.partition_key.get_str_value(&self.raw)
    }

    pub fn get_row_key(&self) -> &str {
        self.row_key.get_str_value(&self.raw)
    }

    pub fn get_time_stamp(&self) -> &str {
        self.row_key.get_str_value(&self.raw)
    }
    pub fn get_src_as_slice(&self) -> &[u8] {
        self.raw.as_slice()
    }

    #[cfg(feature = "master-node")]
    pub fn update_last_read_access(
        &self,
        value: rust_extensions::date_time::DateTimeAsMicroseconds,
    ) {
        self.last_read_access.update(value);
    }
    #[cfg(feature = "master-node")]
    pub fn get_last_read_access(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        self.last_read_access.as_date_time()
    }

    #[cfg(feature = "master-node")]
    pub fn update_expires(
        &self,
        expires: Option<DateTimeAsMicroseconds>,
    ) -> Option<DateTimeAsMicroseconds> {
        let old_value = self.get_expires();

        if let Some(expires) = expires {
            self.expires_value.update(expires);
        } else {
            self.expires_value.update(DateTimeAsMicroseconds::new(0));
        }

        old_value
    }
    #[cfg(feature = "master-node")]
    pub fn get_expires(&self) -> Option<DateTimeAsMicroseconds> {
        let result = self.expires_value.as_date_time();

        if result.unix_microseconds == 0 {
            None
        } else {
            Some(result)
        }
    }
    #[cfg(feature = "master-node")]
    pub fn write_json(&self, out: &mut Vec<u8>) {
        let expires_value = self.get_expires();

        if expires_value.is_none() {
            if let Some(expires) = &self.expires {
                if let Some(before_separator) =
                    find_json_separator_before(&self.raw, expires.key.start - 1)
                {
                    out.extend_from_slice(&self.raw[..before_separator]);
                    out.extend_from_slice(&self.raw[expires.value.end..]);
                    return;
                }

                if let Some(after_separator) =
                    find_json_separator_after(&self.raw, expires.value.end)
                {
                    out.extend_from_slice(&self.raw[..expires.key.start]);
                    out.extend_from_slice(&self.raw[after_separator..]);
                    return;
                }

                out.extend_from_slice(&self.raw[..expires.key.start]);
                out.extend_from_slice(&self.raw[expires.value.end..]);
            } else {
                out.extend_from_slice(&self.raw);
            }

            return;
        }

        let expires_value = expires_value.unwrap();

        if let Some(expires) = &self.expires {
            out.extend_from_slice(&self.raw[..expires.key.start]);
            inject_expires(out, expires_value);
            out.extend_from_slice(&self.raw[expires.value.end..]);
        } else {
            let end_of_json = crate::db_json_entity::get_the_end_of_the_json(&self.raw);
            out.extend_from_slice(&self.raw[..end_of_json]);
            out.push(b',');
            inject_expires(out, expires_value);
            out.extend_from_slice(&self.raw[end_of_json..]);
        }
    }

    #[cfg(not(feature = "master-node"))]
    pub fn write_json(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.raw);
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = Vec::new();
        self.write_json(&mut result);
        result
    }
}

impl EntityWithStrKey for DbRow {
    fn get_key(&self) -> &str {
        self.get_row_key()
    }
}

impl PartitionKeyParameter for Arc<DbRow> {
    fn as_str(&self) -> &str {
        self.get_partition_key()
    }

    fn into_partition_key(self) -> crate::db::PartitionKey {
        self.get_partition_key().into()
    }

    fn to_partition_key(&self) -> crate::db::PartitionKey {
        self.get_partition_key().into()
    }
}

impl RowKeyParameter for Arc<DbRow> {
    fn as_str(&self) -> &str {
        self.get_row_key()
    }
}

#[cfg(feature = "master-node")]
fn inject_expires(out: &mut Vec<u8>, expires_value: DateTimeAsMicroseconds) {
    out.push(b'"');
    out.extend_from_slice(crate::db_json_entity::consts::EXPIRES.as_bytes());
    out.extend_from_slice("\":\"".as_bytes());
    out.extend_from_slice(&expires_value.to_rfc3339().as_bytes()[..19]);
    out.push(b'"');
}
#[cfg(feature = "master-node")]
fn find_json_separator_before(src: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i > 0 {
        let b = src[i];

        if b <= 32 {
            i -= 1;
            continue;
        }

        if b == b',' {
            return Some(i);
        }

        break;
    }

    None
}
#[cfg(feature = "master-node")]
fn find_json_separator_after(src: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i < src.len() {
        let b = src[i];

        if b <= 32 {
            i += 1;
            continue;
        }

        if b == b',' {
            return Some(i + 1);
        }

        break;
    }

    None
}

impl JsonObject for &'_ DbRow {
    fn write_into(&self, dest: &mut Vec<u8>) {
        self.write_json(dest)
    }
}

#[cfg(feature = "master-node")]
impl crate::ExpirationIndex<Arc<DbRow>> for Arc<DbRow> {
    fn get_id_as_str(&self) -> &str {
        self.get_row_key()
    }

    fn to_owned(&self) -> Arc<DbRow> {
        self.clone()
    }

    fn get_expiration_moment(&self) -> Option<rust_extensions::date_time::DateTimeAsMicroseconds> {
        self.get_expires()
    }
}

#[cfg(feature = "debug_db_row")]
impl Drop for DbRow {
    fn drop(&mut self) {
        println!(
            "Dropped DbRow: PK:{}. RK:{}",
            self.get_partition_key(),
            self.get_row_key(),
        );
    }
}
