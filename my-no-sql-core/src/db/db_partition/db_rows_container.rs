#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::lazy::LazyVec;
use std::{
    collections::{btree_map::Values, BTreeMap},
    sync::Arc,
};

use crate::db::DbRow;

pub struct DbRowsContainer {
    data: BTreeMap<String, Arc<DbRow>>,

    #[cfg(feature = "master-node")]
    rows_with_expiration_index: crate::ExpirationIndex<Arc<DbRow>>,
}

impl DbRowsContainer {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            #[cfg(feature = "master-node")]
            rows_with_expiration_index: crate::ExpirationIndex::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[cfg(feature = "master-node")]
    pub fn rows_with_expiration_index_len(&self) -> usize {
        self.rows_with_expiration_index.len()
    }
    #[cfg(feature = "master-node")]
    pub fn get_rows_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<Arc<DbRow>>> {
        self.rows_with_expiration_index.get_items_to_expire(now)
    }

    #[cfg(feature = "master-node")]
    pub fn get_rows_to_gc_by_max_amount(&self, max_rows_amount: usize) -> Option<Vec<Arc<DbRow>>> {
        if self.data.len() <= max_rows_amount {
            return None;
        }

        let mut by_last_read_access = BTreeMap::new();

        for db_row in self.data.values() {
            let last_read_access = db_row.last_read_access.get_unix_microseconds();
            by_last_read_access.insert(last_read_access, db_row.clone());
        }

        let records_amount_to_gc = self.data.len() - max_rows_amount;

        let mut result = Vec::with_capacity(records_amount_to_gc);

        while result.len() < records_amount_to_gc {
            let first = by_last_read_access.pop_first();
            result.push(first.unwrap().1);
        }

        Some(result)
    }

    pub fn insert(&mut self, db_row: Arc<DbRow>) -> Option<Arc<DbRow>> {
        #[cfg(feature = "master-node")]
        self.rows_with_expiration_index
            .add(db_row.get_expires(), &db_row);

        let result = self.data.insert(db_row.get_row_key().to_string(), db_row);

        #[cfg(feature = "master-node")]
        if let Some(removed_db_row) = &result {
            if let Some(old_expires) = removed_db_row.get_expires() {
                self.rows_with_expiration_index
                    .remove(old_expires, &removed_db_row);
            }
        }

        result
    }

    pub fn remove(&mut self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.data.remove(row_key);

        #[cfg(feature = "master-node")]
        if let Some(removed_db_row) = &result {
            if let Some(expires) = removed_db_row.get_expires() {
                self.rows_with_expiration_index
                    .remove(expires, &removed_db_row);
            }
        }

        result
    }

    pub fn get(&self, row_key: &str) -> Option<&Arc<DbRow>> {
        self.data.get(row_key)
    }

    pub fn has_db_row(&self, row_key: &str) -> bool {
        return self.data.contains_key(row_key);
    }

    pub fn get_all<'s>(&'s self) -> Values<'s, String, Arc<DbRow>> {
        self.data.values()
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Option<Vec<&Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for (db_row_key, db_row) in self.data.range(..row_key.to_string()) {
            if db_row_key <= row_key {
                result.add(db_row);

                if let Some(limit) = limit {
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        result.get_result()
    }

    #[cfg(feature = "master-node")]
    pub fn update_expiration_time(
        &mut self,
        row_key: &str,
        expiration_time: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        if let Some(db_row) = self.get(row_key).cloned() {
            let old_expires = db_row.update_expires(expiration_time);

            self.rows_with_expiration_index
                .add(expiration_time, &db_row);

            if are_expires_the_same(old_expires, expiration_time) {
                return None;
            }

            if let Some(old_expires) = old_expires {
                self.rows_with_expiration_index.remove(old_expires, &db_row);
            }

            return Some(db_row);
        }

        None
    }
}

#[cfg(feature = "master-node")]
fn are_expires_the_same(
    old_expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>,
    new_expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>,
) -> bool {
    if let Some(old_expires) = old_expires {
        if let Some(new_expires) = new_expires {
            return new_expires.unix_microseconds == old_expires.unix_microseconds;
        }

        return false;
    }

    if new_expires.is_some() {
        return false;
    }

    true
}

#[cfg(feature = "master-node")]
#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    use super::*;

    #[test]
    fn test_that_index_appears() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.into_db_row(test_json, &time_stamp)));

        assert_eq!(1, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_does_not_appear_since_we_do_not_have_expiration() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.into_db_row(test_json, &time_stamp)));

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_is_removed() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00"
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.into_db_row(test_json, &time_stamp)));

        db_rows.remove("test");

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_update_expiration_time_from_no_to() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test"
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.into_db_row(test_json, &time_stamp)));

        assert_eq!(0, db_rows.rows_with_expiration_index.len());

        let new_expiration_time = DateTimeAsMicroseconds::new(2);

        db_rows.update_expiration_time("test", Some(new_expiration_time));

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .has_data_with_expiration_moment(2)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_to_new_expiration_time() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let mut db_rows = DbRowsContainer::new();

        let time_stamp = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.into_db_row(test_json, &time_stamp));
        db_rows.insert(db_row.clone());

        let current_expiration = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .has_data_with_expiration_moment(current_expiration.unix_microseconds)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time("test", Some(DateTimeAsMicroseconds::new(2)));

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .has_data_with_expiration_moment(2)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_from_some_to_no() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let now = JsonTimeStamp::now();
        let db_row = Arc::new(db_json_entity.into_db_row(test_json, &now));
        db_rows.insert(db_row.clone());

        let current_expiration = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .has_data_with_expiration_moment(current_expiration.unix_microseconds)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time("test", None);
        assert_eq!(0, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_we_do_not_have_db_rows_to_expire() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let now = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.into_db_row(test_json, &now));
        db_rows.insert(db_row.clone());

        let mut now = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();
        now.unix_microseconds -= 1;

        let rows_to_expire = db_rows.rows_with_expiration_index.get_items_to_expire(now);

        assert_eq!(true, rows_to_expire.is_none());
    }

    #[test]
    fn test_we_do_have_db_rows_to_expire() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&test_json).unwrap();

        let now = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.into_db_row(test_json, &now));
        db_rows.insert(db_row.clone());

        let now = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        let rows_to_expire = db_rows.rows_with_expiration_index.get_items_to_expire(now);

        assert_eq!(true, rows_to_expire.is_some());
    }

    #[test]
    fn check_gc_max_rows_amount() {
        let mut db_rows = DbRowsContainer::new();

        let mut now = DateTimeAsMicroseconds::now();

        let json = r#"{
            "PartitionKey": "test",
            "RowKey": "test1",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&json).unwrap();

        let db_row =
            Arc::new(db_json_entity.into_db_row(json, &JsonTimeStamp::from_date_time(now)));

        db_rows.insert(db_row.clone());

        // Next Item

        now.add_seconds(1);

        let raw_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test2",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&raw_json).unwrap();

        let db_row =
            Arc::new(db_json_entity.into_db_row(raw_json, &JsonTimeStamp::from_date_time(now)));

        db_rows.insert(db_row.clone());

        // Next Item

        now.add_seconds(1);

        let json_db_row = r#"{
            "PartitionKey": "test",
            "RowKey": "test3",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&json_db_row).unwrap();

        let db_row =
            Arc::new(db_json_entity.into_db_row(json_db_row, &JsonTimeStamp::from_date_time(now)));

        db_rows.insert(db_row.clone());

        // Next Item

        now.add_seconds(1);

        let raw_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test4",
        }"#
        .as_bytes()
        .to_vec();

        let db_json_entity = DbJsonEntity::parse(&raw_json).unwrap();

        let db_row =
            Arc::new(db_json_entity.into_db_row(raw_json, &JsonTimeStamp::from_date_time(now)));

        db_rows.insert(db_row.clone());

        let db_rows_to_gc = db_rows.get_rows_to_gc_by_max_amount(3).unwrap();

        assert_eq!("test1", db_rows_to_gc.get(0).unwrap().get_row_key());
    }
}
