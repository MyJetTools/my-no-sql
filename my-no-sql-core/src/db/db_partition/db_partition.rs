use my_json::json_writer::JsonObject;

#[cfg(feature = "master-node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;
use rust_extensions::sorted_vec::EntityWithStrKey;

use crate::db::{DbRow, DbRowKey};

use std::sync::Arc;

use super::{DbRowsContainer, PartitionKey, PartitionKeyParameter};

pub struct DbPartition {
    pub partition_key: PartitionKey,
    #[cfg(feature = "master-node")]
    pub expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>,
    pub rows: DbRowsContainer,
    #[cfg(feature = "master-node")]
    pub last_read_moment: AtomicDateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    content_size: usize,
}

impl EntityWithStrKey for DbPartition {
    fn get_key(&self) -> &str {
        self.partition_key.as_str()
    }
}

impl DbPartition {
    pub fn new(partition_key: impl PartitionKeyParameter) -> Self {
        Self {
            partition_key: partition_key.into_partition_key(),
            rows: DbRowsContainer::new(),
            #[cfg(feature = "master-node")]
            last_read_moment: AtomicDateTimeAsMicroseconds::now(),
            #[cfg(feature = "master-node")]
            last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds::now(),
            content_size: 0,
            #[cfg(feature = "master-node")]
            expires: None,
        }
    }

    #[cfg(feature = "master-node")]
    pub fn get_rows_to_expire(
        &self,
        now: rust_extensions::date_time::DateTimeAsMicroseconds,
    ) -> Vec<Arc<DbRow>> {
        self.rows.get_rows_to_expire(now)
    }
    #[cfg(feature = "master-node")]
    pub fn get_expiration_index_owned(
        &self,
    ) -> crate::db::db_table::DbPartitionExpirationIndexOwned {
        crate::db::db_table::DbPartitionExpirationIndexOwned {
            expires: self.expires,
            partition_key: self.partition_key.clone(),
        }
    }

    pub fn get_content_size(&self) -> usize {
        self.content_size
    }

    pub fn rows_count(&self) -> usize {
        return self.rows.len();
    }

    #[inline]
    pub fn insert_row(&mut self, db_row: Arc<DbRow>) -> bool {
        if self.rows.has_db_row(db_row.get_row_key()) {
            return false;
        }

        self.insert_or_replace_row(db_row);
        return true;
    }

    #[inline]
    pub fn insert_or_replace_row(&mut self, db_row: Arc<DbRow>) -> Option<Arc<DbRow>> {
        self.content_size += db_row.get_src_as_slice().len();

        let result = self.rows.insert(db_row);

        if let Some(removed_item) = result.as_ref() {
            self.content_size -= removed_item.get_src_as_slice().len();
        }

        result
    }

    #[inline]
    pub fn insert_or_replace_rows_bulk(&mut self, db_rows: &[Arc<DbRow>]) -> Vec<Arc<DbRow>> {
        let mut result = Vec::new();

        for db_row in db_rows {
            self.content_size += db_row.get_src_as_slice().len();

            if let Some(removed_item) = self.rows.insert(db_row.clone()) {
                self.content_size -= removed_item.get_src_as_slice().len();
                result.push(removed_item);
            }
        }

        result
    }

    pub fn remove_row(&mut self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.rows.remove(row_key);

        if let Some(removed_item) = result.as_ref() {
            self.content_size -= removed_item.get_src_as_slice().len();
        }
        result
    }

    pub fn remove_rows_bulk<'s, TRowsIterator: Iterator<Item = DbRowKey<'s>>>(
        &mut self,
        row_keys: TRowsIterator,
    ) -> Option<Vec<Arc<DbRow>>> {
        let mut result = rust_extensions::lazy::LazyVec::new();

        for row_key in row_keys {
            if let Some(removed_item) = self.rows.remove(row_key.as_str()) {
                self.content_size -= removed_item.get_src_as_slice().len();
                result.add(removed_item);
            }
        }

        result.get_result()
    }

    pub fn get_all_rows<'s>(&'s self) -> std::slice::Iter<Arc<DbRow>> {
        self.rows.get_all()
    }

    pub fn get_all_rows_cloned<'s>(&'s self) -> Vec<Arc<DbRow>> {
        self.rows.get_all().map(|itm| itm.clone()).collect()
    }

    pub fn get_rows_amount(&self) -> usize {
        self.rows.len()
    }

    #[cfg(feature = "master-node")]
    pub fn get_expiration_index_rows_amount(&self) -> usize {
        self.rows.rows_with_expiration_index_len()
    }

    pub fn get_row(&self, row_key: &str) -> Option<&Arc<DbRow>> {
        let result = self.rows.get(row_key);
        result
    }

    pub fn get_row_and_clone(&self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.rows.get(row_key)?;
        Some(result.clone())
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Vec<&Arc<DbRow>> {
        return self.rows.get_highest_row_and_below(row_key, limit);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.len() == 0
    }
}

#[cfg(feature = "master-node")]
impl crate::ExpirationIndex<crate::db::db_table::DbPartitionExpirationIndexOwned> for DbPartition {
    fn get_id_as_str(&self) -> &str {
        self.partition_key.as_str()
    }

    fn to_owned(&self) -> crate::db::db_table::DbPartitionExpirationIndexOwned {
        crate::db::db_table::DbPartitionExpirationIndexOwned {
            partition_key: self.partition_key.clone(),
            expires: self.expires,
        }
    }
    fn get_expiration_moment(&self) -> Option<rust_extensions::date_time::DateTimeAsMicroseconds> {
        self.expires
    }
}

#[cfg(feature = "master-node")]
impl DbPartition {
    pub fn update_last_read_moment(&self, now: rust_extensions::date_time::DateTimeAsMicroseconds) {
        self.last_read_moment.update(now);
    }

    pub fn get_last_write_moment(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        self.last_write_moment
    }

    pub fn get_last_read_moment(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        self.last_read_moment.as_date_time()
    }
}

impl JsonObject for &'_ DbPartition {
    fn write_into(&self, dest: &mut Vec<u8>) {
        let mut first_element = true;
        dest.push(b'[');
        for db_row in self.rows.get_all() {
            if first_element {
                first_element = false;
            } else {
                dest.push(b',');
            }
            db_row.as_ref().write_into(dest)
        }
        dest.push(b']');
    }
}
