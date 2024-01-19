use my_json::json_writer::JsonArrayWriter;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::sorted_vec::SortedVecWithStrKey;
use std::sync::Arc;

use crate::db::{DbPartition, DbRow, PartitionKey, PartitionKeyParameter, RowKeyParameter};

#[cfg(feature = "master-node")]
use super::DbTableAttributes;
use super::{AvgSize, DbPartitionsContainer};

pub struct DbTable {
    pub name: String,
    pub partitions: DbPartitionsContainer,
    pub avg_size: AvgSize,
    #[cfg(feature = "master-node")]
    pub last_write_moment: DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub attributes: DbTableAttributes,
}

impl DbTable {
    #[cfg(not(feature = "master-node"))]
    pub fn new(name: String) -> Self {
        Self {
            name: name.into(),
            partitions: DbPartitionsContainer::new(),
            avg_size: AvgSize::new(),
        }
    }

    pub fn get_partitions_amount(&self) -> usize {
        self.partitions.len()
    }

    #[cfg(feature = "master-node")]
    pub fn get_last_write_moment(&self) -> DateTimeAsMicroseconds {
        self.last_write_moment
    }

    pub fn get_all_rows<'s>(&'s self) -> Vec<&Arc<DbRow>> {
        let mut result = Vec::new();
        for db_partition in self.partitions.get_partitions() {
            result.extend(db_partition.get_all_rows());
        }
        result
    }

    pub fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition in self.partitions.get_partitions() {
            for db_row in db_partition.get_all_rows() {
                json_array_writer.write(db_row.as_ref())
            }
        }

        json_array_writer
    }

    pub fn get_rows_amount(&self) -> usize {
        let mut result = 0;
        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_rows_amount();
        }

        result
    }

    pub fn get_table_size(&self) -> usize {
        let mut result = 0;
        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_content_size();
        }
        result
    }

    pub fn get_partition_as_json_array(&self, partition_key: &str) -> Option<JsonArrayWriter> {
        let mut json_array_writer = JsonArrayWriter::new();

        if let Some(db_partition) = self.partitions.get(partition_key) {
            for db_row in db_partition.get_all_rows() {
                json_array_writer.write(db_row.as_ref())
            }
        }

        json_array_writer.into()
    }

    #[inline]
    pub fn get_partition_mut(&mut self, partition_key: &str) -> Option<&mut DbPartition> {
        self.partitions.get_mut(partition_key)
    }

    #[inline]
    pub fn get_partition(&self, partition_key: &str) -> Option<&DbPartition> {
        self.partitions.get(partition_key)
    }
    #[inline]
    pub fn get_partitions(&self) -> std::slice::Iter<DbPartition> {
        self.partitions.get_partitions()
    }
}

/// Insert Operations

impl DbTable {
    #[inline]
    pub fn insert_or_replace_row(
        &mut self,
        db_row: &Arc<DbRow>,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> (PartitionKey, Option<Arc<DbRow>>) {
        self.avg_size.add(db_row);

        let db_partition = self.partitions.add_partition_if_not_exists(db_row);

        let removed_db_row = db_partition.insert_or_replace_row(db_row.clone());

        #[cfg(feature = "master-node")]
        if let Some(set_last_write_moment) = set_last_write_moment {
            self.last_write_moment = set_last_write_moment;
            db_partition.last_write_moment = set_last_write_moment;
        }

        (db_partition.partition_key.clone(), removed_db_row)
    }

    #[inline]
    pub fn insert_row(
        &mut self,
        db_row: &Arc<DbRow>,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<PartitionKey> {
        self.avg_size.add(db_row);

        let db_partition = self.partitions.add_partition_if_not_exists(db_row);

        let result = db_partition.insert_row(db_row.clone());
        #[cfg(feature = "master-node")]
        if result {
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }
        }
        if result {
            Some(db_partition.partition_key.clone())
        } else {
            None
        }
    }

    #[inline]
    pub fn bulk_insert_or_replace(
        &mut self,
        partition_key: &impl PartitionKeyParameter,
        db_rows: &[Arc<DbRow>],
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> (PartitionKey, Vec<Arc<DbRow>>) {
        for db_row in db_rows {
            self.avg_size.add(db_row);
        }

        let db_partition = self.partitions.add_partition_if_not_exists(partition_key);

        let result = db_partition.insert_or_replace_rows_bulk(db_rows);
        #[cfg(feature = "master-node")]
        if let Some(set_last_write_moment) = set_last_write_moment {
            self.last_write_moment = set_last_write_moment;
            db_partition.last_write_moment = set_last_write_moment;
        }

        (db_partition.partition_key.clone(), result)
    }

    #[inline]
    pub fn init_partition(&mut self, db_partition: DbPartition) {
        self.partitions.insert(db_partition);
    }
}

/// Delete Operations
///
///

impl DbTable {
    pub fn remove_row(
        &mut self,
        partition_key: &impl PartitionKeyParameter,
        row_key: &impl RowKeyParameter,
        delete_empty_partition: bool,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<(PartitionKey, Arc<DbRow>, bool)> {
        let (partition_key, removed_row, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key.as_str())?;

            let removed_row = db_partition.remove_row(row_key.as_str())?;
            #[cfg(feature = "master-node")]
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }

            (
                db_partition.partition_key.clone(),
                removed_row,
                db_partition.is_empty(),
            )
        };

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key.as_str());
        }

        return Some((partition_key, removed_row, partition_is_empty));
    }

    pub fn bulk_remove_rows(
        &mut self,
        partition_key: &impl PartitionKeyParameter,
        row_keys: impl Iterator<Item = impl RowKeyParameter>,
        delete_empty_partition: bool,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<(PartitionKey, Vec<Arc<DbRow>>, bool)> {
        let (partition_key, removed_rows, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key.as_str())?;

            let removed_rows = db_partition.remove_rows_bulk(row_keys)?;

            #[cfg(feature = "master-node")]
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }

            (
                db_partition.partition_key.clone(),
                removed_rows,
                db_partition.is_empty(),
            )
        };

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key.as_str());
        }

        return Some((partition_key, removed_rows, partition_is_empty));
    }

    #[inline]
    pub fn remove_partition(
        &mut self,
        partition_key: &impl PartitionKeyParameter,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key.as_str());

        #[cfg(feature = "master-node")]
        if removed_partition.is_some() {
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = set_last_write_moment;
            }
        }

        removed_partition
    }

    pub fn clear_table(&mut self) -> Option<SortedVecWithStrKey<DbPartition>> {
        self.partitions.clear()
    }
}
