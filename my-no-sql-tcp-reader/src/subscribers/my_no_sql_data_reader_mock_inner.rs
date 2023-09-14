use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use rust_extensions::lazy::LazyVec;
use tokio::sync::RwLock;

pub struct MyNoSqlDataReaderMockInner<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    pub data: RwLock<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderMockInner<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn update(&self, items: impl Iterator<Item = Arc<TMyNoSqlEntity>>) {
        let mut write_access = self.data.write().await;
        for item in items {
            let partition_key = item.get_partition_key();
            let row_key = item.get_row_key();

            let partition = write_access
                .entry(partition_key.to_string())
                .or_insert_with(BTreeMap::new);
            partition.insert(row_key.to_string(), item);
        }
    }
    pub async fn delete(&self, to_delete: impl Iterator<Item = (String, String)>) {
        let mut write_access = self.data.write().await;

        let mut partitions_to_remove = HashSet::new();
        for (partition_key, row_key) in to_delete {
            if let Some(partition) = write_access.get_mut(&partition_key) {
                partition.remove(&row_key);
            }

            if let Some(partition) = write_access.get(partition_key.as_str()) {
                if partition.is_empty() {
                    partitions_to_remove.insert(partition_key);
                }
            }
        }

        for partition_to_remove in partitions_to_remove {
            write_access.remove(partition_to_remove.as_str());
        }
    }

    pub async fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        let mut result = LazyVec::new();
        for partition in read_access.values() {
            for item in partition.values() {
                result.add(item.clone());
            }
        }

        result.get_result()
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        read_access.get(partition_key).cloned()
    }

    pub async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        let mut result = LazyVec::new();
        if let Some(partition) = read_access.get(partition_key) {
            for item in partition.values() {
                result.add(item.clone());
            }
        }

        result.get_result()
    }

    pub async fn get_by_partition_key_as_vec_with_filter(
        &self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        let mut result = LazyVec::new();
        if let Some(partition) = read_access.get(partition_key) {
            for item in partition.values() {
                if filter(item) {
                    result.add(item.clone());
                }
            }
        }

        result.get_result()
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let read_access = self.data.read().await;
        read_access
            .get(partition_key)
            .and_then(|partition| partition.get(row_key))
            .cloned()
    }

    pub async fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        let mut result = LazyVec::new();
        for partition in read_access.values() {
            for item in partition.values() {
                result.add(item.clone());
            }
        }

        result.get_result()
    }

    pub async fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.data.read().await;
        let mut result = LazyVec::new();
        for partition in read_access.values() {
            for item in partition.values() {
                if filter(item) {
                    result.add(item.clone());
                }
            }
        }

        result.get_result()
    }

    pub async fn has_partition(&self, partition_key: &str) -> bool {
        let read_access = self.data.read().await;
        read_access.contains_key(partition_key)
    }
}
