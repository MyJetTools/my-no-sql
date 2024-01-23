use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;
use rust_extensions::ApplicationStates;
use tokio::sync::RwLock;

use crate::{subscribers::DeleteRowEvent, MyNoSqlDataReaderData};

pub struct MyNoSqlDataReaderInner<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    pub data: RwLock<MyNoSqlDataReaderData<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> MyNoSqlDataReaderInner<TMyNoSqlEntity> {
    pub fn new(app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>) -> Self {
        Self {
            data: RwLock::new(MyNoSqlDataReaderData::new(app_states)),
        }
    }
    pub async fn init_table(&self, data: BTreeMap<String, Vec<TMyNoSqlEntity>>) {
        let mut new_table: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> =
            BTreeMap::new();

        for (partition_key, src_entities_by_partition) in data {
            new_table.insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = new_table.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities_by_partition {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }

        let mut write_access = self.data.write().await;

        let before = write_access.entities.replace(new_table);

        if let Some(callbacks) = write_access.callbacks.as_ref() {
            super::callback_triggers::trigger_table_difference(
                callbacks.as_ref(),
                before,
                write_access.entities.as_ref().unwrap(),
            )
            .await;
        }
    }

    pub async fn init_partition(
        &self,
        partition_key: &str,
        src_entities: BTreeMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        let mut write_access = self.data.write().await;
        let callbacks = write_access.callbacks.clone();

        let entities = write_access.get_init_table();

        let mut new_partition = BTreeMap::new();

        let before_partition = entities.remove(partition_key);

        for (row_key, entities) in src_entities {
            for entity in entities {
                new_partition.insert(row_key.clone(), Arc::new(entity));
            }
        }

        entities.insert(partition_key.to_string(), new_partition);

        if let Some(callbacks) = callbacks {
            super::callback_triggers::trigger_partition_difference(
                callbacks.as_ref(),
                partition_key,
                before_partition,
                entities.get(partition_key).unwrap(),
            )
            .await;
        }
    }

    pub async fn update_rows(&self, src_data: BTreeMap<String, Vec<TMyNoSqlEntity>>) {
        let mut write_access = self.data.write().await;
        let callbacks = write_access.callbacks.clone();

        let entities = write_access.get_init_table();

        for (partition_key, src_entities) in src_data {
            let mut updates = if callbacks.is_some() {
                Some(Vec::new())
            } else {
                None
            };

            if !entities.contains_key(partition_key.as_str()) {
                entities.insert(partition_key.to_string(), BTreeMap::new());
            }

            let by_partition = entities.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities {
                let entity = Arc::new(entity);
                if let Some(updates) = updates.as_mut() {
                    updates.push(entity.clone());
                }
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }

            if let Some(callbacks) = callbacks.as_ref() {
                if let Some(updates) = updates {
                    if updates.len() > 0 {
                        callbacks.inserted_or_replaced(partition_key.as_str(), updates);
                    }
                }
            }
        }
    }

    pub async fn delete_rows(&self, rows_to_delete: Vec<DeleteRowEvent>) {
        let mut write_access = self.data.write().await;
        let callbacks = write_access.callbacks.clone();

        let mut deleted_rows = if callbacks.is_some() {
            Some(BTreeMap::new())
        } else {
            None
        };

        let entities = write_access.get_init_table();

        for row_to_delete in rows_to_delete {
            let mut delete_partition = false;
            if let Some(partition) = entities.get_mut(row_to_delete.partition_key.as_str()) {
                if partition.remove(row_to_delete.row_key.as_str()).is_some() {
                    if let Some(deleted_rows) = deleted_rows.as_mut() {
                        if !deleted_rows.contains_key(row_to_delete.partition_key.as_str()) {
                            deleted_rows
                                .insert(row_to_delete.partition_key.to_string(), Vec::new());
                        }

                        deleted_rows
                            .get_mut(row_to_delete.partition_key.as_str())
                            .unwrap()
                            .push(
                                partition
                                    .get(row_to_delete.row_key.as_str())
                                    .unwrap()
                                    .clone(),
                            );
                    }
                }

                delete_partition = partition.len() == 0;
            }

            if delete_partition {
                entities.remove(row_to_delete.partition_key.as_str());
            }
        }

        if let Some(callbacks) = callbacks.as_ref() {
            if let Some(partitions) = deleted_rows {
                for (partition_key, rows) in partitions {
                    callbacks.deleted(partition_key.as_str(), rows);
                }
            }
        }
    }
}
