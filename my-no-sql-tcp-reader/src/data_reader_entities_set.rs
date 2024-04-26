use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;

use crate::subscribers::MyNoSqlDataReaderCallBacksPusher;

pub struct DataReaderEntitiesSet<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    entities: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
    table_name: &'static str,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> DataReaderEntitiesSet<TMyNoSqlEntity> {
    pub fn new(table_name: &'static str) -> Self {
        Self {
            entities: None,
            table_name,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.entities.is_some()
    }

    pub fn as_ref(&self) -> Option<&BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        self.entities.as_ref()
    }

    fn init_and_get_table(
        &mut self,
    ) -> &mut BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        if self.entities.is_none() {
            println!("MyNoSqlTcpReader table {} is initialized", self.table_name);
            self.entities = Some(BTreeMap::new());
            return self.entities.as_mut().unwrap();
        }

        return self.entities.as_mut().unwrap();
    }

    pub fn init_table<'s>(
        &'s mut self,
        data: BTreeMap<String, Vec<TMyNoSqlEntity>>,
    ) -> InitTableResult<'s, TMyNoSqlEntity> {
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

        let table_before = self.entities.replace(new_table);

        InitTableResult {
            table_now: self.entities.as_ref().unwrap(),
            table_before,
        }
    }

    pub fn init_partition<'s>(
        &'s mut self,
        partition_key: &str,
        src_entities: BTreeMap<String, Vec<TMyNoSqlEntity>>,
    ) -> InitPartitionResult<'s, TMyNoSqlEntity> {
        let entities = self.init_and_get_table();

        let mut new_partition = BTreeMap::new();

        let before_partition = entities.remove(partition_key);

        for (row_key, entities) in src_entities {
            for entity in entities {
                new_partition.insert(row_key.clone(), Arc::new(entity));
            }
        }

        entities.insert(partition_key.to_string(), new_partition);

        InitPartitionResult {
            partition_before: entities.get(partition_key).unwrap(),
            partition_now: before_partition,
        }
    }

    pub fn update_rows(
        &mut self,
        src_data: BTreeMap<String, Vec<TMyNoSqlEntity>>,
        callbacks: &Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    ) {
        let entities = self.init_and_get_table();

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

            if let Some(callbacks) = callbacks {
                if let Some(updates) = updates {
                    if updates.len() > 0 {
                        callbacks.inserted_or_replaced(partition_key.as_str(), updates);
                    }
                }
            }
        }
    }

    pub fn delete_rows(
        &mut self,
        rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>,
        callbacks: &Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    ) {
        let mut deleted_rows = if callbacks.is_some() {
            Some(BTreeMap::new())
        } else {
            None
        };

        let entities = self.init_and_get_table();

        for row_to_delete in &rows_to_delete {
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

pub struct InitTableResult<'s, TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    pub table_now: &'s BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
    pub table_before: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
}

pub struct InitPartitionResult<'s, TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    pub partition_before: &'s BTreeMap<String, Arc<TMyNoSqlEntity>>,
    pub partition_now: Option<BTreeMap<String, Arc<TMyNoSqlEntity>>>,
}
