use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use rust_extensions::{lazy::LazyVec, ApplicationStates};

use super::{MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderCallBacksPusher};

pub struct MyNoSqlDataReaderData<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    table_name: &'static str,
    entities: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
    callbacks: Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
{
    pub async fn new(
        table_name: &'static str,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        Self {
            table_name,
            entities: None,
            callbacks: None,
            app_states,
        }
    }

    pub async fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &mut self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let pusher =
            MyNoSqlDataReaderCallBacksPusher::new(callbacks, self.app_states.clone()).await;

        self.callbacks = Some(Arc::new(pusher));
    }
    fn get_init_table(&mut self) -> &mut BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        if self.entities.is_none() {
            println!("Initialized data for table {}", self.table_name);
            self.entities = Some(BTreeMap::new());
            return self.entities.as_mut().unwrap();
        }

        return self.entities.as_mut().unwrap();
    }

    pub async fn init_table(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
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

        let before = self.entities.replace(new_table);

        if let Some(callbacks) = self.callbacks.as_ref() {
            super::callback_triggers::trigger_table_difference(
                callbacks.as_ref(),
                before,
                self.entities.as_ref().unwrap(),
            )
            .await;
        }
    }

    pub async fn init_partition(
        &mut self,
        partition_key: &str,
        src_entities: HashMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        let callbacks = self.callbacks.clone();

        let entities = self.get_init_table();

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

    pub fn update_rows(&mut self, src_data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        let callbacks = self.callbacks.clone();

        let entities = self.get_init_table();

        for (partition_key, src_entities) in src_data {
            let mut updates = if callbacks.is_some() {
                Some(LazyVec::new())
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
                    updates.add(entity.clone());
                }
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }

            if let Some(callbacks) = callbacks.as_ref() {
                if let Some(updates) = updates {
                    if let Some(updates) = updates.get_result() {
                        callbacks.inserted_or_replaced(partition_key.as_str(), updates);
                    }
                }
            }
        }
    }

    pub fn delete_rows(&mut self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        let callbacks = self.callbacks.clone();

        let mut deleted_rows = if callbacks.is_some() {
            Some(HashMap::new())
        } else {
            None
        };

        let entities = self.get_init_table();

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

    pub fn get_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let entities = self.entities.as_ref()?;

        return Some(entities.clone());
    }

    pub fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        if entities.len() == 0 {
            return None;
        }

        let mut result = Vec::new();

        for partition in entities.values() {
            for entity in partition.values() {
                result.push(entity.clone());
            }
        }

        Some(result)
    }

    pub fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        let row = partition.get(row_key)?;

        Some(row.clone())
    }

    pub fn get_by_partition(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        Some(partition.clone())
    }

    pub fn get_by_partition_with_filter(
        &self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        let mut result = BTreeMap::new();

        for db_row in partition.values() {
            if filter(db_row) {
                result.insert(db_row.get_row_key().to_string(), db_row.clone());
            }
        }

        Some(result)
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        let entities = self.entities.as_ref();

        if entities.is_none() {
            return false;
        }

        let entities = entities.unwrap();

        entities.contains_key(partition_key)
    }

    pub fn get_by_partition_as_vec(&self, partition_key: &str) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        if partition.len() == 0 {
            return None;
        }

        let mut result = Vec::with_capacity(partition.len());

        for db_row in partition.values() {
            result.push(db_row.clone());
        }

        Some(result)
    }

    pub fn get_by_partition_as_vec_with_filter(
        &self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        if partition.len() == 0 {
            return None;
        }

        let mut result = Vec::with_capacity(partition.len());

        for db_row in partition.values() {
            if filter(db_row.as_ref()) {
                result.push(db_row.clone());
            }
        }

        Some(result)
    }

    pub async fn has_entities_at_all(&self) -> bool {
        self.entities.is_some()
    }
}
