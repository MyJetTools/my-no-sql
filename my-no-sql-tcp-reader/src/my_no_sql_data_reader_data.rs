use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;
use rust_extensions::ApplicationStates;

use super::{MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderCallBacksPusher};

pub struct MyNoSqlDataReaderData<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    pub entities: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
    pub callbacks: Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
{
    pub fn new(app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>) -> Self {
        Self {
            entities: None,
            callbacks: None,
            app_states,
        }
    }

    pub fn get_init_table(
        &mut self,
    ) -> &mut BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        if self.entities.is_none() {
            self.entities = Some(BTreeMap::new());
            return self.entities.as_mut().unwrap();
        }

        return self.entities.as_mut().unwrap();
    }

    pub fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &mut self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let pusher = MyNoSqlDataReaderCallBacksPusher::new(callbacks, self.app_states.clone());

        self.callbacks = Some(Arc::new(pusher));
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
    ) -> Option<&BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;
        entities.get(partition_key)
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

    pub fn has_data(&self) -> bool {
        self.entities.is_some()
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
