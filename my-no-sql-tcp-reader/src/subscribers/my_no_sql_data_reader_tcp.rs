use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use my_json::json_reader::array_parser::JsonArrayIterator;
use my_no_sql_core::db_json_entity::DbJsonEntity;
use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;
use rust_extensions::{ApplicationStates, StrOrString};
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use super::{
    GetEntitiesBuilder, GetEntityBuilder, MyNoSqlDataReader, MyNoSqlDataReaderCallBacks,
    MyNoSqlDataReaderData, UpdateEvent,
};

pub struct MyNoSqlDataReaderInner<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    data: RwLock<MyNoSqlDataReaderData<TMyNoSqlEntity>>,
    sync_handler: Arc<SyncToMainNodeHandler>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> MyNoSqlDataReaderInner<TMyNoSqlEntity> {
    pub fn get_data(&self) -> &RwLock<MyNoSqlDataReaderData<TMyNoSqlEntity>> {
        &self.data
    }

    pub fn get_sync_handler(&self) -> &Arc<SyncToMainNodeHandler> {
        &self.sync_handler
    }
}

pub struct MyNoSqlDataReaderTcp<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderTcp<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
{
    pub async fn new(
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        sync_handler: Arc<SyncToMainNodeHandler>,
    ) -> Self {
        Self {
            inner: Arc::new(MyNoSqlDataReaderInner {
                data: RwLock::new(
                    MyNoSqlDataReaderData::new(TMyNoSqlEntity::TABLE_NAME, app_states).await,
                ),
                sync_handler,
            }),
        }
    }

    pub async fn get_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let reader = self.inner.data.read().await;
        return reader.get_table_snapshot();
    }

    pub async fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let reader = self.inner.data.read().await;
        reader.get_table_snapshot_as_vec()
    }

    pub async fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let mut write_access = self.inner.data.write().await;
        write_access.assign_callback(callbacks).await;
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let reader = self.inner.data.read().await;
        reader.get_by_partition(partition_key)
    }

    pub async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let reader = self.inner.data.read().await;
        reader.get_by_partition_as_vec(partition_key)
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let reader = self.inner.data.read().await;
        reader.get_entity(partition_key, row_key)
    }

    pub fn get_entities<'s>(
        &self,
        partition_key: impl Into<StrOrString<'s>>,
    ) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        GetEntitiesBuilder::new(partition_key.into().to_string(), self.inner.clone())
    }

    pub fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<TMyNoSqlEntity> {
        GetEntityBuilder::new(partition_key, row_key, self.inner.clone())
    }

    pub async fn has_partition(&self, partition_key: &str) -> bool {
        let reader: tokio::sync::RwLockReadGuard<'_, MyNoSqlDataReaderData<TMyNoSqlEntity>> =
            self.inner.data.read().await;
        reader.has_partition(partition_key)
    }

    pub fn deserialize_entity<'s>(&self, data: &[u8]) -> TMyNoSqlEntity {
        let parse_result: Result<TMyNoSqlEntity, _> = serde_json::from_slice(&data);

        match parse_result {
            Ok(el) => return el,
            Err(err) => {
                let db_entity = DbJsonEntity::parse(data);

                match db_entity {
                    Ok(db_entity) => {
                        panic!(
                            "Table: {}. Can not parse entity with PartitionKey: [{}] and RowKey: [{}]. Err: {:?}",
                             TMyNoSqlEntity::TABLE_NAME, db_entity.partition_key, db_entity.row_key, err
                        );
                    }
                    Err(err) => {
                        panic!(
                            "Table: {}. Can not extract partitionKey and rowKey. Looks like entity broken at all. Err: {:?}",
                            TMyNoSqlEntity::TABLE_NAME, err
                        )
                    }
                }
            }
        }
    }

    pub fn deserialize_array(&self, data: &[u8]) -> HashMap<String, Vec<TMyNoSqlEntity>> {
        let mut result = HashMap::new();

        for db_entity in JsonArrayIterator::new(data) {
            if let Err(err) = &db_entity {
                panic!(
                    "Table: {}. The whole array of json entities is broken. Err: {:?}",
                    TMyNoSqlEntity::TABLE_NAME,
                    err
                );
            }

            let db_entity_data = db_entity.unwrap();

            let el = self.deserialize_entity(db_entity_data);

            let partition_key = el.get_partition_key();
            if !result.contains_key(partition_key) {
                result.insert(partition_key.to_string(), Vec::new());
            }

            result.get_mut(partition_key).unwrap().push(el);
        }

        result
    }

    pub async fn wait_until_first_data_arrives(&self) {
        loop {
            {
                let reader = self.inner.data.read().await;
                if reader.has_entities_at_all().await {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned> UpdateEvent
    for MyNoSqlDataReaderTcp<TMyNoSqlEntity>
{
    async fn init_table(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.write().await;
        write_access.init_table(data).await;
    }

    async fn init_partition(&self, partition_key: &str, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.write().await;
        write_access.init_partition(partition_key, data).await;
    }

    async fn update_rows(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.write().await;
        write_access.update_rows(data);
    }

    async fn delete_rows(&self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        let mut write_access = self.inner.data.write().await;
        write_access.delete_rows(rows_to_delete);
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity> MyNoSqlDataReader<TMyNoSqlEntity> for MyNoSqlDataReaderTcp<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
{
    async fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.get_table_snapshot_as_vec().await
    }

    async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        self.get_by_partition_key(partition_key).await
    }

    async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.get_by_partition_key_as_vec(partition_key).await
    }

    async fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        self.get_entity(partition_key, row_key).await
    }

    fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        self.get_entities(partition_key)
    }

    fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<TMyNoSqlEntity> {
        self.get_entity_with_callback_to_server(partition_key, row_key)
    }

    async fn has_partition(&self, partition_key: &str) -> bool {
        self.has_partition(partition_key).await
    }
}
