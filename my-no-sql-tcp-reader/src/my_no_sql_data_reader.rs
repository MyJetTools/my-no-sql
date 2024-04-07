use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_core::my_json::json_reader::array_iterator::JsonArrayIterator;
use rust_extensions::{array_of_bytes_iterator::SliceIterator, ApplicationStates};

use crate::{
    my_no_sql_connector::MyNoSqlConnector,
    subscribers::{DataReaderUpdater, DeleteRowEvent},
    GetEntitiesBuilder, GetEntityBuilder, MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderInner,
};

pub struct MyNoSqlDataReader<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> MyNoSqlDataReader<TMyNoSqlEntity> {
    pub fn new(
        connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        Self {
            connector,
            inner: Arc::new(MyNoSqlDataReaderInner::new(app_states)),
        }
    }
    pub async fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.data.read().await;
        read_access.get_table_snapshot_as_vec()
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.data.read().await;
        read_access.get_by_partition(partition_key).cloned()
    }

    pub async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.data.read().await;
        let result = read_access.get_by_partition(partition_key)?;

        Some(result.values().cloned().collect())
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let read_access = self.inner.data.read().await;
        read_access.get_entity(partition_key, row_key)
    }

    pub async fn get_enum_case_model<
        's,
        T: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + From<Arc<TMyNoSqlEntity>>
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Option<T> {
        let result = self.get_entity(T::PARTITION_KEY, T::ROW_KEY).await?;
        let result = result.into();
        Some(result)
    }

    pub async fn get_enum_case_models_by_partition_key<
        T: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + From<Arc<TMyNoSqlEntity>>
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Option<BTreeMap<String, T>> {
        let items = self.get_by_partition_key(T::PARTITION_KEY).await?;
        let mut result = BTreeMap::new();

        for (pk, entity) in items {
            let item: T = entity.into();
            result.insert(pk, item);
        }

        Some(result)
    }

    pub async fn get_enum_case_models_by_partition_key_as_vec<
        T: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + From<Arc<TMyNoSqlEntity>>
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Option<Vec<T>> {
        let items = self.get_by_partition_key_as_vec(T::PARTITION_KEY).await?;

        let mut result = Vec::with_capacity(items.len());

        for entity in items {
            let item: T = entity.into();
            result.push(item);
        }

        Some(result)
    }

    pub fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        GetEntitiesBuilder::new(
            partition_key.to_string(),
            self.inner.clone(),
            self.connector.clone(),
        )
    }

    pub fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<TMyNoSqlEntity> {
        GetEntityBuilder::new(
            partition_key,
            row_key,
            self.inner.clone(),
            self.connector.clone(),
        )
    }

    pub async fn has_partition(&self, partition_key: &str) -> bool {
        let read_access = self.inner.data.read().await;
        read_access.has_partition(partition_key)
    }

    pub async fn has_data(&self) -> bool {
        let read_access = self.inner.data.read().await;
        read_access.has_data()
    }

    pub async fn wait_until_first_data_arrives(&self) {
        loop {
            if self.has_data().await {
                return;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    pub async fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let mut write_access = self.inner.data.write().await;
        write_access.assign_callback(callbacks)
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    DataReaderUpdater for MyNoSqlDataReader<TMyNoSqlEntity>
{
    async fn init_table(&self, data: Vec<u8>) {
        let data = deserialize_array(data.as_slice());
        self.inner.init_table(data).await;
    }
    async fn init_partition(&self, partition_key: &str, data: Vec<u8>) {
        let data = deserialize_array(data.as_slice());

        self.inner.init_partition(partition_key, data).await;
    }
    async fn update_rows(&self, data: Vec<u8>) {
        let data = deserialize_array(data.as_slice());

        self.inner.update_rows(data).await;
    }
    async fn delete_rows(&self, rows_to_delete: Vec<DeleteRowEvent>) {
        self.inner.delete_rows(rows_to_delete).await;
    }
}

fn deserialize_array<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
>(
    data: &[u8],
) -> BTreeMap<String, Vec<TMyNoSqlEntity>> {
    let slice_iterator = SliceIterator::new(data);

    let mut json_array_iterator = JsonArrayIterator::new(slice_iterator);

    let mut result = BTreeMap::new();

    while let Some(db_entity) = json_array_iterator.get_next() {
        if let Err(err) = &db_entity {
            panic!(
                "Table: {}. The whole array of json entities is broken. Err: {:?}",
                TMyNoSqlEntity::TABLE_NAME,
                err
            );
        }

        let db_entity_data = db_entity.unwrap();

        let el = TMyNoSqlEntity::deserialize_entity(db_entity_data.as_bytes(&json_array_iterator));

        let partition_key = el.get_partition_key();
        if !result.contains_key(partition_key) {
            result.insert(partition_key.to_string(), Vec::new());
        }

        result.get_mut(partition_key).unwrap().push(el);
    }

    result
}
