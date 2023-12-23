use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;

use crate::MyNoSqlDataReaderCallBacks;

use super::{GetEntitiesBuilder, GetEntityBuilder};

#[async_trait::async_trait]
pub trait MyNoSqlDataReader<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    async fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>>;

    async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>>;

    async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>>;

    async fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>>;

    async fn get_enum_case_model<
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

    async fn get_enum_case_models_by_partition_key<
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

    async fn get_enum_case_models_by_partition_key_as_vec<
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

    fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity>;

    fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<TMyNoSqlEntity>;

    async fn has_partition(&self, partition_key: &str) -> bool;

    async fn wait_until_first_data_arrives(&self);

    async fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    );
}
