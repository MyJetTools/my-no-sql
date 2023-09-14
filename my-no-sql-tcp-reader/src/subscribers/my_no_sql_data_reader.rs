use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_server_abstractions::MyNoSqlEntity;

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

    fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity>;

    fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<TMyNoSqlEntity>;

    async fn has_partition(&self, partition_key: &str) -> bool;
}
