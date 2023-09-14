use std::sync::Arc;

use my_no_sql_server_abstractions::MyNoSqlEntity;

#[async_trait::async_trait]
pub trait MyNoSqlDataReaderCallBacks<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    async fn inserted_or_replaced(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>);
    async fn deleted(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>);
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static>
    MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> for ()
{
    async fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<Arc<TMyNoSqlEntity>>,
    ) {
        panic!("This is a dumb implementation")
    }

    async fn deleted(&self, _partition_key: &str, _entities: Vec<Arc<TMyNoSqlEntity>>) {
        panic!("This is a dumb implementation")
    }
}
