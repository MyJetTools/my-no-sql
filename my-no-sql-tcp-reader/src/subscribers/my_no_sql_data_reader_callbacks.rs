use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use super::LazyMyNoSqlEntity;

#[async_trait::async_trait]
pub trait MyNoSqlDataReaderCallBacks<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
>
{
    async fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    );
    async fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static>
    MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> for ()
{
    async fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        panic!("This is a dumb implementation")
    }

    async fn deleted(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        panic!("This is a dumb implementation")
    }
}
