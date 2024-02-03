use std::sync::Arc;

use my_no_sql_abstractions::MyNoSqlEntity;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{my_no_sql_connector::MyNoSqlConnector, MyNoSqlDataReaderInner};

use super::GetEntityBuilderInner;

pub struct GetEntityBuilder<'s, TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    inner: GetEntityBuilderInner<'s, TMyNoSqlEntity>,
}

impl<'s, TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static>
    GetEntityBuilder<'s, TMyNoSqlEntity>
{
    pub fn new(
        partition_key: &'s str,
        row_key: &'s str,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
        connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
    ) -> Self {
        Self {
            inner: GetEntityBuilderInner::new(partition_key, row_key, inner, connector),
        }
    }

    pub fn set_partition_last_read_moment(mut self) -> Self {
        self.inner.set_partition_last_read_moment();
        self
    }

    pub fn set_row_last_read_moment(mut self) -> Self {
        self.inner.set_row_last_read_moment();
        self
    }

    pub fn set_partition_expiration_moment(
        mut self,
        value: Option<DateTimeAsMicroseconds>,
    ) -> Self {
        self.inner.set_partition_expiration_moment(value);
        self
    }

    pub fn set_row_expiration_moment(mut self, value: Option<DateTimeAsMicroseconds>) -> Self {
        self.inner.set_row_expiration_moment(value);
        self
    }

    pub async fn execute(&self) -> Option<Arc<TMyNoSqlEntity>> {
        self.inner.execute().await
    }
}
