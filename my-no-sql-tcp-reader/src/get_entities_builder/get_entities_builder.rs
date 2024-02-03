use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{my_no_sql_connector::MyNoSqlConnector, MyNoSqlDataReaderInner};

use super::GetEntitiesBuilderInner;

pub struct GetEntitiesBuilder<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    inner: GetEntitiesBuilderInner<TMyNoSqlEntity>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> GetEntitiesBuilder<TMyNoSqlEntity> {
    pub fn new(
        partition_key: String,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
        connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
    ) -> Self {
        Self {
            inner: GetEntitiesBuilderInner::new(partition_key, inner, connector),
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

    pub async fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.inner.get_as_vec().await
    }

    pub async fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.inner.get_as_vec_with_filter(filter).await
    }

    pub async fn get_as_btree_map(&self) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        self.inner.get_as_btree_map().await
    }

    pub async fn get_as_btree_map_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        self.inner.get_as_btree_map_with_filter(filter).await
    }
}
