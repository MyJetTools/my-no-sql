use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::sync_to_main::UpdateEntityStatisticsData;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{my_no_sql_connector::MyNoSqlConnector, MyNoSqlDataReaderInner};

pub struct GetEntitiesBuilderInner<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    partition_key: String,
    update_statistic_data: UpdateEntityStatisticsData,
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
    connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static>
    GetEntitiesBuilderInner<TMyNoSqlEntity>
{
    pub fn new(
        partition_key: String,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
        connector: Arc<dyn MyNoSqlConnector + Send + Sync + 'static>,
    ) -> Self {
        Self {
            partition_key,
            update_statistic_data: UpdateEntityStatisticsData::default(),
            inner,
            connector,
        }
    }

    pub fn set_partition_last_read_moment(&mut self) {
        self.update_statistic_data.partition_last_read_moment = true;
    }

    pub fn set_row_last_read_moment(&mut self) {
        self.update_statistic_data.row_last_read_moment = true;
    }

    pub fn set_partition_expiration_moment(&mut self, value: Option<DateTimeAsMicroseconds>) {
        self.update_statistic_data.partition_expiration_moment = Some(value);
    }

    pub fn set_row_expiration_moment(&mut self, value: Option<DateTimeAsMicroseconds>) {
        self.update_statistic_data.row_expiration_moment = Some(value);
    }

    pub async fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let db_rows = {
            let reader = self.inner.data.read().await;
            reader.get_by_partition_as_vec(self.partition_key.as_str())
        }?;

        if let Some(sync_handler) = self.connector.get_sync_handler() {
            sync_handler
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    &self.partition_key,
                    || db_rows.iter().map(|itm| itm.get_row_key()),
                    &self.update_statistic_data,
                )
                .await;
        }

        Some(db_rows)
    }

    pub async fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let db_rows = {
            let reader = self.inner.data.read().await;
            reader.get_by_partition_as_vec_with_filter(&self.partition_key, filter)
        }?;

        if let Some(sync_handler) = self.connector.get_sync_handler() {
            sync_handler
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    &self.partition_key,
                    || db_rows.iter().map(|itm| itm.get_row_key()),
                    &self.update_statistic_data,
                )
                .await;
        }

        Some(db_rows)
    }

    pub async fn get_as_btree_map(&self) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let db_rows = {
            let reader = self.inner.data.read().await;
            reader.get_by_partition(&self.partition_key).cloned()
        }?;

        if let Some(sync_handler) = self.connector.get_sync_handler() {
            sync_handler
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    &self.partition_key,
                    || db_rows.values().map(|itm| itm.get_row_key()),
                    &self.update_statistic_data,
                )
                .await;
        }

        Some(db_rows)
    }

    pub async fn get_as_btree_map_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let db_rows = {
            let reader = self.inner.data.read().await;
            reader.get_by_partition_with_filter(&self.partition_key, filter)
        }?;

        if let Some(sync_handler) = self.connector.get_sync_handler() {
            sync_handler
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    &self.partition_key,
                    || db_rows.values().map(|itm| itm.get_row_key()),
                    &self.update_statistic_data,
                )
                .await;
        }

        Some(db_rows)
    }
}
