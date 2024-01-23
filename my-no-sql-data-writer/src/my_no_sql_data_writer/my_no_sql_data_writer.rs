use std::{marker::PhantomData, sync::Arc, time::Duration};

use flurl::FlUrl;
use my_no_sql_abstractions::{DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer};

use serde::{Deserialize, Serialize};

use crate::{MyNoSqlDataWriterWithRetries, MyNoSqlWriterSettings};

use super::{DataWriterError, UpdateReadStatistics};

pub struct CreateTableParams {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub max_rows_per_partition_amount: Option<usize>,
}

impl CreateTableParams {
    pub fn populate_params(&self, mut fl_url: FlUrl) -> FlUrl {
        if let Some(max_partitions_amount) = self.max_partitions_amount {
            fl_url = fl_url.append_query_param(
                "maxPartitionsAmount",
                Some(max_partitions_amount.to_string()),
            )
        };

        if let Some(max_rows_per_partition_amount) = self.max_rows_per_partition_amount {
            fl_url = fl_url.append_query_param(
                "maxRowsPerPartitionAmount",
                Some(max_rows_per_partition_amount.to_string()),
            )
        };

        if !self.persist {
            fl_url = fl_url.append_query_param("persist", Some("false"));
        };

        fl_url
    }
}

pub struct MyNoSqlDataWriter<TEntity: MyNoSqlEntity + Sync + Send> {
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    sync_period: DataSynchronizationPeriod,
    phantom: PhantomData<TEntity>,
}

impl<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send> MyNoSqlDataWriter<TEntity> {
    pub fn new(
        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        auto_create_table_params: Option<CreateTableParams>,
        sync_period: DataSynchronizationPeriod,
    ) -> Self {
        if let Some(create_table_params) = auto_create_table_params {
            tokio::spawn(super::execution::create_table_if_not_exists(
                settings.clone(),
                TEntity::TABLE_NAME,
                create_table_params,
                sync_period,
            ));
        }

        Self {
            settings,
            phantom: PhantomData,
            sync_period,
        }
    }

    pub async fn create_table(&self, params: CreateTableParams) -> Result<(), DataWriterError> {
        super::execution::create_table(
            &self.settings,
            TEntity::TABLE_NAME,
            params,
            &self.sync_period,
        )
        .await
    }

    pub async fn create_table_if_not_exists(
        &self,
        params: CreateTableParams,
    ) -> Result<(), DataWriterError> {
        super::execution::create_table_if_not_exists(
            self.settings.clone(),
            TEntity::TABLE_NAME,
            params,
            self.sync_period,
        )
        .await
    }

    pub fn with_retries(
        &self,
        delay_between_attempts: Duration,
        max_attempts: usize,
    ) -> MyNoSqlDataWriterWithRetries<TEntity> {
        MyNoSqlDataWriterWithRetries::new(
            self.settings.clone(),
            self.sync_period,
            delay_between_attempts,
            max_attempts,
        )
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        super::execution::insert_entity(&self.settings, entity, &self.sync_period).await
    }

    pub async fn insert_or_replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        super::execution::insert_or_replace_entity(&self.settings, entity, &self.sync_period).await
    }

    pub async fn bulk_insert_or_replace(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        super::execution::bulk_insert_or_replace(&self.settings, entities, &self.sync_period).await
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TEntity>, DataWriterError> {
        super::execution::get_entity(
            &self.settings,
            partition_key,
            row_key,
            update_read_statistics.as_ref(),
        )
        .await
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        super::execution::get_by_partition_key(
            &self.settings,
            partition_key,
            update_read_statistics.as_ref(),
        )
        .await
    }

    pub async fn get_enum_case_models_by_partition_key<
        TResult: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + From<TEntity>
            + Sync
            + Send
            + 'static,
    >(
        &self,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TResult>>, DataWriterError> {
        super::execution::get_enum_case_models_by_partition_key(
            &self.settings,
            update_read_statistics.as_ref(),
        )
        .await
    }

    pub async fn get_enum_case_model<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + Sync
            + Send
            + 'static,
    >(
        &self,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TResult>, DataWriterError> {
        super::execution::get_enum_case_model(&self.settings, update_read_statistics.as_ref()).await
    }

    pub async fn get_by_row_key(
        &self,
        row_key: &str,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        super::execution::get_by_row_key(&self.settings, row_key).await
    }

    pub async fn delete_enum_case<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Result<Option<TResult>, DataWriterError> {
        super::execution::delete_enum_case(&self.settings).await
    }

    pub async fn delete_enum_case_with_row_key<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + Sync
            + Send
            + 'static,
    >(
        &self,
        row_key: &str,
    ) -> Result<Option<TResult>, DataWriterError> {
        super::execution::delete_enum_case_with_row_key(&self.settings, row_key).await
    }

    pub async fn delete_row(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        super::execution::delete_row(&self.settings, partition_key, row_key).await
    }

    pub async fn delete_partitions(&self, partition_keys: &[&str]) -> Result<(), DataWriterError> {
        super::execution::delete_partitions(&self.settings, TEntity::TABLE_NAME, partition_keys)
            .await
    }

    pub async fn get_all(&self) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        super::execution::get_all(&self.settings).await
    }

    pub async fn clean_table_and_bulk_insert(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        super::execution::clean_table_and_bulk_insert(&self.settings, entities, &self.sync_period)
            .await
    }

    pub async fn clean_partition_and_bulk_insert(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        super::execution::clean_partition_and_bulk_insert(
            &self.settings,
            partition_key,
            entities,
            &self.sync_period,
        )
        .await
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationFailHttpContract {
    pub reason: String,
    pub message: String,
}
