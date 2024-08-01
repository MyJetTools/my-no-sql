use std::{marker::PhantomData, time::Duration};

use my_no_sql_abstractions::{DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer};

use crate::{DataWriterError, UpdateReadStatistics};

use super::fl_url_factory::FlUrlFactory;

pub struct MyNoSqlDataWriterWithRetries<TEntity: MyNoSqlEntity + Sync + Send> {
    fl_url_factory: FlUrlFactory,
    sync_period: DataSynchronizationPeriod,
    attempt_delay: Duration,
    phantom: PhantomData<TEntity>,
    max_attempts: usize,
}

impl<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>
    MyNoSqlDataWriterWithRetries<TEntity>
{
    pub fn new(
        fl_url_factory: FlUrlFactory,
        sync_period: DataSynchronizationPeriod,
        attempt_delay: Duration,
        max_attempts: usize,
    ) -> Self {
        Self {
            phantom: PhantomData,
            sync_period,
            attempt_delay,
            max_attempts,
            fl_url_factory,
        }
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;

            let result = super::execution::insert_entity(fl_url, entity, &self.sync_period).await;

            if result.is_ok() {
                return result;
            }

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn insert_or_replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result =
                super::execution::insert_or_replace_entity(fl_url, entity, &self.sync_period).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn bulk_insert_or_replace(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result =
                super::execution::bulk_insert_or_replace(fl_url, entities, &self.sync_period).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;

            let result = super::execution::get_entity(
                fl_url,
                partition_key,
                row_key,
                update_read_statistics.as_ref(),
            )
            .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::get_by_partition_key(
                fl_url,
                partition_key,
                update_read_statistics.as_ref(),
            )
            .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
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
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::get_enum_case_models_by_partition_key(
                fl_url,
                update_read_statistics.as_ref(),
            )
            .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
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
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result =
                super::execution::get_enum_case_model(fl_url, update_read_statistics.as_ref())
                    .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn get_by_row_key(
        &self,
        row_key: &str,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::get_by_row_key(fl_url, row_key).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
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
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::delete_enum_case(fl_url).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
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
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::delete_enum_case_with_row_key(fl_url, row_key).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn delete_row(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::delete_row(fl_url, partition_key, row_key).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn delete_partitions(&self, partition_keys: &[&str]) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;

            let result =
                super::execution::delete_partitions(fl_url, TEntity::TABLE_NAME, partition_keys)
                    .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn get_all(&self) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;

            let result = super::execution::get_all(fl_url).await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn clean_table_and_bulk_insert(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result =
                super::execution::clean_table_and_bulk_insert(fl_url, entities, &self.sync_period)
                    .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }

    pub async fn clean_partition_and_bulk_insert(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let mut attempt_no = 0;
        loop {
            let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
            let result = super::execution::clean_partition_and_bulk_insert(
                fl_url,
                partition_key,
                entities,
                &self.sync_period,
            )
            .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    handle_retry_error(err, attempt_no, self.max_attempts, self.attempt_delay)
                        .await?;
                    attempt_no += 1
                }
            }
        }
    }
}

async fn handle_retry_error(
    err: DataWriterError,
    attempt_no: usize,
    max_attempts: usize,
    attempt_delay: Duration,
) -> Result<(), DataWriterError> {
    if attempt_no < max_attempts {
        tokio::time::sleep(attempt_delay).await;
        return Ok(());
    }

    Err(err)
}
