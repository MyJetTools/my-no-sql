use std::sync::Arc;

use my_json::json_writer::{JsonArrayWriter, RawJsonObject};
use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use rust_extensions::AppStates;

use crate::{data_reader_update::DeleteRowEvent, subscribers::Subscribers, MyNoSqlDataReader};

use super::MockConnectionInner;

pub struct MyNoSqlMockConnection {
    inner: Arc<MockConnectionInner>,
    app_states: Arc<AppStates>,
    subscribers: Subscribers,
}

impl MyNoSqlMockConnection {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockConnectionInner::new()),
            app_states: Arc::new(AppStates::create_initialized()),
            subscribers: Subscribers::new(),
        }
    }

    pub async fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        self.subscribers
            .create_subscriber(self.app_states.clone(), self.inner.clone())
            .await
    }

    pub async fn init_table<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
        entities: Vec<TMyNoSqlEntity>,
    ) {
        let updater = self.subscribers.get(TMyNoSqlEntity::TABLE_NAME).await;

        if let Some(updater) = updater {
            let payload = serialize_entities(entities);
            updater.init_table(payload).await;
        }
    }

    pub async fn init_partitions<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
        entities: Vec<TMyNoSqlEntity>,
    ) {
        if entities.len() == 0 {
            return;
        }

        let updater = self.subscribers.get(TMyNoSqlEntity::TABLE_NAME).await;
        let partition_key = entities[0].get_partition_key().to_string();

        if let Some(updater) = updater {
            let payload = serialize_entities(entities);
            updater.init_partition(&partition_key, payload).await;
        }
    }

    pub async fn update_rows<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
        entities: Vec<TMyNoSqlEntity>,
    ) {
        if entities.len() == 0 {
            return;
        }

        let updater = self.subscribers.get(TMyNoSqlEntity::TABLE_NAME).await;

        if let Some(updater) = updater {
            let payload = serialize_entities(entities);
            updater.update_rows(payload).await;
        }
    }

    pub async fn delete_rows(&self, table_name: &str, entities: Vec<DeleteRowEvent>) {
        if entities.len() == 0 {
            return;
        }

        let updater = self.subscribers.get(table_name).await;

        if let Some(updater) = updater {
            let payload = serialize_entities(entities);
            updater.update_rows(payload).await;
        }
    }
}

fn serialize_entities<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
>(
    entities: Vec<TMyNoSqlEntity>,
) -> Vec<u8> {
    let mut result = Vec::new();

    let mut json_array = JsonArrayWriter::new();

    for entity in entities {
        let payload = entity.serialize_entity();
        let payload: RawJsonObject = payload.into();
        json_array.write(payload);
    }

    result
}
