use std::{collections::BTreeMap, sync::Arc};

use super::DataReaderUpdater;
use tokio::sync::RwLock;

pub struct Subscribers {
    subscribers: RwLock<BTreeMap<&'static str, Arc<dyn DataReaderUpdater + Send + Sync + 'static>>>,
}

impl Subscribers {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn add(
        &self,
        table_name: &'static str,
        subscriber: Arc<dyn DataReaderUpdater + Send + Sync + 'static>,
    ) {
        let mut write_access = self.subscribers.write().await;

        if write_access.contains_key(table_name) {
            panic!("You already subscribed for the table {}", table_name);
        }

        write_access.insert(table_name, subscriber);
    }

    pub async fn get(
        &self,
        table_name: &str,
    ) -> Option<Arc<dyn DataReaderUpdater + Send + Sync + 'static>> {
        let read_access = self.subscribers.write().await;
        let result = read_access.get(table_name)?;
        Some(result.clone())
    }

    pub async fn get_tables_to_subscribe(&self) -> Vec<String> {
        let read_access = self.subscribers.write().await;
        read_access.keys().map(|itm| itm.to_string()).collect()
    }
}
