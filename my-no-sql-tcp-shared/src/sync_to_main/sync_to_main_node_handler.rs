use std::sync::Arc;

use rust_extensions::{events_loop::EventsLoop, ApplicationStates, Logger};

use super::{
    sync_to_main_node_handler_inner::SyncToMainNodeHandlerInner, DataReaderTcpConnection,
    SyncToMainNodeEvent, UpdateEntityStatisticsData,
};

pub struct SyncToMainNodeHandler {
    pub inner: Arc<SyncToMainNodeHandlerInner>,
}

impl SyncToMainNodeHandler {
    pub fn new() -> Self {
        let inner = Arc::new(SyncToMainNodeHandlerInner::new());

        Self { inner }
    }

    pub async fn start(
        &self,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) {
        let mut events_loop = EventsLoop::new("SyncToMainNodeQueues".to_string(), logger);

        events_loop.register_event_loop(self.inner.clone());

        self.inner.set_event_loop(events_loop).await;
        self.inner.start(app_states).await
    }

    pub async fn tcp_events_pusher_new_connection_established(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        let queues = self.inner.queues.lock().await;

        if let Some(events_loop) = queues.events_loop.as_ref() {
            events_loop.send(SyncToMainNodeEvent::Connected(connection));
        }
    }

    pub async fn tcp_events_pusher_connection_disconnected(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        let queues = self.inner.queues.lock().await;
        if let Some(events_loop) = queues.events_loop.as_ref() {
            events_loop.send(SyncToMainNodeEvent::Disconnected(connection));
        }
    }

    pub async fn tcp_events_pusher_got_confirmation(&self, confirmation_id: i64) {
        let queues = self.inner.queues.lock().await;

        if let Some(events_loop) = queues.events_loop.as_ref() {
            events_loop.send(SyncToMainNodeEvent::Delivered(confirmation_id));
        }
    }

    pub async fn update<'s, TRowKeys: Iterator<Item = &'s str>>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_keys: impl Fn() -> TRowKeys,
        data: &UpdateEntityStatisticsData,
    ) {
        if !data.has_data_to_update() {
            return;
        }

        let mut inner = self.inner.queues.lock().await;

        if data.partition_last_read_moment {
            inner
                .update_partitions_last_read_time_queue
                .add_partition(table_name, partition_key);

            if let Some(events_loop) = inner.events_loop.as_ref() {
                events_loop.send(SyncToMainNodeEvent::PingToDeliver);
            }
        }

        if let Some(partition_expiration) = data.partition_expiration_moment {
            inner.update_partition_expiration_time_update.add(
                table_name,
                partition_key,
                partition_expiration,
            );

            if let Some(events_loop) = inner.events_loop.as_ref() {
                events_loop.send(SyncToMainNodeEvent::PingToDeliver);
            }
        }

        if data.row_last_read_moment {
            if data.row_last_read_moment {
                inner
                    .update_rows_last_read_time_queue
                    .add(table_name, partition_key, row_keys());
                if let Some(events_loop) = inner.events_loop.as_ref() {
                    events_loop.send(SyncToMainNodeEvent::PingToDeliver);
                }
            }
        }

        if let Some(row_expiration) = data.row_expiration_moment {
            inner.update_rows_expiration_time_queue.add(
                table_name,
                partition_key,
                row_keys(),
                row_expiration,
            );
            if let Some(events_loop) = inner.events_loop.as_ref() {
                events_loop.send(SyncToMainNodeEvent::PingToDeliver);
            }
        }
    }
}
