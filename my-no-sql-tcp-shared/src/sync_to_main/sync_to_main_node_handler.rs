use std::sync::Arc;

use rust_extensions::{events_loop::EventsLoop, ApplicationStates, Logger};
use tokio::sync::Mutex;

use super::{
    sync_to_main_node_handler_inner::SyncToMainNodeHandlerInner, DataReaderTcpConnection,
    SyncToMainNodeEvent, UpdateEntityStatisticsData,
};

pub struct SyncToMainNodeHandler {
    pub inner: Arc<SyncToMainNodeHandlerInner>,
    events_loop: Mutex<EventsLoop<SyncToMainNodeEvent>>,
}

impl SyncToMainNodeHandler {
    pub fn new(logger: Arc<dyn Logger + Send + Sync + 'static>) -> Self {
        let mut events_loop = EventsLoop::new("SyncToMainNodeQueues".to_string(), logger);

        let events_publisher = events_loop.get_publisher();

        let inner = Arc::new(SyncToMainNodeHandlerInner::new(events_publisher));

        events_loop.register_event_loop(inner.clone());

        Self {
            inner,
            events_loop: Mutex::new(events_loop),
        }
    }

    pub async fn start(&self, app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>) {
        let mut events_loop = self.events_loop.lock().await;
        events_loop.start(app_states);
    }

    pub fn tcp_events_pusher_new_connection_established(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Connected(connection));
    }

    pub fn tcp_events_pusher_connection_disconnected(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Disconnected(connection));
    }

    pub fn tcp_events_pusher_got_confirmation(&self, confirmation_id: i64) {
        self.inner
            .events_publisher
            .send(SyncToMainNodeEvent::Delivered(confirmation_id));
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

            self.inner
                .events_publisher
                .send(SyncToMainNodeEvent::PingToDeliver);
        }

        if let Some(partition_expiration) = data.partition_expiration_moment {
            inner.update_partition_expiration_time_update.add(
                table_name,
                partition_key,
                partition_expiration,
            );

            self.inner
                .events_publisher
                .send(SyncToMainNodeEvent::PingToDeliver);
        }

        if data.row_last_read_moment {
            if data.row_last_read_moment {
                inner
                    .update_rows_last_read_time_queue
                    .add(table_name, partition_key, row_keys());

                self.inner
                    .events_publisher
                    .send(SyncToMainNodeEvent::PingToDeliver);
            }
        }

        if let Some(row_expiration) = data.row_expiration_moment {
            inner.update_rows_expiration_time_queue.add(
                table_name,
                partition_key,
                row_keys(),
                row_expiration,
            );

            self.inner
                .events_publisher
                .send(SyncToMainNodeEvent::PingToDeliver);
        }
    }
}
