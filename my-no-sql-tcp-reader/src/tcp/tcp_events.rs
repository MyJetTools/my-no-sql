use std::sync::Arc;

use my_no_sql_tcp_shared::{
    sync_to_main::SyncToMainNodeHandler, MyNoSqlReaderTcpSerializer, MyNoSqlTcpContract,
};
use my_tcp_sockets::{tcp_connection::TcpSocketConnection, SocketEventCallback};

use crate::{my_no_sql_connector::MyNoSqlConnector, subscribers::Subscribers};

pub type TcpConnection = TcpSocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>;
pub struct TcpEvents {
    app_name: String,
    pub subscribers: Subscribers,
    pub sync_handler: Arc<SyncToMainNodeHandler>,
}

impl TcpEvents {
    pub fn new(app_name: String, sync_handler: Arc<SyncToMainNodeHandler>) -> Self {
        Self {
            app_name,
            subscribers: Subscribers::new(),
            sync_handler,
        }
    }
    pub async fn handle_incoming_packet(
        &self,
        _tcp_contract: MyNoSqlTcpContract,
        _connection: Arc<TcpConnection>,
    ) {
    }
}

#[async_trait::async_trait]
impl SocketEventCallback<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()> for TcpEvents {
    async fn connected(
        &self,
        connection: Arc<TcpSocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>>,
    ) {
        let contract = MyNoSqlTcpContract::Greeting {
            name: self.app_name.to_string(),
        };

        connection.send(&contract).await;

        for table in self.subscribers.get_tables_to_subscribe().await {
            let contract = MyNoSqlTcpContract::Subscribe {
                table_name: table.to_string(),
            };

            connection.send(&contract).await;
        }

        self.sync_handler
            .tcp_events_pusher_new_connection_established(connection);
    }

    async fn disconnected(
        &self,
        connection: Arc<TcpSocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>>,
    ) {
        self.sync_handler
            .tcp_events_pusher_connection_disconnected(connection);
    }

    async fn payload(
        &self,
        _connection: &Arc<TcpSocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>>,
        contract: MyNoSqlTcpContract,
    ) {
        match contract {
            MyNoSqlTcpContract::Ping => {}
            MyNoSqlTcpContract::Pong => {}
            MyNoSqlTcpContract::Greeting { name: _ } => {}
            MyNoSqlTcpContract::Subscribe { table_name: _ } => {}
            MyNoSqlTcpContract::InitTable { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().init_table(data).await;
                }
            }
            MyNoSqlTcpContract::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event
                        .as_ref()
                        .init_partition(partition_key.as_str(), data)
                        .await;
                }
            }
            MyNoSqlTcpContract::UpdateRows { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().update_rows(data).await;
                }
            }
            MyNoSqlTcpContract::DeleteRows { table_name, rows } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    let mut items = Vec::with_capacity(rows.len());

                    for row in rows {
                        items.push(crate::subscribers::DeleteRowEvent {
                            partition_key: row.partition_key,
                            row_key: row.row_key,
                        })
                    }
                    update_event.as_ref().delete_rows(items).await;
                }
            }
            MyNoSqlTcpContract::Error { message } => {
                panic!("Server error: {}", message);
            }
            MyNoSqlTcpContract::GreetingFromNode {
                node_location: _,
                node_version: _,
                compress: _,
            } => {}
            MyNoSqlTcpContract::SubscribeAsNode(_) => {}
            MyNoSqlTcpContract::Unsubscribe(_) => {}
            MyNoSqlTcpContract::TableNotFound(_) => {}
            MyNoSqlTcpContract::CompressedPayload(_) => {}
            MyNoSqlTcpContract::Confirmation { confirmation_id } => self
                .sync_handler
                .tcp_events_pusher_got_confirmation(confirmation_id),
            MyNoSqlTcpContract::UpdatePartitionsLastReadTime {
                confirmation_id: _,
                table_name: _,
                partitions: _,
            } => {}
            MyNoSqlTcpContract::UpdateRowsLastReadTime {
                confirmation_id: _,
                table_name: _,
                partition_key: _,
                row_keys: _,
            } => {}
            MyNoSqlTcpContract::UpdatePartitionsExpirationTime {
                confirmation_id: _,
                table_name: _,
                partitions: _,
            } => {}
            MyNoSqlTcpContract::UpdateRowsExpirationTime {
                confirmation_id: _,
                table_name: _,
                partition_key: _,
                row_keys: _,
                expiration_time: _,
            } => {}
        }
    }
}

impl MyNoSqlConnector for TcpEvents {
    fn get_sync_handler(&self) -> Option<&SyncToMainNodeHandler> {
        Some(self.sync_handler.as_ref())
    }
}
