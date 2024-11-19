use std::{sync::Arc, time::Duration};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::{sync_to_main::SyncToMainNodeHandler, MyNoSqlTcpSerializerFactory};
use my_tcp_sockets::{TcpClient, TlsSettings};
use rust_extensions::{AppStates, StrOrString};

use crate::{
    subscribers::MyNoSqlDataReaderTcp, tcp_events::TcpEvents, MyNoSqlTcpConnectionSettings,
};

pub struct TcpConnectionSettings {
    settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
}

#[async_trait::async_trait]
impl my_tcp_sockets::TcpClientSocketSettings for TcpConnectionSettings {
    async fn get_host_port(&self) -> Option<String> {
        self.settings.get_host_port().await.into()
    }

    async fn get_tls_settings(&self) -> Option<TlsSettings> {
        None
    }
}

pub struct MyNoSqlTcpConnection {
    tcp_client: TcpClient,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_events: Arc<TcpEvents>,
    app_states: Arc<AppStates>,
}

impl MyNoSqlTcpConnection {
    pub fn new(
        app_name: impl Into<StrOrString<'static>>,
        settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
    ) -> Self {
        let settings = TcpConnectionSettings { settings };

        let app_name: StrOrString<'static> = app_name.into();

        Self {
            tcp_client: TcpClient::new("MyNoSqlClient".to_string(), Arc::new(settings)),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
            tcp_events: Arc::new(TcpEvents::new(
                app_name.to_string(),
                Arc::new(SyncToMainNodeHandler::new(my_logger::LOGGER.clone())),
            )),
            app_states: Arc::new(AppStates::create_un_initialized()),
        }
    }

    pub async fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    >(
        &self,
    ) -> Arc<MyNoSqlDataReaderTcp<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber(
                self.app_states.clone(),
                self.tcp_events.sync_handler.clone(),
            )
            .await
    }

    pub async fn start(&self) {
        self.app_states.set_initialized();

        self.tcp_client
            .start(
                Arc::new(MyNoSqlTcpSerializerFactory),
                self.tcp_events.clone(),
                my_logger::LOGGER.clone(),
            )
            .await;

        self.tcp_events
            .sync_handler
            .start(self.app_states.clone())
            .await;
    }
}
