use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;

use crate::my_no_sql_connector::MyNoSqlConnector;

pub struct MockConnectionInner {}

impl MockConnectionInner {
    pub fn new() -> Self {
        Self {}
    }
}

impl MyNoSqlConnector for MockConnectionInner {
    fn get_sync_handler(&self) -> Option<&SyncToMainNodeHandler> {
        None
    }
}
