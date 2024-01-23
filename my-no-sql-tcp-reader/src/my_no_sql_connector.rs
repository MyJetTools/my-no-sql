use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;

pub trait MyNoSqlConnector {
    fn get_sync_handler(&self) -> Option<&SyncToMainNodeHandler>;
}
