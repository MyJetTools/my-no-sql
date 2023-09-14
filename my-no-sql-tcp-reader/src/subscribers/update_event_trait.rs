use async_trait::async_trait;
use my_no_sql_tcp_shared::DeleteRowTcpContract;

#[async_trait]
pub trait UpdateEvent {
    async fn init_table(&self, data: Vec<u8>);
    async fn init_partition(&self, partition_key: &str, data: Vec<u8>);
    async fn update_rows(&self, data: Vec<u8>);
    async fn delete_rows(&self, rows_to_delete: Vec<DeleteRowTcpContract>);
}
