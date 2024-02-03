pub struct DeleteRowEvent {
    pub partition_key: String,
    pub row_key: String,
}

#[async_trait::async_trait]
pub trait DataReaderUpdater {
    async fn init_table(&self, data: Vec<u8>);
    async fn init_partition(&self, partition_key: &str, data: Vec<u8>);
    async fn update_rows(&self, data: Vec<u8>);
    async fn delete_rows(&self, rows_to_delete: Vec<DeleteRowEvent>);
}
