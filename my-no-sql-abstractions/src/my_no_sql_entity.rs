pub trait MyNoSqlEntity {
    const TABLE_NAME: &'static str;
    fn get_partition_key(&self) -> &str;
    fn get_row_key(&self) -> &str;
    fn get_time_stamp(&self) -> i64;

    fn serialize_entity(&self) -> Vec<u8>;
    fn deserialize_entity(src: &[u8]) -> Self;
}


