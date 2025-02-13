use my_no_sql_macros::my_no_sql_entity;
use my_no_sql_sdk::abstractions::Timestamp;
use serde::{Deserialize, Serialize};

#[my_no_sql_entity("test-table")]
#[derive(Debug, Serialize, Deserialize)]
pub struct MyEntity {
    pub expires: Timestamp,
}
