use flurl::FlUrl;
use my_no_sql_abstractions::DataSynchronizationPeriod;

pub trait FlUrlExt {
    fn with_table_name_as_query_param(self, table_name: &str) -> FlUrl;
    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> FlUrl;
    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl;
    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> FlUrl;
    fn with_row_key_as_query_param(self, partition_key: &str) -> FlUrl;
}

impl FlUrlExt for FlUrl {
    fn with_table_name_as_query_param(self, table_name: &str) -> FlUrl {
        self.append_query_param("tableName", Some(table_name))
    }

    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> FlUrl {
        let value = match sync_period {
            DataSynchronizationPeriod::Immediately => "i",
            DataSynchronizationPeriod::Sec1 => "1",
            DataSynchronizationPeriod::Sec5 => "5",
            DataSynchronizationPeriod::Sec15 => "15",
            DataSynchronizationPeriod::Sec30 => "30",
            DataSynchronizationPeriod::Min1 => "60",
            DataSynchronizationPeriod::Asap => "a",
        };

        self.append_query_param("syncPeriod", Some(value))
    }

    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl {
        self.append_query_param("partitionKey", Some(partition_key))
    }

    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> FlUrl {
        let mut s = self;
        for partition_key in partition_keys {
            s = s.append_query_param("partitionKey", Some(*partition_key));
        }
        s
    }

    fn with_row_key_as_query_param(self, row_key: &str) -> FlUrl {
        self.append_query_param("rowKey", Some(row_key))
    }
}
