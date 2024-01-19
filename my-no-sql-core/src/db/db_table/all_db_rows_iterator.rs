use std::sync::Arc;

use crate::db::DbPartition;

pub struct DbRowsIterator<'a> {
    db_partition: &'a DbPartition,
    iterator: std::slice::Iter<'a, Arc<crate::db::DbRow>>,
}

pub struct AllDbRowsIterator<'a> {
    partitions_iterator: std::slice::Iter<'a, DbPartition>,
    db_rows_iterator: Option<DbRowsIterator<'a>>,
}

impl<'a> AllDbRowsIterator<'a> {
    pub fn new(partitions_iterator: std::slice::Iter<'a, DbPartition>) -> Self {
        Self {
            partitions_iterator,
            db_rows_iterator: None,
        }
    }
}

impl<'a> Iterator for AllDbRowsIterator<'a> {
    type Item = (&'a DbPartition, &'a Arc<crate::db::DbRow>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.db_rows_iterator.is_none() {
                let db_partition = self.partitions_iterator.next()?;
                let db_rows_iterator = db_partition.get_all_rows();
                self.db_rows_iterator = Some(DbRowsIterator {
                    db_partition,
                    iterator: db_rows_iterator,
                });
            }

            let db_rows_iterator = self.db_rows_iterator.as_mut().unwrap();

            if let Some(db_row) = db_rows_iterator.iterator.next() {
                return Some((db_rows_iterator.db_partition, db_row));
            } else {
                self.db_rows_iterator = None;
            }
        }
    }
}
