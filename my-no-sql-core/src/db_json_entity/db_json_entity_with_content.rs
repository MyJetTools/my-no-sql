use crate::db::DbRow;

use super::{DbEntityParseFail, DbJsonEntity, JsonTimeStamp};

pub struct DbJsonEntityWithContent<'s> {
    entity: DbJsonEntity,
    raw: &'s [u8],
    time_stamp: JsonTimeStamp,
}

impl<'s> DbJsonEntityWithContent<'s> {
    pub fn new(raw: &'s [u8], time_stamp: JsonTimeStamp, entity: DbJsonEntity) -> Self {
        Self {
            entity,
            raw,
            time_stamp,
        }
    }

    pub fn get_partition_key(&self) -> &str {
        self.entity.get_partition_key(self.raw)
    }

    pub fn get_row_key(&self) -> &str {
        self.entity.get_row_key(self.raw)
    }

    pub fn get_expires(&self) -> Option<&str> {
        self.entity.get_expires(self.raw)
    }

    pub fn into_db_row(self) -> Result<DbRow, DbEntityParseFail> {
        DbJsonEntity::parse_into_db_row(self.raw, &self.time_stamp)
    }
}
