use std::sync::Arc;

pub struct DbTableName(Arc<String>);

impl DbTableName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Into<DbTableName> for String {
    fn into(self) -> DbTableName {
        DbTableName(Arc::new(self))
    }
}

impl<'s> Into<DbTableName> for &'s str {
    fn into(self) -> DbTableName {
        DbTableName(Arc::new(self.to_string()))
    }
}
