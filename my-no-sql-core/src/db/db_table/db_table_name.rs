use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbTableName(Arc<String>);

impl DbTableName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
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
