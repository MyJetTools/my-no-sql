pub enum DbRowKey<'s> {
    AsString(String),
    AsStr(&'s str),
}

impl<'s> DbRowKey<'s> {
    pub fn as_str(&self) -> &str {
        match self {
            DbRowKey::AsString(s) => s.as_str(),
            DbRowKey::AsStr(s) => s,
        }
    }

    pub fn into_string(self) -> String {
        match self {
            DbRowKey::AsString(s) => s,
            DbRowKey::AsStr(s) => s.to_string(),
        }
    }
}

impl<'s> Into<DbRowKey<'s>> for &'s str {
    fn into(self) -> DbRowKey<'s> {
        DbRowKey::AsStr(self)
    }
}

impl<'s> Into<DbRowKey<'s>> for &'s String {
    fn into(self) -> DbRowKey<'s> {
        DbRowKey::AsStr(self)
    }
}

impl<'s> Into<DbRowKey<'s>> for String {
    fn into(self) -> DbRowKey<'s> {
        DbRowKey::AsString(self)
    }
}
