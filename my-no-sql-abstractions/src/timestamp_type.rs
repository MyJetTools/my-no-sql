use std::fmt::{Debug, Display};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::{Deserialize, Deserializer};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn to_date_time(&self) -> DateTimeAsMicroseconds {
        DateTimeAsMicroseconds::new(self.0)
    }
}

impl Into<Timestamp> for DateTimeAsMicroseconds {
    fn into(self) -> Timestamp {
        Timestamp(self.unix_microseconds)
    }
}

impl Into<DateTimeAsMicroseconds> for Timestamp {
    fn into(self) -> DateTimeAsMicroseconds {
        DateTimeAsMicroseconds::new(self.0)
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.to_date_time().to_rfc3339();
        f.write_str(timestamp.as_str())
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.to_date_time().to_rfc3339();
        f.debug_tuple("Timestamp").field(&timestamp).finish()
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let rfc3339 = self.to_date_time().to_rfc3339();
        serializer.serialize_str(&rfc3339)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let datetime = DateTimeAsMicroseconds::from_str(s.as_str());

        if datetime.is_none() {
            println!("Can not parse timestamp: {}", s);
        }
        Ok(Timestamp(datetime.unwrap().unix_microseconds))
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self(0)
    }
}

impl Into<Timestamp> for i64 {
    fn into(self) -> Timestamp {
        Timestamp(self)
    }
}

impl Into<Timestamp> for u64 {
    fn into(self) -> Timestamp {
        Timestamp(self as i64)
    }
}

#[cfg(test)]
mod test {
    use rust_extensions::date_time::DateTimeAsMicroseconds;
    use serde::{Deserialize, Serialize};

    use super::Timestamp;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct MyType {
        pub my_field: i32,
        pub timestamp: Timestamp,
    }

    #[test]
    fn test_serialization() {
        use rust_extensions::date_time::DateTimeAsMicroseconds;

        let my_type = MyType {
            my_field: 15,
            timestamp: DateTimeAsMicroseconds::from_str("2025-01-01T12:00:00.123456")
                .unwrap()
                .into(),
        };

        println!("{:?}", my_type);

        let serialized = serde_json::to_string(&my_type).unwrap();

        println!("Serialized: {}", serialized);

        let result_type: MyType = serde_json::from_str(serialized.as_str()).unwrap();

        assert_eq!(my_type.my_field, result_type.my_field);
        assert_eq!(my_type.timestamp.0, result_type.timestamp.0);
    }
}
