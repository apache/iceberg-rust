use std::fmt::Formatter;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::de;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::Error;
use crate::error::Result as IcebergResult;
use crate::ErrorKind::Unexpected;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// A NewType wrapping DateTime<Utc>
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Create new Timestamp
    pub fn new(ms_since_epoch: i64) -> IcebergResult<Timestamp> {
        let naive_date_time =
            NaiveDateTime::from_timestamp_millis(ms_since_epoch).ok_or(Error::new(
                Unexpected,
                format!(
                "{ms_since_epoch} milliseconds is out of range for a NaiveDateTime."
            ),
            ))?;

        Ok(Timestamp(naive_date_time.and_utc()))
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0.timestamp_millis())
    }
}

struct I64Visitor;

impl<'de> Visitor<'de> for I64Visitor {
    type Value = i64;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("an integer between -(2^63) and (2^63)-1")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        i64::try_from(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ms_since_epoch = deserializer.deserialize_any(I64Visitor)?;
        Timestamp::new(ms_since_epoch).map_err(|e| de::Error::custom(e.message()))
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::timestamp::Timestamp;

    #[test]
    fn round_trip() {
        let timestamp = Timestamp::new(3).unwrap();
        let str = serde_json::to_string(&timestamp).unwrap();
        let deserialized = serde_json::from_str::<Timestamp>(&str).unwrap();
        assert_eq!(timestamp, deserialized);
    }
}
