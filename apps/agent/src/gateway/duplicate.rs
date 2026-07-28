//! Duplicate-key-aware JSON parsing shared across gateway raw JSON boundaries.
//!
//! serde's default `Deserialize` for object maps silently overwrites earlier
//! occurrences of the same key with the last value. Wire contract validation
//! must fail-closed on duplicate object keys so a peer cannot smuggle two
//! values for a field such as `seq` or `generation`.

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

/// Parse raw JSON into a `serde_json::Value` while rejecting any object that
/// contains duplicate keys. Also rejects trailing tokens after the top-level
/// value.
pub(crate) fn parse_duplicate_checked_bytes(bytes: &[u8]) -> serde_json::Result<serde_json::Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = DuplicateCheckedValue::deserialize(&mut deserializer)?.0;
    deserializer.end()?;
    Ok(value)
}

struct DuplicateCheckedValue(serde_json::Value);

impl<'de> Deserialize<'de> for DuplicateCheckedValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DuplicateCheckedValueVisitor)
    }
}

struct DuplicateCheckedValueVisitor;

impl<'de> Visitor<'de> for DuplicateCheckedValueVisitor {
    type Value = DuplicateCheckedValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(DuplicateCheckedValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(serde_json::Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(serde_json::Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        while let Some(value) = sequence.next_element::<DuplicateCheckedValue>()? {
            values.push(value.0);
        }
        Ok(DuplicateCheckedValue(serde_json::Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::with_capacity(object.size_hint().unwrap_or_default());
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(format_args!(
                    "duplicate object key {key:?}"
                )));
            }
            let value = object.next_value::<DuplicateCheckedValue>()?;
            values.insert(key, value.0);
        }
        Ok(DuplicateCheckedValue(serde_json::Value::Object(values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Dummy {
        a: i32,
        b: String,
    }

    #[test]
    fn rejects_duplicate_top_level_keys() {
        let raw = br#"{"a":1,"a":2,"b":"x"}"#;
        let err = parse_duplicate_checked_bytes(raw).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"));
    }

    #[test]
    fn rejects_duplicate_nested_keys() {
        let raw = br#"{"a":{"x":1,"x":2},"b":"x"}"#;
        let err = parse_duplicate_checked_bytes(raw).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"));
    }

    #[test]
    fn accepts_unique_keys_and_round_trips() {
        let raw = br#"{"a":1,"b":"x"}"#;
        let value = parse_duplicate_checked_bytes(raw).unwrap();
        let parsed: Dummy = serde_json::from_value(value).unwrap();
        assert_eq!(
            parsed,
            Dummy {
                a: 1,
                b: "x".to_owned()
            }
        );
    }

    #[test]
    fn rejects_trailing_tokens() {
        let raw = br#"{"a":1}{"b":2}"#;
        let err = parse_duplicate_checked_bytes(raw).unwrap_err();
        assert!(err.to_string().contains("trailing"));
    }
}
