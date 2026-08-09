//! Strict JSON parsing used by every signed AgentForge wire document.

use std::fmt;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use thiserror::Error;

const MAX_INTEROPERABLE_INTEGER: u64 = 9_007_199_254_740_991;

/// A strict JSON decoding failure.
#[derive(Debug, Error)]
#[error("invalid strict JSON at line {line}, column {column}: {message}")]
pub struct StrictJsonError {
    /// Human-readable parser diagnostic.
    pub message: String,
    /// One-based input line, when reported by `serde_json`.
    pub line: usize,
    /// One-based input column, when reported by `serde_json`.
    pub column: usize,
}

impl From<serde_json::Error> for StrictJsonError {
    fn from(error: serde_json::Error) -> Self {
        Self {
            message: error.to_string(),
            line: error.line(),
            column: error.column(),
        }
    }
}

/// Parse exactly one UTF-8 JSON value while rejecting duplicate object keys.
///
/// `serde_json` additionally rejects invalid UTF-8, non-JSON numbers, trailing
/// commas, trailing input and integers outside its safe in-memory range.
pub fn from_slice(input: &[u8]) -> Result<Value, StrictJsonError> {
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = StrictValue::deserialize(&mut deserializer)?.0;
    deserializer.end()?;
    Ok(value)
}

/// Parse exactly one JSON value while rejecting duplicate object keys.
pub fn from_str(input: &str) -> Result<Value, StrictJsonError> {
    from_slice(input.as_bytes())
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueSeed;

impl<'de> DeserializeSeed<'de> for StrictValueSeed {
    type Value = StrictValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StrictValue::deserialize(deserializer)
    }
}

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value with unique object member names")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.unsigned_abs() > MAX_INTEROPERABLE_INTEGER {
            return Err(E::custom(
                "integer exceeds the interoperable JSON safe range",
            ));
        }
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value > MAX_INTEROPERABLE_INTEGER {
            return Err(E::custom(
                "integer exceeds the interoperable JSON safe range",
            ));
        }
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.fract() == 0.0 && value.abs() > MAX_INTEROPERABLE_INTEGER as f64 {
            return Err(E::custom(
                "integer-valued number exceeds the interoperable JSON safe range",
            ));
        }
        Number::from_f64(value)
            .map(Value::Number)
            .map(StrictValue)
            .ok_or_else(|| E::custom("NaN and Infinity are not valid JSON numbers"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StrictValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        while let Some(value) = sequence.next_element_seed(StrictValueSeed)? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::with_capacity(object.size_hint().unwrap_or_default());
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "duplicate object key {key:?} is forbidden"
                )));
            }
            let value = object.next_value_seed(StrictValueSeed)?;
            values.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(values)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_keys_at_every_depth() {
        let error = from_str(r#"{"outer":{"id":1,"id":2}}"#).unwrap_err();
        assert!(error.message.contains("duplicate object key \"id\""));
    }

    #[test]
    fn rejects_invalid_json_extensions_and_trailing_input() {
        for input in [
            br#"{"n":NaN}"#.as_slice(),
            br#"{"n":Infinity}"#.as_slice(),
            br#"{"a":1,}"#.as_slice(),
            br#"{} {}"#.as_slice(),
            br#"{"n":9007199254740992}"#.as_slice(),
            br#"{"n":-9007199254740992}"#.as_slice(),
            br#"{"n":9007199254740992.0}"#.as_slice(),
            br#"{"n":9007199254740993.0}"#.as_slice(),
            &[0xff],
        ] {
            assert!(from_slice(input).is_err(), "accepted {input:?}");
        }
    }
}
