// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use iceberg::Result;
use iceberg::expr::Predicate;
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType, Transform};
use iceberg::transform::{BoxedTransformFunction, create_transform_function};
use regex::Regex;
use serde_json::{Value, json};

use super::operator;

static STRING_HASH: LazyLock<BoxedTransformFunction> = LazyLock::new(|| {
    create_transform_function(&Transform::Bucket(i32::MAX as u32))
        .expect("valid string bucket transform")
});

static TEMPORAL_STRING: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?:",
        r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})|",
        r"(?P<time>[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?)|",
        r"(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?)",
        r"(?P<zone>[-+][0-9]{2}:[0-9]{2}|Z)?",
        r")\z",
    ))
    .expect("valid temporal literal patterns")
});

pub(super) fn redact_predicate(predicate: &Predicate, now: DateTime<Utc>) -> Result<Value> {
    Ok(match predicate {
        Predicate::AlwaysTrue => json!(true),
        Predicate::AlwaysFalse => json!(false),
        Predicate::And(expr) | Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            json!({
                "type": if matches!(predicate, Predicate::And(_)) { "and" } else { "or" },
                "left": redact_predicate(left, now)?,
                "right": redact_predicate(right, now)?,
            })
        }
        Predicate::Not(expr) => json!({
            "type": "not",
            "child": redact_predicate(expr.inputs()[0], now)?,
        }),
        Predicate::Unary(expr) => json!({
            "type": operator(expr.op())?,
            "term": expr.term().name(),
        }),
        Predicate::Binary(expr) => json!({
            "type": operator(expr.op())?,
            "term": expr.term().name(),
            "value": redact_literal(expr.literal(), now)?,
        }),
        Predicate::Set(expr) => {
            let mut values = expr
                .literals()
                .iter()
                .map(|literal| redact_literal(literal, now))
                .collect::<Result<Vec<_>>>()?;
            values.sort_unstable();
            json!({
                "type": operator(expr.op())?,
                "term": expr.term().name(),
                "values": values,
            })
        }
    })
}

fn redact_literal(datum: &Datum, now: DateTime<Utc>) -> Result<String> {
    Ok(match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => redact_date(*days, now),
        (PrimitiveType::Time, _) => "(time)".to_owned(),
        (PrimitiveType::Timestamp | PrimitiveType::Timestamptz, PrimitiveLiteral::Long(micros)) => {
            redact_timestamp(*micros, now)
        }
        (
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(nanos),
        ) => redact_timestamp(nanos.div_euclid(1_000), now),
        (PrimitiveType::Unknown, _)
        | (_, PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin) => "(unknown)".to_owned(),
        (_, PrimitiveLiteral::Int(value)) => redact_number(f64::from(*value), "int"),
        (_, PrimitiveLiteral::Long(value)) => redact_number(*value as f64, "int"),
        (_, PrimitiveLiteral::Float(value)) => redact_number(f64::from(value.0), "float"),
        (_, PrimitiveLiteral::Double(value)) => redact_number(value.0, "float"),
        (_, PrimitiveLiteral::String(value)) => return redact_string(value, now),
        _ => return hash_string(&datum.to_human_string()),
    })
}

fn redact_number(value: f64, kind: &str) -> String {
    let digits = if value == 0.0 {
        1
    } else {
        (value.abs().log10() as i32).wrapping_add(1)
    };
    format!("({digits}-digit-{kind})")
}

fn redact_date(days: i32, now: DateTime<Utc>) -> String {
    let today = now.timestamp().div_euclid(86_400);
    let difference = today - i64::from(days);
    let distance = difference.unsigned_abs();
    if distance == 0 {
        "(date-today)".to_owned()
    } else if distance < 90 {
        let direction = if difference > 0 { "ago" } else { "from-now" };
        format!("(date-{distance}-days-{direction})")
    } else {
        "(date)".to_owned()
    }
}

fn redact_timestamp(micros: i64, now: DateTime<Utc>) -> String {
    let difference = i128::from(now.timestamp_millis()) * 1_000 - i128::from(micros);
    let distance = difference.unsigned_abs();
    let hours = distance / 3_600_000_000;
    let direction = if difference > 0 { "ago" } else { "from-now" };
    if distance < 300_000_000 {
        "(timestamp-about-now)".to_owned()
    } else if hours <= 72 {
        format!("(timestamp-{hours}-hours-{direction})")
    } else if hours < 90 * 24 {
        format!("(timestamp-{}-days-{direction})", hours / 24)
    } else {
        "(timestamp)".to_owned()
    }
}

fn redact_string(value: &str, now: DateTime<Utc>) -> Result<String> {
    if let Some(captures) = TEMPORAL_STRING.captures(value) {
        if captures.name("time").is_some() {
            return Ok("(time)".to_owned());
        }
        let parsed = if captures.name("date").is_some() {
            Datum::date_from_str(value)
        } else {
            let timestamp = &captures["timestamp"];
            let zone = captures.name("zone").map_or("", |zone| zone.as_str());
            let normalized = if timestamp.len() == 16 {
                format!("{timestamp}:00{zone}")
            } else {
                value.to_owned()
            };
            if zone.is_empty() {
                Datum::timestamp_from_str(normalized)
            } else {
                Datum::timestamptz_from_str(normalized)
            }
        };
        if let Ok(datum) = parsed {
            return redact_literal(&datum, now);
        }
    }
    hash_string(value)
}

fn hash_string(value: &str) -> Result<String> {
    let hash = STRING_HASH.transform_literal_result(&Datum::string(value))?;
    let PrimitiveLiteral::Int(hash) = hash.literal() else {
        unreachable!("bucket transforms return integers")
    };
    Ok(format!("(hash-{hash:08x})"))
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use iceberg::expr::Reference;

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-11-25T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn test_numeric_magnitudes() {
        for (datum, expected) in [
            (Datum::int(0), "(1-digit-int)"),
            (Datum::int(34), "(2-digit-int)"),
            (Datum::int(i32::MIN), "(10-digit-int)"),
            (Datum::long(i64::MIN), "(19-digit-int)"),
            (Datum::long(i64::MAX), "(19-digit-int)"),
            (Datum::float(34.12_f32), "(2-digit-float)"),
            (Datum::double(-0.0), "(1-digit-float)"),
            (Datum::double(0.1), "(0-digit-float)"),
            (Datum::double(0.0001), "(-3-digit-float)"),
            (Datum::double(f64::MAX), "(309-digit-float)"),
            (Datum::double(f64::NAN), "(1-digit-float)"),
            (Datum::double(f64::INFINITY), "(-2147483648-digit-float)"),
        ] {
            assert_eq!(redact_literal(&datum, now()).unwrap(), expected);
        }
    }

    #[test]
    fn test_string_hashes() {
        for (datum, expected) in [
            (Datum::string("aaa"), "(hash-34d05fb7)"),
            (Datum::string("test"), "(hash-3a6bd213)"),
            (Datum::string("iceberg"), "(hash-481f22d9)"),
            (Datum::bool(true), "(hash-34edcf72)"),
            (Datum::bool(false), "(hash-55619e17)"),
            (Datum::decimal_from_str("12.34").unwrap(), "(hash-4975d6b6)"),
            (Datum::uuid(uuid::Uuid::from_u128(1)), "(hash-7c85510a)"),
            (Datum::string("名🧊"), "(hash-4b688a35)"),
        ] {
            assert_eq!(redact_literal(&datum, now()).unwrap(), expected);
        }
        for value in ["2026-99-99", "2026-11-25T99:99:99", "名🧊"] {
            assert_eq!(
                redact_string(value, now()).unwrap(),
                hash_string(value).unwrap()
            );
        }
    }

    #[test]
    fn test_relative_dates() {
        let today = now().timestamp().div_euclid(86_400) as i32;
        for (offset, expected) in [
            (0, "(date-today)"),
            (-1, "(date-1-days-ago)"),
            (1, "(date-1-days-from-now)"),
            (-89, "(date-89-days-ago)"),
            (89, "(date-89-days-from-now)"),
            (-90, "(date)"),
            (90, "(date)"),
        ] {
            let datum = Datum::date(today + offset);
            assert_eq!(redact_literal(&datum, now()).unwrap(), expected);
            assert_eq!(redact_string(&datum.to_string(), now()).unwrap(), expected);
        }
        for days in [i32::MIN, i32::MAX] {
            assert_eq!(redact_literal(&Datum::date(days), now()).unwrap(), "(date)");
        }
    }

    #[test]
    fn test_relative_timestamps() {
        for (offset, expected) in [
            (0, "(timestamp-about-now)"),
            (299, "(timestamp-about-now)"),
            (-299, "(timestamp-about-now)"),
            (300, "(timestamp-0-hours-from-now)"),
            (-300, "(timestamp-0-hours-ago)"),
            (-5_400, "(timestamp-1-hours-ago)"),
            (5_400, "(timestamp-1-hours-from-now)"),
            (-72 * 3_600, "(timestamp-72-hours-ago)"),
            (-73 * 3_600, "(timestamp-3-days-ago)"),
            (7 * 86_400, "(timestamp-7-days-from-now)"),
            (90 * 86_400, "(timestamp)"),
        ] {
            let timestamp = now() + TimeDelta::seconds(offset);
            for datum in [
                Datum::timestamp_micros(timestamp.timestamp_micros()),
                Datum::timestamptz_micros(timestamp.timestamp_micros()),
                Datum::timestamp_nanos(timestamp.timestamp_nanos_opt().unwrap()),
                Datum::timestamptz_nanos(timestamp.timestamp_nanos_opt().unwrap()),
            ] {
                assert_eq!(redact_literal(&datum, now()).unwrap(), expected);
            }
            for value in [
                timestamp.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
                timestamp.to_rfc3339(),
            ] {
                assert_eq!(redact_string(&value, now()).unwrap(), expected);
            }
        }
        for micros in [i64::MIN, i64::MAX] {
            assert_eq!(redact_timestamp(micros, now()), "(timestamp)");
        }
        assert_eq!(
            redact_string("2026-11-25T13:00+01:00", now()).unwrap(),
            "(timestamp-about-now)"
        );
    }

    #[test]
    fn test_time_literals() {
        for value in ["12:34", "12:34:56", "12:34:56.123456789"] {
            assert_eq!(redact_string(value, now()).unwrap(), "(time)");
        }
        assert_eq!(
            redact_literal(&Datum::time_micros(0).unwrap(), now()).unwrap(),
            "(time)"
        );
    }

    #[test]
    fn test_sanitized_sets_preserve_cardinality() {
        let predicate =
            Reference::new("id").is_in([Datum::int(10), Datum::int(20), Datum::int(300)]);
        assert_eq!(
            redact_predicate(&predicate, now()).unwrap(),
            json!({"type":"in", "term":"id", "values":[
                "(2-digit-int)", "(2-digit-int)", "(3-digit-int)"
            ]})
        );
    }
}
