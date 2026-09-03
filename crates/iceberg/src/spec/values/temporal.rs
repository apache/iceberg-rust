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

//! Temporal value conversions for dates, times, and timestamps

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use crate::{Error, ErrorKind, Result};

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_DAY: i128 = 86_400 * NANOS_PER_SECOND;

fn invalid_iso_value(kind: &str) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("Invalid ISO-8601 {kind} value"),
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Howard Hinnant's proleptic Gregorian calendar conversion. Unlike chrono,
    // this covers every date reachable by Iceberg's i32 day and i64 microsecond
    // representations.
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month as u32, day as u32)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn format_iso_year(year: i64) -> String {
    match year {
        0..=9_999 => format!("{year:04}"),
        -9_999..=-1 => format!("-{:04}", -year),
        10_000.. => format!("+{year}"),
        _ => year.to_string(),
    }
}

fn format_iso_date(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{}-{month:02}-{day:02}", format_iso_year(year))
}

fn parse_iso_date(value: &str) -> Result<i64> {
    let (year_and_month, day) = value
        .rsplit_once('-')
        .ok_or_else(|| invalid_iso_value("date"))?;
    let (year, month) = year_and_month
        .rsplit_once('-')
        .ok_or_else(|| invalid_iso_value("date"))?;
    if month.len() != 2
        || day.len() != 2
        || !month.bytes().all(|byte| byte.is_ascii_digit())
        || !day.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_iso_value("date"));
    }

    let (sign, year_digits) = match year.as_bytes().first() {
        Some(b'+') => (1_i64, &year[1..]),
        Some(b'-') => (-1_i64, &year[1..]),
        _ => (1_i64, year),
    };
    let signed = year.starts_with(['+', '-']);
    if year_digits.len() < 4
        || year_digits.len() > 10
        || (!signed && year_digits.len() != 4)
        || (year.starts_with('+') && year_digits.len() == 4)
        || !year_digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_iso_value("date"));
    }
    let magnitude = year_digits
        .parse::<i64>()
        .map_err(|err| invalid_iso_value("date").with_source(err))?;
    if year.starts_with('-') && magnitude == 0 {
        return Err(invalid_iso_value("date"));
    }
    let year = sign * magnitude;
    let month = month
        .parse::<u32>()
        .map_err(|err| invalid_iso_value("date").with_source(err))?;
    let day = day
        .parse::<u32>()
        .map_err(|err| invalid_iso_value("date").with_source(err))?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(invalid_iso_value("date"));
    }
    Ok(days_from_civil(year, month, day))
}

fn parse_two_digits(value: &str, kind: &str) -> Result<u32> {
    if value.len() != 2 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_iso_value(kind));
    }
    value
        .parse::<u32>()
        .map_err(|err| invalid_iso_value(kind).with_source(err))
}

fn parse_iso_time_nanos(value: &str) -> Result<i128> {
    let mut components = value.split(':');
    let hour = parse_two_digits(
        components.next().ok_or_else(|| invalid_iso_value("time"))?,
        "time",
    )?;
    let minute = parse_two_digits(
        components.next().ok_or_else(|| invalid_iso_value("time"))?,
        "time",
    )?;
    let second_and_fraction = components.next();
    if components.next().is_some() {
        return Err(invalid_iso_value("time"));
    }

    let (second, nanos) = if let Some(second_and_fraction) = second_and_fraction {
        let (second, fraction) = second_and_fraction
            .split_once('.')
            .map_or((second_and_fraction, None), |(second, fraction)| {
                (second, Some(fraction))
            });
        let second = parse_two_digits(second, "time")?;
        let nanos = if let Some(fraction) = fraction {
            if fraction.is_empty()
                || fraction.len() > 9
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(invalid_iso_value("time"));
            }
            let value = fraction
                .parse::<u32>()
                .map_err(|err| invalid_iso_value("time").with_source(err))?;
            value * 10_u32.pow(9 - fraction.len() as u32)
        } else {
            0
        };
        (second, nanos)
    } else {
        (0, 0)
    };

    if hour > 23 || minute > 59 || second > 59 {
        return Err(invalid_iso_value("time"));
    }
    Ok(
        (i128::from(hour) * 3_600 + i128::from(minute) * 60 + i128::from(second))
            * NANOS_PER_SECOND
            + i128::from(nanos),
    )
}

fn parse_iso_datetime_nanos(value: &str) -> Result<i128> {
    let separator = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, 'T' | 't').then_some(index))
        .ok_or_else(|| invalid_iso_value("timestamp"))?;
    let days = parse_iso_date(&value[..separator])?;
    let nanos = parse_iso_time_nanos(&value[separator + 1..])?;
    Ok(i128::from(days) * NANOS_PER_DAY + nanos)
}

fn parse_utc_offset(value: &str) -> Result<i32> {
    if matches!(value, "Z" | "z") {
        return Ok(0);
    }
    let (sign, digits) = match value.as_bytes().first() {
        Some(b'+') => (1_i32, &value[1..]),
        Some(b'-') => (-1_i32, &value[1..]),
        _ => return Err(invalid_iso_value("UTC offset")),
    };
    let components = digits.split(':').collect::<Vec<_>>();
    if !matches!(components.as_slice(), [_, _] | [_, _, _]) {
        return Err(invalid_iso_value("UTC offset"));
    }
    let hours = parse_two_digits(components[0], "UTC offset")? as i32;
    let minutes = parse_two_digits(components[1], "UTC offset")? as i32;
    let seconds = components
        .get(2)
        .map_or(Ok(0), |value| parse_two_digits(value, "UTC offset"))? as i32;
    if hours > 18 || minutes > 59 || seconds > 59 || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return Err(invalid_iso_value("UTC offset"));
    }
    Ok(sign * (hours * 3_600 + minutes * 60 + seconds))
}

fn parse_iso_offset_datetime_nanos(value: &str) -> Result<(i128, i32)> {
    let time_start = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, 'T' | 't').then_some(index + 1))
        .ok_or_else(|| invalid_iso_value("timestamptz"))?;
    let offset_start = value[time_start..]
        .char_indices()
        .find_map(|(index, character)| {
            matches!(character, 'Z' | 'z' | '+' | '-').then_some(time_start + index)
        })
        .ok_or_else(|| invalid_iso_value("timestamptz"))?;
    let local_nanos = parse_iso_datetime_nanos(&value[..offset_start])?;
    let offset_seconds = parse_utc_offset(&value[offset_start..])?;
    Ok((
        local_nanos - i128::from(offset_seconds) * NANOS_PER_SECOND,
        offset_seconds,
    ))
}

fn nanos_to_microseconds(nanos: i128) -> Result<i64> {
    i64::try_from(nanos / 1_000).map_err(|err| {
        Error::new(
            ErrorKind::DataInvalid,
            "Timestamp is outside the representable microsecond range",
        )
        .with_source(err)
    })
}

fn datetime_components(micros: i64) -> (i64, i64, i64, i64, i64) {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    let days = micros.div_euclid(MICROS_PER_DAY);
    let micros_of_day = micros.rem_euclid(MICROS_PER_DAY);
    let seconds_of_day = micros_of_day / 1_000_000;
    let micros_of_second = micros_of_day % 1_000_000;
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    (days, hour, minute, second, micros_of_second)
}

fn format_iso_datetime(micros: i64, with_offset: bool) -> String {
    let (days, hour, minute, second, micros_of_second) = datetime_components(micros);
    let mut result = format!(
        "{}T{hour:02}:{minute:02}:{second:02}",
        format_iso_date(days)
    );
    if micros_of_second != 0 {
        let fraction = format!("{micros_of_second:06}");
        result.push('.');
        result.push_str(fraction.trim_end_matches('0'));
    }
    if with_offset {
        result.push_str("+00:00");
    }
    result
}

fn format_display_datetime(micros: i64, with_utc: bool) -> String {
    let (days, hour, minute, second, micros_of_second) = datetime_components(micros);
    let mut result = format!(
        "{} {hour:02}:{minute:02}:{second:02}",
        format_iso_date(days)
    );
    if micros_of_second % 1_000 != 0 {
        result.push_str(&format!(".{micros_of_second:06}"));
    } else if micros_of_second != 0 {
        result.push_str(&format!(".{:03}", micros_of_second / 1_000));
    }
    if with_utc {
        result.push_str(" UTC");
    }
    result
}

pub(crate) mod date {
    use super::*;

    pub(crate) fn days_to_iso_date(days: i32) -> String {
        format_iso_date(i64::from(days))
    }

    pub(crate) fn iso_date_to_days(value: &str) -> Result<i32> {
        i32::try_from(parse_iso_date(value)?).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                "Date is outside the representable day range",
            )
            .with_source(err)
        })
    }

    /// Returns unix epoch.
    pub(crate) fn unix_epoch() -> DateTime<Utc> {
        Utc.timestamp_nanos(0)
    }

    /// Creates date literal from `NaiveDate`, assuming it's utc timezone.
    pub(crate) fn date_from_naive_date(date: NaiveDate) -> i32 {
        (date - unix_epoch().date_naive()).num_days() as i32
    }
}

pub(crate) mod time {
    use super::*;

    pub(crate) fn time_to_microseconds(time: &NaiveTime) -> i64 {
        time.signed_duration_since(
            // This is always the same and shouldn't fail
            NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap(),
        )
        .num_microseconds()
        .unwrap()
    }

    pub(crate) fn microseconds_to_time(micros: i64) -> NaiveTime {
        let (secs, rem) = (micros / 1_000_000, micros % 1_000_000);

        NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, rem as u32 * 1_000).unwrap()
    }
}

pub(crate) mod timestamp {
    use super::*;

    pub(crate) fn microseconds_to_iso_datetime(micros: i64) -> String {
        format_iso_datetime(micros, false)
    }

    pub(crate) fn microseconds_to_display(micros: i64) -> String {
        format_display_datetime(micros, false)
    }

    pub(crate) fn iso_datetime_to_microseconds(value: &str) -> Result<i64> {
        nanos_to_microseconds(parse_iso_datetime_nanos(value)?)
    }

    pub(crate) fn nanoseconds_to_datetime(nanos: i64) -> NaiveDateTime {
        DateTime::from_timestamp_nanos(nanos).naive_utc()
    }

    /// Nanoseconds since the Unix epoch, or `None` if outside the representable `i64` range
    /// (roughly the years 1678–2262).
    pub(crate) fn datetime_to_nanoseconds(time: &NaiveDateTime) -> Option<i64> {
        time.and_utc().timestamp_nanos_opt()
    }
}

pub(crate) mod timestamptz {
    use super::*;

    pub(crate) fn microseconds_to_iso_datetime(micros: i64) -> String {
        format_iso_datetime(micros, true)
    }

    pub(crate) fn microseconds_to_display(micros: i64) -> String {
        format_display_datetime(micros, true)
    }

    pub(crate) fn iso_datetime_to_microseconds(value: &str) -> Result<i64> {
        let (nanos, offset_seconds) = parse_iso_offset_datetime_nanos(value)?;
        if offset_seconds != 0 {
            return Err(invalid_iso_value("UTC timestamptz"));
        }
        nanos_to_microseconds(nanos)
    }

    pub(crate) fn nanoseconds_to_datetimetz(nanos: i64) -> DateTime<Utc> {
        let (secs, rem) = (
            nanos.div_euclid(1_000_000_000),
            nanos.rem_euclid(1_000_000_000),
        );

        DateTime::from_timestamp(secs, rem as u32).unwrap()
    }

    /// Nanoseconds since the Unix epoch, or `None` if outside the representable `i64` range
    /// (roughly the years 1678–2262).
    pub(crate) fn datetimetz_to_nanoseconds(time: &DateTime<Utc>) -> Option<i64> {
        time.timestamp_nanos_opt()
    }
}
