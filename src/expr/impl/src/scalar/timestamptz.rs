// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Write;

use anyhow::{anyhow, bail, Context as _, Result};
use num_traits::CheckedNeg;
use risingwave_common::types::{CheckedAdd, Interval, IntoOrdered, Timestamp, Timestamptz, F64};
use risingwave_expr::function;

/// Just a wrapper to reuse the `map_err` logic.
#[inline(always)]
pub fn time_zone_err(inner_err: String) -> anyhow::Error {
    anyhow!("{}", inner_err)
}

#[function("to_timestamp(float8) -> timestamptz")]
pub fn f64_sec_to_timestamptz(elem: F64) -> Result<Timestamptz> {
    // TODO(#4515): handle +/- infinity
    let micros = (elem.0 * 1e6)
        .into_ordered()
        .try_into()
        .ok()
        .context("numeric out of range")?;
    Ok(Timestamptz::from_micros(micros))
}

#[function("at_time_zone(timestamp, varchar) -> timestamptz")]
pub fn timestamp_at_time_zone(input: Timestamp, time_zone: &str) -> Result<Timestamptz> {
    let time_zone = Timestamptz::lookup_time_zone(time_zone).map_err(time_zone_err)?;
    // https://www.postgresql.org/docs/current/datetime-invalid-input.html
    // Special cases:
    // * invalid time during daylight forward
    //   * PostgreSQL uses UTC offset before the transition
    //   * We report an error (FIXME)
    // * ambiguous time during daylight backward
    //   * We follow PostgreSQL to use UTC offset after the transition
    let instant_local = input
        .0
        .and_local_timezone(time_zone)
        .latest()
        .ok_or_else(|| {
            anyhow!("fail to interpret local timestamp \"{input}\" in time zone \"{time_zone}\"")
        })?;
    let usec = instant_local.timestamp_micros();
    Ok(Timestamptz::from_micros(usec))
}

#[function("cast_with_time_zone(timestamptz, varchar) -> varchar")]
pub fn timestamptz_to_string(
    elem: Timestamptz,
    time_zone: &str,
    writer: &mut impl Write,
) -> Result<()> {
    let time_zone = Timestamptz::lookup_time_zone(time_zone).map_err(time_zone_err)?;
    let instant_local = elem.to_datetime_in_zone(time_zone);
    write!(
        writer,
        "{}",
        instant_local.format("%Y-%m-%d %H:%M:%S%.f%:z")
    )
    .unwrap();
    Ok(())
}

// Tries to interpret the string with a timezone, and if failing, tries to interpret the string as a
// timestamp and then adjusts it with the session timezone.
#[function("cast_with_time_zone(varchar, varchar) -> timestamptz")]
pub fn str_to_timestamptz(elem: &str, time_zone: &str) -> Result<Timestamptz> {
    elem.parse()
        .or_else(|_| timestamp_at_time_zone(elem.parse::<Timestamp>()?, time_zone))
}

#[function("at_time_zone(timestamptz, varchar) -> timestamp")]
pub fn timestamptz_at_time_zone(input: Timestamptz, time_zone: &str) -> Result<Timestamp> {
    let time_zone = Timestamptz::lookup_time_zone(time_zone).map_err(time_zone_err)?;
    let instant_local = input.to_datetime_in_zone(time_zone);
    let naive = instant_local.naive_local();
    Ok(Timestamp(naive))
}

/// This operation is zone agnostic.
#[function("subtract(timestamptz, timestamptz) -> interval")]
pub fn timestamptz_timestamptz_sub(l: Timestamptz, r: Timestamptz) -> Result<Interval> {
    let usecs = l
        .timestamp_micros()
        .checked_sub(r.timestamp_micros())
        .context("numeric out of range: overflow")?;
    let interval = Interval::from_month_day_usec(0, 0, usecs);
    // https://github.com/postgres/postgres/blob/REL_15_3/src/backend/utils/adt/timestamp.c#L2697
    let interval = interval
        .justify_hour()
        .context("numeric out of range: overflow")?;
    Ok(interval)
}

#[function("subtract_with_time_zone(timestamptz, interval, varchar) -> timestamptz")]
pub fn timestamptz_interval_sub(
    input: Timestamptz,
    interval: Interval,
    time_zone: &str,
) -> Result<Timestamptz> {
    timestamptz_interval_add(
        input,
        interval
            .checked_neg()
            .context("numeric out of range: overflow")?,
        time_zone,
    )
}

#[function("add_with_time_zone(timestamptz, interval, varchar) -> timestamptz")]
pub fn timestamptz_interval_add(
    input: Timestamptz,
    interval: Interval,
    time_zone: &str,
) -> Result<Timestamptz> {
    use num_traits::Zero as _;

    // A month may have 28-31 days, a day may have 23 or 25 hours during Daylight Saving switch.
    // So their interpretation depends on the local time of a specific zone.
    let qualitative = interval.truncate_day();
    // Units smaller than `day` are zone agnostic.
    let quantitative = interval - qualitative;

    let mut t = input;
    if !qualitative.is_zero() {
        // Only convert into and from naive local when necessary because it is lossy.
        // See `e2e_test/batch/functions/issue_12072.slt.part` for the difference.
        let naive = timestamptz_at_time_zone(t, time_zone)?;
        let naive = naive
            .checked_add(qualitative)
            .context("numeric out of range: overflow")?;
        t = timestamp_at_time_zone(naive, time_zone)?;
    }
    let t = timestamptz_interval_quantitative(t, quantitative, i64::checked_add)?;
    Ok(t)
}

// Retained mostly for backward compatibility with old query plans. The signature is also useful for
// binder type inference.
#[function("add(timestamptz, interval) -> timestamptz")]
pub fn timestamptz_interval_add_legacy(l: Timestamptz, r: Interval) -> Result<Timestamptz> {
    timestamptz_interval_quantitative(l, r, i64::checked_add)
}

#[function("subtract(timestamptz, interval) -> timestamptz")]
pub fn timestamptz_interval_sub_legacy(l: Timestamptz, r: Interval) -> Result<Timestamptz> {
    timestamptz_interval_quantitative(l, r, i64::checked_sub)
}

#[function("add(interval, timestamptz) -> timestamptz")]
pub fn interval_timestamptz_add_legacy(l: Interval, r: Timestamptz) -> Result<Timestamptz> {
    timestamptz_interval_add_legacy(r, l)
}

#[inline(always)]
fn timestamptz_interval_quantitative(
    l: Timestamptz,
    r: Interval,
    f: fn(i64, i64) -> Option<i64>,
) -> Result<Timestamptz> {
    // Without session TimeZone, we cannot add month/day in local time. See #5826.
    if r.months() != 0 || r.days() != 0 {
        bail!("timestamp with time zone +/- interval of days");
    }
    let delta_usecs = r.usecs();
    let usecs = f(l.timestamp_micros(), delta_usecs).context("numeric out of range")?;
    Ok(Timestamptz::from_micros(usecs))
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::iter_util::ZipEqFast;

    use super::*;

    #[test]
    fn test_time_zone_conversion() {
        let zones = ["US/Pacific", "ASIA/SINGAPORE", "europe/zurich"];
        #[rustfmt::skip]
        let test_cases = [
            // winter
            ["2022-01-01 00:00:00Z", "2021-12-31 16:00:00", "2022-01-01 08:00:00", "2022-01-01 01:00:00"],
            // summer
            ["2022-07-01 00:00:00Z", "2022-06-30 17:00:00", "2022-07-01 08:00:00", "2022-07-01 02:00:00"],
            // before and after PST -> PDT, where [02:00, 03:00) are invalid
            ["2022-03-13 09:59:00Z", "2022-03-13 01:59:00", "2022-03-13 17:59:00", "2022-03-13 10:59:00"],
            ["2022-03-13 10:00:00Z", "2022-03-13 03:00:00", "2022-03-13 18:00:00", "2022-03-13 11:00:00"],
            // before and after CET -> CEST, where [02:00. 03:00) are invalid
            ["2022-03-27 00:59:00Z", "2022-03-26 17:59:00", "2022-03-27 08:59:00", "2022-03-27 01:59:00"],
            ["2022-03-27 01:00:00Z", "2022-03-26 18:00:00", "2022-03-27 09:00:00", "2022-03-27 03:00:00"],
            // before and after CEST -> CET, where [02:00, 03:00) are ambiguous
            ["2022-10-29 23:59:00Z", "2022-10-29 16:59:00", "2022-10-30 07:59:00", "2022-10-30 01:59:00"],
            ["2022-10-30 02:00:00Z", "2022-10-29 19:00:00", "2022-10-30 10:00:00", "2022-10-30 03:00:00"],
            // before and after PDT -> PST, where [01:00, 02:00) are ambiguous
            ["2022-11-06 07:59:00Z", "2022-11-06 00:59:00", "2022-11-06 15:59:00", "2022-11-06 08:59:00"],
            ["2022-11-06 10:00:00Z", "2022-11-06 02:00:00", "2022-11-06 18:00:00", "2022-11-06 11:00:00"],
        ];
        for case in test_cases {
            let usecs = str_to_timestamptz(case[0], "UTC").unwrap();
            case.iter()
                .skip(1)
                .zip_eq_fast(zones)
                .for_each(|(local, zone)| {
                    let local = local.parse().unwrap();

                    let actual = timestamptz_at_time_zone(usecs, zone).unwrap();
                    assert_eq!(local, actual);

                    let actual = timestamp_at_time_zone(local, zone).unwrap();
                    assert_eq!(usecs, actual);
                });
        }
    }

    #[test]
    fn test_time_zone_conversion_daylight_forward() {
        for (local, zone) in [
            ("2022-03-13 02:00:00", "US/Pacific"),
            ("2022-03-13 02:59:00", "US/Pacific"),
            ("2022-03-27 02:00:00", "europe/zurich"),
            ("2022-03-27 02:59:00", "europe/zurich"),
        ] {
            let actual = timestamp_at_time_zone(local.parse().unwrap(), zone);
            assert!(actual.is_err());
        }
    }

    #[test]
    fn test_time_zone_conversion_daylight_backward() {
        #[rustfmt::skip]
        let test_cases = [
            ("2022-10-30 00:00:00Z", "2022-10-30 02:00:00", "europe/zurich", false),
            ("2022-10-30 00:59:00Z", "2022-10-30 02:59:00", "europe/zurich", false),
            ("2022-10-30 01:00:00Z", "2022-10-30 02:00:00", "europe/zurich", true),
            ("2022-10-30 01:59:00Z", "2022-10-30 02:59:00", "europe/zurich", true),
            ("2022-11-06 08:00:00Z", "2022-11-06 01:00:00", "US/Pacific", false),
            ("2022-11-06 08:59:00Z", "2022-11-06 01:59:00", "US/Pacific", false),
            ("2022-11-06 09:00:00Z", "2022-11-06 01:00:00", "US/Pacific", true),
            ("2022-11-06 09:59:00Z", "2022-11-06 01:59:00", "US/Pacific", true),
        ];
        for (instant, local, zone, preferred) in test_cases {
            let usecs = str_to_timestamptz(instant, "UTC").unwrap();
            let local = local.parse().unwrap();

            let actual = timestamptz_at_time_zone(usecs, zone).unwrap();
            assert_eq!(local, actual);

            if preferred {
                let actual = timestamp_at_time_zone(local, zone).unwrap();
                assert_eq!(usecs, actual)
            }
        }
    }

    #[test]
    fn test_timestamptz_to_and_from_string() {
        let str1 = "1600-11-15 15:35:40.999999+08:00";
        let timestamptz1 = str_to_timestamptz(str1, "UTC").unwrap();
        assert_eq!(timestamptz1.timestamp_micros(), -11648507059000001);

        let mut writer = String::new();
        timestamptz_to_string(timestamptz1, "UTC", &mut writer).unwrap();
        assert_eq!(writer, "1600-11-15 07:35:40.999999+00:00");

        let str2 = "1969-12-31 23:59:59.999999+00:00";
        let timestamptz2 = str_to_timestamptz(str2, "UTC").unwrap();
        assert_eq!(timestamptz2.timestamp_micros(), -1);

        let mut writer = String::new();
        timestamptz_to_string(timestamptz2, "UTC", &mut writer).unwrap();
        assert_eq!(writer, str2);

        // Parse a timestamptz from a str without timezone
        let str3 = "2022-01-01 00:00:00+08:00";
        let timestamptz3 = str_to_timestamptz(str3, "UTC").unwrap();

        let timestamp_from_no_tz =
            str_to_timestamptz("2022-01-01 00:00:00", "Asia/Singapore").unwrap();
        assert_eq!(timestamptz3, timestamp_from_no_tz);
    }
}
