//! Testing out csv handling, using serde
//!
//! Using [Energy Stats UK Agile data](https://energy-stats.uk/)
//!
//! get-content .\csv_agile_A_Eastern_England.csv | cargo run
use super::*;
use chrono::prelude::*;
use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Europe::London;
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;

mod my_date_format {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S%z";

    /// The signature of a deserialize_with function must follow the pattern:
    ///
    ///    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    ///    where
    ///        D: Deserializer<'de>
    ///
    /// although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}

mod my_tz_date_format {
    use chrono::DateTime;
    use chrono_tz::Tz;
    use serde::{self, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S%z";

    pub fn serialize<S>(date: &DateTime<Tz>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
}

mod my_time_format {
    use chrono::NaiveTime;
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%H:%M";

    pub fn serialize<S>(time: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", time.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let time = NaiveTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(time)
    }
}

/// type for importing csv Octopus data
/// from [Energy Stats UK](https://files.energy-stats.uk/csv_output/)
///
/// # Example
///
/// ```
/// use csvtutor::OctopusImportCSV;
/// let reading = OctopusImportCSV {
///    timestamp: DateTime<Utc>,
///    time: NaiveTime,
///    a: String,
///    region: String,
///    price: f32,
/// }
/// ```
#[derive(Debug, Deserialize, Clone)]
struct OctopusImportCSV {
    #[serde(with = "my_date_format")]
    timestamp: DateTime<Utc>,
    #[serde(with = "my_time_format")]
    time: NaiveTime,
    #[allow(dead_code)]
    a: String,
    #[allow(dead_code)]
    region: String,
    price: f32,
}

#[derive(Debug, Serialize, Clone)]
struct OctopusImportPriceHistory {
    #[serde(with = "my_tz_date_format")]
    timestamp: DateTime<Tz>,
    #[serde(with = "my_time_format")]
    time: NaiveTime,
    price: f32,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    //! run() does the main work
    let readings = get_readings()?;

    let weekend_readings: Vec<OctopusImportPriceHistory> = readings
        .iter()
        .filter(|r| r.timestamp.weekday() == Weekday::Sat || r.timestamp.weekday() == Weekday::Sun)
        .cloned()
        .collect();

    let weekday_readings: Vec<OctopusImportPriceHistory> = readings
        .iter()
        .filter(|r| r.timestamp.weekday() == Weekday::Sat || r.timestamp.weekday() == Weekday::Sun)
        .cloned()
        .collect();

    print_mean(weekend_readings);

    print_mean(weekday_readings);

    Ok(())
}

fn print_mean(filtered_readings: Vec<OctopusImportPriceHistory>) {
    let mut map = BTreeMap::new();

    for reading in &filtered_readings {
        let thing = map.entry(reading.time).or_insert(vec![reading.price]);
        thing.push(reading.price);
    }

    for (k, v) in &map {
        let sum = v.clone().into_iter().sum::<f32>();
        let mean = sum / v.len() as f32;
        println!("{k} {}", mean);
    }
}

fn get_readings() -> Result<Vec<OctopusImportPriceHistory>, Box<dyn Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(CSV_FILE)?;
    let mut readings: Vec<OctopusImportPriceHistory> = Vec::new();
    for result in rdr.deserialize() {
        let record: OctopusImportCSV = result?;
        let converted = record.timestamp.with_timezone(&London);
        readings.push(OctopusImportPriceHistory {
            timestamp: converted,
            time: record.time,
            price: record.price,
        });
    }
    Ok(readings)
}
