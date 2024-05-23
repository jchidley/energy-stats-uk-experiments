//! [](https://github.com/pola-rs/polars/issues/16092#issuecomment-2097457985)
use polars::{lazy::dsl::StrptimeOptions, prelude::*};

use super::*;

/// csv_load_no_schema
/// load from named csv file without a schema, make a clone from selected
/// fields, adjusting some of these fields.
/// Calculate mean.
///
/// # More information
///
/// [Parsing](https://docs.pola.rs/user-guide/transformations/time-series/parsing/)
/// [Grouping, Sorting](https://docs.pola.rs/user-guide/transformations/time-series/rolling/)
pub fn csv_load() {
    let df = LazyCsvReader::new(CSV_FILE)
        .has_header(false)
        .with_try_parse_dates(true)
        .finish()
        .unwrap();

    let time_options = StrptimeOptions {
        format: Some("%H:%M".to_string()),
        strict: true,
        cache: true,
        ..StrptimeOptions::default()
    };

    let df_numerical = df
        .select([
            (col("column_1"))
                // .str()
                // .to_datetime(Some(TimeUnit::Milliseconds), None, date_options, lit(""))
                .alias("Date"),
            (col("column_2"))
                .alias("Time")
                .str()
                .to_time(time_options)
                .alias("Time"),
            (col("column_5")).alias("Price"),
        ])
        .with_columns([col("Date").dt().month().alias("Month")]);

    let q_df_time = df_numerical
        .group_by(vec![col("Month"), col("Time")])
        .agg([col("Price").mean()])
        .sort(
            vec!["Month", "Time"],
            SortMultipleOptions::default().with_maintain_order(true),
        );

    println!("{:?}", q_df_time.explain(true));

    let df_time = q_df_time.collect();

    println!("{}", &df_time.unwrap());
}
