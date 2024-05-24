use super::*;
use datafusion::common::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;

pub async fn get_datafusion_data() -> datafusion::error::Result<()> {
    let field_datetime = Field::new(
        "date_time",
        DataType::Timestamp(TimeUnit::Second, None),
        false,
    );
    let field_time = Field::new("time", DataType::Utf8, false);
    let field_a = Field::new("a", DataType::Utf8, false);
    let field_region = Field::new("region", DataType::Utf8, false);
    let field_price = Field::new("price", DataType::Float32, false);

    let schema = Schema::new(vec![
        field_datetime,
        field_time,
        field_a,
        field_region,
        field_price,
    ]);

    let ctx = SessionContext::new();
    let csv_read_options = CsvReadOptions::new().has_header(false).schema(&schema);
    let df = ctx.read_csv(CSV_FILE, csv_read_options).await?;
    let df = df
        .aggregate(vec![col("time")], vec![avg(col("price"))])?
        .sort(vec![col("time").sort(true, true)])?;

    df.show().await?;
    Ok(())
}
