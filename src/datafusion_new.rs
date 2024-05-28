use datafusion::common::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;

pub async fn get_datafusion_new() -> datafusion::error::Result<()> {
    let field_datetime = Field::new(
        "date_time",
        DataType::Timestamp(TimeUnit::Second, Some("Europe/London".into())),
        true,
    );
    let field_1 = Field::new("price", DataType::Float32, true);
    let field_2 = Field::new("price", DataType::Float32, true);
    let field_3 = Field::new("price", DataType::Float32, true);
    let field_4 = Field::new("price", DataType::Float32, true);
    let field_5 = Field::new("price", DataType::Float32, true);
    let field_6 = Field::new("price", DataType::Float32, true);

    let schema = Schema::new(vec![
        field_datetime,
        field_1,
        field_2,
        field_3,
        field_4,
        field_5,
        field_6,
    ]);

    let ctx = SessionContext::new();
    let csv_read_options = CsvReadOptions::new().has_header(false).schema(&schema);
    let df = ctx.read_csv("new_without.csv", csv_read_options).await?;

    df.show().await?;
    Ok(())
}
