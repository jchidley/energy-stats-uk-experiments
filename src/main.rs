mod csv_experiments;
mod datafusion_experiments;
mod polars_experiments;
use crate::csv_experiments::run;
use crate::datafusion_experiments::get_datafusion_data;
use crate::polars_experiments::csv_load;
use std::error::Error;

const CSV_FILE: &str = "csv_agile_A_Eastern_England.csv";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run()?;
    csv_load();
    get_datafusion_data().await?;
    Ok(())
}
