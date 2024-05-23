mod csv_experiments;
mod polars_experiments;
use crate::csv_experiments::run;
use crate::polars_experiments::csv_load;
use std::error::Error;

const CSV_FILE: &str = "csv_agile_A_Eastern_England.csv";

fn main() -> Result<(), Box<dyn Error>> {
    run()?;
    csv_load();
    Ok(())
}
