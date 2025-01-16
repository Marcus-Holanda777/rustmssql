use anyhow::Ok;
use parquet::schema::printer;

mod connections;
pub use connections::*;
mod schema_file;
pub use schema_file::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let schema_sql: Vec<MSchema> = shema_mssql("cosmospdp", "recuperavel_coleta_cab").await?;
    let schema = create_schema_parquet(schema_sql);

    let mut buf = Vec::new();
    printer::print_schema(&mut buf, &schema);

    let string_schema = String::from_utf8(buf).unwrap();
    
    println!("{}", string_schema);

    Ok(())
}
