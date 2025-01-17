use anyhow::Ok;
use parquet::schema::printer;

mod connections;
pub use connections::*;
mod schema_file;
pub use schema_file::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let schema_sql: Vec<MSchema> = shema_mssql("cosmospdp", "sistema_nota").await?;
    let schema = create_schema_parquet(schema_sql);

    let mut buf = Vec::new();
    printer::print_schema(&mut buf, &schema);

    let string_schema = String::from_utf8(buf).unwrap();

    println!("{}", string_schema);

    let mut client = connect_server("cosmos").await?;
    let stream = client
        .query("select * from cosmospdp.dbo.sistema_nota", &[])
        .await?;

    write_parquet_from_stream(stream, Arc::new(schema), "teste.parquet").await?;

    Ok(())
}
