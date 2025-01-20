use anyhow::Ok;

mod connections;
pub use connections::*;
mod schema_file;
pub use schema_file::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let name_server = "server";
    let query = r#"query"#;

    let file_parquet = "teste.parquet";

    let schema_sql: Vec<MSchema> = shema_mssql_query(query, name_server).await?;
    let schema = create_schema_parquet(&schema_sql);

    //let mut buf = Vec::new();
    //printer::print_schema(&mut buf, &schema);

    //let string_schema = String::from_utf8(buf)?;
    //println!("{}", string_schema);

    let mut client = connect_server(name_server).await?;
    let stream = client.query(query, &[]).await?;

    write_parquet_from_stream(stream, Arc::new(schema), &schema_sql, file_parquet).await?;

    Ok(())
}
