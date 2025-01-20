use anyhow::Ok;
use clap::Parser;
use tiberius::{Query, QueryStream};

mod connections;
pub use connections::*;
mod schema_file;
pub use schema_file::*;
use std::sync::Arc;

/// Executa uma query no servidor e gera um arquivo parquet com o resultado
#[derive(Parser)]
struct Cli {
    /// nome do servidor
    #[arg(short, long)]
    name_server: String,
    /// query a ser executada
    #[arg(short, long)]
    query: String,
    /// arquivo parquet de saída
    #[arg(short, long, default_value = "result_query.parquet")]
    file_parquet: String,
    /// parametro de condicoes do programa
    parameters: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli: Cli = Cli::parse();

    println!("Servidor: {}", cli.name_server);
    println!("Query: {}", cli.query);
    println!("Arquivo parquet: {}", cli.file_parquet);
    println!("Condicoes: {:#?}", cli.parameters);

    let schema_sql: Vec<MSchema> =
        shema_mssql_query(cli.query.as_str(), cli.name_server.as_str()).await?;
    let schema = create_schema_parquet(&schema_sql);

    let mut client = connect_server(cli.name_server.as_str()).await?;

    let mut select: Query<'_> = Query::new(cli.query);
    for param in cli.parameters {
        select.bind(param);
    }

    let stream: QueryStream<'_> = select.query(&mut client).await?;

    write_parquet_from_stream(
        stream,
        Arc::new(schema),
        &schema_sql,
        cli.file_parquet.as_str(),
    )
    .await?;

    Ok(())
}
