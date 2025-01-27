use anyhow::Ok;
use clap::Parser;
use tiberius::{Query, QueryStream};

mod connections;
pub use connections::*;
mod schema_file;
pub use schema_file::*;
mod converter;
pub use converter::*;

use std::fs;
use std::sync::Arc;

/// Executa uma query no servidor e gera um arquivo parquet com o resultado
#[derive(Parser)]
struct Cli {
    /// nome do servidor
    #[arg(short, long)]
    name_server: String,
    /// query a ser executada
    #[arg(short, long)]
    query: Option<String>,
    /// query a partir de um arquivo
    #[arg(short, long)]
    path_file: Option<std::path::PathBuf>,
    /// arquivo parquet de saída
    #[arg(short, long, default_value = "result_query.parquet")]
    file_parquet: String,
    /// parametro de condicoes da consulta (opcional)
    parameters: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli: Cli = Cli::parse();

    println!("{}", "=*".repeat(30));
    println!("Servidor: {}", cli.name_server);
    println!("Saida parquet: {}", cli.file_parquet);

    let mut query: String = String::new();

    if let Some(str_query) = cli.query {
        println!("\n=> Query importada ! ...\n\n");
        query = str_query;
    } else if let Some(file_query) = cli.path_file {
        query = fs::read_to_string(&file_query)?;
        println!("\n=> Arquivo importado ! ...");
    };

    let schema_sql: Vec<MSchema> =
        shema_mssql_query(query.as_str(), cli.name_server.as_str()).await?;
    let schema = create_schema_parquet(&schema_sql);

    let mut client = connect_server(cli.name_server.as_str()).await?;

    let mut select: Query<'_> = Query::new(query);
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

    println!("{}", "=*".repeat(30));

    Ok(())
}
