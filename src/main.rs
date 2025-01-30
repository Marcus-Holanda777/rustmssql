use anyhow::Ok;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
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
    /// nome do usuario
    #[arg(short, long)]
    user: Option<String>,
    /// senha de acesso
    #[arg(short, long)]
    secret: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli: Cli = Cli::parse();

    println!("{}", "=*".repeat(30));
    println!("Servidor: {}", cli.name_server);
    println!("Saida parquet: {}", cli.file_parquet);

    let mut query: String = String::new();

    if let Some(str_query) = cli.query {
        println!("\n=> Query importada ! ...\n");
        query = str_query;
    } else if let Some(file_query) = cli.path_file {
        query = fs::read_to_string(&file_query)?;
        println!("\n=> Arquivo importado ! ...\n");
    };

    let schema_sql: Vec<MSchema> = schema_mssql_query(
        query.as_str(),
        cli.name_server.as_str(),
        cli.user.as_deref(),
        cli.secret.as_deref(),
    )
    .await?;
    let schema = create_schema_parquet(&schema_sql);

    let mut client = connect_server(
        cli.name_server.as_str(),
        cli.user.as_deref(),
        cli.secret.as_deref(),
    )
    .await?;

    let mut select: Query<'_> = Query::new(query);
    for param in cli.parameters {
        select.bind(param);
    }

    // INICIAR A BARRA DE PROGRESSO AQUI

    // Barra de progresso
    let progress = start_progress()?;
    progress.set_message("Consultando ...");

    let stream: QueryStream<'_> = select.query(&mut client).await?;

    progress.finish_with_message("Consulta Finalizada ... ✅");

    let progress = start_progress()?;

    write_parquet_from_stream(
        stream,
        Arc::new(schema),
        &schema_sql,
        cli.file_parquet.as_str(),
        &progress,
    )
    .await?;

    println!("{}", "=*".repeat(30));

    Ok(())
}

fn start_progress() -> anyhow::Result<ProgressBar> {
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("[{elapsed_precise:.magenta}] {spinner:.cyan} {msg}")?
            .tick_strings(&[
                "▰▱▱▱▱▱▱",
                "▰▰▱▱▱▱▱",
                "▰▰▰▱▱▱▱",
                "▰▰▰▰▱▱▱",
                "▰▰▰▰▰▱▱",
                "▰▰▰▰▰▰▱",
                "▰▰▰▰▰▰▰",
                "▰▱▱▱▱▱▱",
            ]),
    );
    progress.enable_steady_tick(std::time::Duration::from_millis(80));
    Ok(progress)
}
