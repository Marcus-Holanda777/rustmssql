use anyhow::Ok;
use tiberius::{AuthMethod, Client, Config};
use tiberius::{Query, QueryItem, QueryStream};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::compat::Compat;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[derive(Debug)]
pub struct MSchema {
    pub column_name: Option<String>,
    pub data_type: Option<String>,
    pub is_nullable: Option<String>,
    pub numeric_precision: Option<u8>,
    pub numeric_scale: Option<i32>,
    pub datetime_precision: Option<i16>,
}

pub async fn connect_server(server: &str) -> anyhow::Result<Client<Compat<TcpStream>>> {
    let mut config: Config = Config::new();
    config.host(server);
    config.port(1433);
    config.authentication(AuthMethod::Integrated);
    config.trust_cert();

    let tcp_stream: TcpStream = TcpStream::connect(config.get_addr()).await?;
    tcp_stream.set_nodelay(true)?;

    let client: Client<Compat<TcpStream>> =
        Client::connect(config, tcp_stream.compat_write()).await?;

    Ok(client)
}

pub async fn shema_mssql(database: &str, table_name: &str) -> anyhow::Result<Vec<MSchema>> {
    let mut schema: Vec<MSchema> = Vec::new();
    let mut client: Client<Compat<TcpStream>> = connect_server("cosmos").await?;

    let sql: String = format!(
        r#"
        select
             column_name
            ,data_type
            ,is_nullable
            ,numeric_precision
            ,numeric_scale
            ,datetime_precision
        from {}.INFORMATION_SCHEMA.columns
        where TABLE_NAME = '{}'
       "#,
        database, table_name
    );

    let select: Query<'_> = Query::new(sql);
    let mut stream: QueryStream<'_> = select.query(&mut client).await?;

    while let Some(row) = stream.try_next().await? {
        if let QueryItem::Row(r) = row {
            let ms_schema: MSchema = MSchema {
                column_name: r.get(0).map(|f: &str| f.to_string()),
                data_type: r.get(1).map(|f: &str| f.to_string()),
                is_nullable: r.get(2).map(|f: &str| f.to_string()),
                numeric_precision: r.get(3),
                numeric_scale: r.get(4),
                datetime_precision: r.get(5),
            };

            schema.push(ms_schema);
        }
    }

    Ok(schema)
}
