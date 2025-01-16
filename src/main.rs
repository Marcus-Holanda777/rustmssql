use anyhow::Ok;

mod connections;
pub use connections::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client: Vec<MSchema> = shema_mssql("cosmos_v14b", "produto_mestre").await?;

    println!("{:?}", client);

    Ok(())
}
