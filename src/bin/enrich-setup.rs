#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    color_eyre::install().ok();

    noir_plus_extra::enrich::postgres::db_setup().await?;

    Ok(())
}