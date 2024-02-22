use std::time::Duration;

use color_eyre::eyre::Context;
use sqlx::migrate::MigrateDatabase;
use sqlx::migrate;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, PgExecutor};

use super::types::*;

pub type Pool = PgPool;

pub async fn db_init_pool() -> color_eyre::Result<PgPool> {
    let url = std::env::var("DATABASE_URL").context("Missing DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(16))
        .max_lifetime(None)
        .idle_timeout(None)
        .min_connections(4)
        .max_connections(8)
        .connect_lazy_with(url.parse::<PgConnectOptions>()?.disable_statement_logging());

    Ok(pool)
}

pub async fn db_setup() -> color_eyre::Result<()> {
    let url = std::env::var("DATABASE_URL").context("Missing DATABASE_URL")?;

    if !sqlx::Postgres::database_exists(&url).await? {
        tracing::info!("creating database");
        sqlx::Postgres::create_database(&url).await?;
    } else {
        tracing::info!("database exists");
    }

    let pool = db_init_pool().await?;
    tracing::info!("populating...");
    migrate!()
        .run(&pool)
        .await
    .context("Failed to run migration")?;

    tracing::info!("init complete");
    Ok(())
}

pub async fn get_product<'c, E: PgExecutor<'c> + 'c>(db: E, id: i32) -> sqlx::Result<Option<Product>> {
    sqlx::query_as::<_, Product>("SELECT * FROM product WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
}

pub async fn mark_hit<'c, E: PgExecutor<'c> + 'c>(db: E, p: &Product) -> sqlx::Result<()> {
    sqlx::query("UPDATE product SET hits = hits + 1 WHERE id = $1")
        .bind(p.id)
        .execute(db)
        .await?;

    Ok(())
}

pub async fn recommend_0<'c, E: PgExecutor<'c> + 'c>(db: E, p: &Product) -> sqlx::Result<Vec<Product>> {
    sqlx::query_as::<_, Product>(
        "SELECT * FROM product WHERE category_id = $1 ORDER BY hits DESC LIMIT 5",
    )
    .bind(p.category_id)
    .fetch_all(db)
    .await
}

pub async fn recommend_1<'c, E: PgExecutor<'c> + 'c>(db: E, p: &Product) -> sqlx::Result<Vec<Product>> {
    sqlx::query_as::<_, Product>(
        "SELECT p.id, p.name, p.description, p.category_id, p.hits FROM product as p, product_tag as t WHERE
p.id = t.product_id AND t.tag_id IN (SELECT tag_id FROM tag WHERE product_id = $1)
ORDER BY p.hits DESC LIMIT 5"
    )
    .bind(p.id)
    .fetch_all(db)
    .await
}
