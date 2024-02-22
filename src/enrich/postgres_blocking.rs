use color_eyre::eyre::Context;

use r2d2_postgres::postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use r2d2_postgres::{postgres, r2d2};

use super::types::*;

pub type PgPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

pub fn db_init_pool() -> eyre::Result<PgPool> {
    let url = std::env::var("DATABASE_URL").context("Missing DATABASE_URL")?;

    let manager = PostgresConnectionManager::new(url.parse().unwrap(), NoTls);
    let pool = r2d2::Pool::builder()
        .max_size(16)
        .min_idle(Some(4))
        .build_unchecked(manager);

    Ok(pool)
}

// pub fn db_setup() -> eyre::Result<()> {
//     let url = std::env::var("DATABASE_URL").context("Missing DATABASE_URL")?;

//     if !sqlx::Postgres::database_exists(&url)? {
//         tracing::info!("creating database");
//         sqlx::Postgres::create_database(&url)?;
//     } else {
//         tracing::info!("database exists");
//     }

//     let pool = db_init_pool()?;
//     tracing::info!("populating...");
//     migrate!()
//         .run(&pool)
//
//     .context("Failed to run migration")?;

//     tracing::info!("init complete");
//     Ok(())
// }

pub fn get_product(db: &mut postgres::Client, id: i32) -> Result<Option<Product>, postgres::Error> {
    db.query_opt("SELECT * FROM product WHERE id = $1", &[&id])
        .map(|o| o.map(Product::from_pg_row))
}

pub fn mark_hit(db: &mut postgres::Client, p: &Product) -> Result<(), postgres::Error> {
    db.execute("UPDATE product SET hits = hits + 1 WHERE id = $1", &[&p.id])?;
    Ok(())
}

pub fn recommend_0(
    db: &mut postgres::Client,
    p: &Product,
) -> Result<Vec<Product>, postgres::Error> {
    let v = db.query(
        "SELECT * FROM product WHERE category_id = $1 ORDER BY hits DESC LIMIT 5",
        &[&p.category_id],
    )?;
    Ok(v.into_iter().map(Product::from_pg_row).collect())
}

pub fn recommend_1(
    db: &mut postgres::Client,
    p: &Product,
) -> Result<Vec<Product>, postgres::Error> {
    let v = db.query(
        "SELECT p.id, p.name, p.description, p.category_id, p.hits FROM product as p, product_tag as t WHERE
p.id = t.product_id AND t.tag_id IN (SELECT tag_id FROM tag WHERE product_id = $1)
ORDER BY p.hits DESC LIMIT 5"
    , &[&p.id])?;
    Ok(v.into_iter().map(Product::from_pg_row).collect())
}
