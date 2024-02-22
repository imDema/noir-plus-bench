use std::future::Future;

use deadpool_redis::{
    redis::{self, AsyncCommands},
    Config, Connection,
};
use rand::{thread_rng, Rng};
use tokio::task::JoinSet;

use crate::types::Product;

pub use deadpool_redis::Pool;

const REDIS_LIB: &str = include_str!("redis-lib.lua");

async fn populate(pool: &Pool) -> color_eyre::Result<()> {
    // Generate 100 categories
    run_many(pool, 1..=100, |mut db: Connection, i| async move {
        let _: () = db
            .set(format!("cat:{i}"), format!("Category {}", i))
            .await
            .unwrap();
    })
    .await?;
    log::info!("categories done.");

    // // Generate one million products and assign them to a category and random tags
    run_many(pool, 1..=1_000_000, |mut db: Connection, i| async move {
        let p = Product {
            id: i,
            name: format!("Product {i}"),
            description: Some(format!("Description of product {i}")),
            category_id: thread_rng().gen_range(1..=100),
            hits: 0,
        };
        let c = p.category_id;
        let ser = rmp_serde::to_vec(&p).unwrap();

        let _: () = db.set(format!("prod:{i}"), ser).await.unwrap();

        let _: () = db.zadd(format!("cat:{c}:prod:hits"), i, 0).await.unwrap();
    })
    .await?;
    log::info!("products done.");

    log::info!("loading functions...");

    let mut db = pool.get().await.unwrap();

    let _: () = redis::cmd("FUNCTION")
        .arg("LOAD")
        .arg(REDIS_LIB)
        .query_async(&mut db)
        .await?;

    Ok(())
}

async fn run_many<F, Fut>(
    pool: &Pool,
    ids: impl IntoIterator<Item = i32>,
    run: F,
) -> color_eyre::Result<()>
where
    F: Fn(Connection, i32) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut set = JoinSet::new();
    for i in ids {
        let db = pool.get().await?;
        set.spawn((run)(db, i));
        if set.len() == pool.status().max_size {
            set.join_next().await.unwrap().unwrap();
        }
    }
    Ok(while let Some(r) = set.join_next().await {
        r.unwrap();
    })
}

pub async fn db_init() -> color_eyre::Result<Pool> {
    let uri = std::env::var("REDIS_URI").unwrap();

    let cfg = Config::from_url(uri);

    let pool = cfg
        .builder()
        .unwrap()
        .runtime(deadpool::Runtime::Tokio1)
        .max_size(48)
        .build()
        .unwrap();

    Ok(pool)
}

pub async fn db_setup() -> color_eyre::Result<()> {
    let pool = db_init().await?;

    log::info!("checking if already populated...");
    if check_migrated(&pool).await? {
        log::info!("db already setup, skipping");
        return Ok(());
    }

    log::info!("populating...");
    populate(&pool).await?;
    set_migrated(&pool).await?;

    log::info!("init complete");
    Ok(())
}

async fn set_migrated(pool: &Pool) -> color_eyre::Result<()> {
    let mut db = pool.get().await?;
    let _: () = db.set("db:ready", true).await?;
    Ok(())
}

async fn check_migrated(pool: &Pool) -> color_eyre::Result<bool> {
    let mut db = pool.get().await?;
    let r: Option<bool> = db.get("db:ready").await?;
    Ok(r.unwrap_or(false))
}

pub async fn get_product(db: &Pool, id: i32) -> color_eyre::Result<Option<Product>> {
    let mut db = db.get().await?;
    let q: Option<Vec<u8>> = db.get(format!("prod:{id}")).await?;

    let Some(mut p) = q.map(|v| rmp_serde::from_slice::<Product>(&v).unwrap()) else {
        log::warn!("product not found!");
        return Ok(None);
    };

    let score: f32 = db
        .zscore(format!("cat:{}:prod:hits", p.category_id), id)
        .await?;
    p.hits = score as i64;

    Ok(Some(p))
}

pub async fn mget_product(db: &Pool, cat: i32, ids: &[i32]) -> color_eyre::Result<Vec<Product>> {
    assert!(!ids.is_empty(), "ids is empty!");
    let mut db = db.get().await?;

    let keys = ids
        .into_iter()
        .map(|i| format!("prod:{i}"))
        .collect::<Vec<_>>();

    let (ser, scores): (Vec<Vec<u8>>, Vec<f32>) = redis::pipe()
        .mget(keys)
        .zscore_multiple(format!("cat:{cat}:prod:hits"), ids)
        .query_async(&mut db)
        .await?;

    let r: Vec<Product> = ser
        .into_iter()
        .zip(scores.into_iter())
        .map(|(b, s)| {
            let mut p = rmp_serde::from_slice::<Product>(&b).unwrap();
            p.hits = s as i64;
            p
        })
        .collect();

    Ok(r)
}

pub async fn mark_hit(db: &Pool, p: &Product) -> color_eyre::Result<()> {
    let mut db = db.get().await?;

    let Product {
        category_id: c, id, ..
    } = p;
    let q: f32 = db.zincr(format!("cat:{c}:prod:hits"), id, 1).await?;
    log::debug!("incresed {} to {q:.1}", p.id);

    Ok(())
}

pub async fn recommend_0(pool: &Pool, p: &Product) -> color_eyre::Result<Vec<Product>> {
    let mut db = pool.get().await?;
    let c = p.category_id;

    let r: Vec<i32> = redis::cmd("ZRANGE")
        .arg(format!("cat:{c}:prod:hits"))
        .arg(&[0, 4])
        .arg("REV")
        .query_async(&mut db)
        .await?;
    if !r.is_empty() {
        drop(db);
        mget_product(pool, p.category_id, r.as_ref()).await
    } else {
        log::error!("empty response from ZRANGE");
        Ok(vec![])
    }
}



// pub async fn recommend_1(db: &Pool, p: &Product) -> color_eyre::Result<Vec<Product>> {
//     let db = db.get().await?;
//     let q1 = db.prepare("SELECT tag_id FROM ks.product_tag WHERE product_id = ?").await?;
//     let q2 = db.prepare("SELECT product_id FROM ks.product_tag as t WHERE tag_id IN ? ORDER BY p.hits DESC LIMIT 5")
//     let r = db.execute(&q1, (p.id,)).await?;

//     let p = r.rows_typed_or_empty::<Product>().map(Result::unwrap).collect();
//     Ok(p)
// }
