use std::{ops::Rem, time::Instant};

use clap::Parser;
use eyre::{Context, Result};
use noir_compute::{operator::Operator, prelude::*, Stream};
use noir_plus_extra::enrich::{postgres as pg_async, postgres_blocking as db, types::Product};
use r2d2_postgres::postgres;
use rand::prelude::*;
use rand_distr::Exp;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Options {
    /// Number of generated events
    #[clap(short('n'), long, default_value_t = 1_000_000)]
    event_number: u64,

    /// 1 / lambda parameter for exponential id distribution
    #[clap(short('l'), long, default_value_t = 20_000)]
    lambda_inv: usize,

    /// Size of the memoization caches, disable memoization if None
    #[clap(short('m'), long)]
    memo: Option<usize>,

    #[clap(long, short)]
    shared: bool,
}

fn make_source(
    lambda: f32,
    env: &mut StreamEnvironment,
    events: u64,
) -> eyre::Result<Stream<impl Operator<Out = i32>>> {
    let distr = Exp::new(lambda)?;
    let source = env
        .stream_par_iter(move |i, n| {
            let mut rng = SmallRng::seed_from_u64(i ^ 0xfeeddabeef);
            (0..events / n).map(move |_| distr.sample(&mut rng).max(1.0).rem(1_000_000.) as i32)
        })
        .batch_mode(BatchMode::adaptive(8192, std::time::Duration::from_secs(1)));
    Ok(source)
}

fn main() -> Result<()> {
    color_eyre::install().ok();
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    let (conf, args) = EnvironmentConfig::from_args();
    conf.spawn_remote_workers();
    let opt = Options::try_parse_from(args)?;
    tracing::info!("config: {opt:?}");

    let name = if let Some(n) = &opt.memo {
        format!("memo{n}")
    } else {
        "nomemo".into()
    };
    let lambda = 1. / opt.lambda_inv as f32;

    // db::db_setup()?;

    let start = Instant::now();
    match opt.memo {
        Some(0) | None => pipeline_async(conf, lambda, opt.event_number)?,
        Some(n) => pipeline_async_memo(conf, lambda, opt.event_number, n)?,
    }
    eprintln!("time: {:?}", start.elapsed());
    micrometer::summary_grouped();
    // micrometer::append_csv_uniform(
    //     "target/mm.csv",
    //     &format!("{}-c{}l{:?}", name, opt.event_number, lambda),
    // )?;

    Ok(())
}

fn make_source(
    lambda: f32,
    env: &mut StreamEnvironment,
    events: u64,
) -> eyre::Result<Stream<impl Operator<Out = i32>>> {
    let distr = Exp::new(lambda)?;
    let source = env
        .stream_par_iter(move |i, n| {
            let mut rng = SmallRng::seed_from_u64(i ^ 0xfeeddabeef);
            (0..events / n).map(move |_| distr.sample(&mut rng).max(1.0).rem(1_000_000.) as i32)
        })
        .batch_mode(BatchMode::adaptive(8192, std::time::Duration::from_secs(1)));
    Ok(source)
}

fn inspect((p, rec): (Product, Vec<Product>)) {
    if p.id % 5000 == 0 {
        println!(
            "{}: {}",
            p.id,
            rec.iter().map(|p| format!("{},", p.id)).collect::<String>()
        );
    }
}

async fn map_get_product_async(db: pg_async::Pool, id: i32) -> Option<Product> {
    pg_async::get_product(&db, id)
        .await
        .context("get_product")
        .unwrap()
}

async fn map_get_recommendation_async(db: pg_async::Pool, p: Product) -> (Product, Vec<Product>) {
    let rec = pg_async::recommend_0(&db, &p)
        .await
        .context("recommend")
        .unwrap();
    (p, rec)
}

#[allow(unused)]
async fn map_mark_hit_async(db: pg_async::Pool, p: Product) {
    pg_async::mark_hit(&db, &p)
        .await
        .context("mark_hit")
        .unwrap();
}

fn pipeline_async(conf: EnvironmentConfig, lambda: f32, events: u64) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut env = StreamEnvironment::new(conf);
            let source = make_source(lambda, &mut env, events)?;
            let pool = pg_async::db_init_pool().await?;

            // Load
            let db = pool.clone();
            let s2 = source
                .map_async(move |id| map_get_product_async(db.clone(), id))
                .flatten()
                .filter(|p| p.id % 101 < 57);

            // Recommend
            let db = pool.clone();
            s2
                // .pop()
                // .unwrap()
                .map_async(move |p| map_get_recommendation_async(db.clone(), p))
                .for_each(inspect);

            env.execute().await;
            Ok::<(), eyre::Error>(())
        })?;

    Ok(())
}

fn pipeline_async_memo(
    conf: EnvironmentConfig,
    lambda: f32,
    events: u64,
    memo: usize,
) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut env = StreamEnvironment::new(conf);
            let source = make_source(lambda, &mut env, events)?;
            let pool = pg_async::db_init_pool().await?;

            // Load
            let db = pool.clone();
            let s2 = source
                .map_async_memo(move |id| map_get_product_async(db.clone(), id), memo)
                .flatten()
                .filter(|p| p.id % 101 < 57);

            // Recommend
            let db = pool.clone();
            s2
                // .pop()
                // .unwrap()
                .map_async_memo_by(
                    move |p| map_get_recommendation_async(db.clone(), p),
                    |p| p.category_id,
                    memo,
                )
                .for_each(inspect);

            env.execute().await;
            Ok::<(), eyre::Error>(())
        })?;

    Ok(())
}
