use std::{ops::Rem, sync::Arc, time::Instant};

use backoff::{retry_notify, ExponentialBackoff};
use clap::Parser;
use eyre::{Context, Result};
use noir_compute::{operator::Operator, prelude::*, Stream};
use noir_plus_extra::enrich::{postgres_blocking as db, types::Product};
use r2d2_postgres::postgres::{self, NoTls};
use rand::prelude::*;
use rand_distr::Exp;
use sqlx::{Connection, PgExecutor};

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
    memo_n: Option<usize>,

    #[clap(long, short)]
    shared: bool,
}

fn main() -> Result<()> {
    color_eyre::install().ok();
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    let (conf, args) = EnvironmentConfig::from_args();
    conf.spawn_remote_workers();
    let opt = Options::try_parse_from(args)?;
    tracing::info!("config: {opt:?}");

    let name = if let Some(n) = &opt.memo_n {
        format!("memo{n}")
    } else {
        "nomemo".into()
    };
    let lambda = 1. / opt.lambda_inv as f32;

    // db::db_setup()?;

    let start = Instant::now();
    match opt.shared {
        true => pipeline_pool(conf, lambda, opt.event_number)?,
        false => pipeline_nopool(conf, lambda, opt.event_number)?,
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

fn map_get_product(db: &mut postgres::Client, id: i32) -> Option<Product> {
    db::get_product(db, id).context("get_product").unwrap()
}

fn map_get_recommendation(db: &mut postgres::Client, p: Product) -> (Product, Vec<Product>) {
    let rec = db::recommend_0(db, &p).context("recommend").unwrap();
    (p, rec)
}

#[allow(unused)]
fn map_mark_hit(db: &mut postgres::Client, p: Product) {
    db::mark_hit(db, &p).context("mark_hit").unwrap();
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

fn pipeline_nopool(conf: EnvironmentConfig, lambda: f32, events: u64) -> Result<()> {
    let mut env = StreamEnvironment::new(conf);
    let source = make_source(lambda, &mut env, events)?;
    let url = std::env::var("DATABASE_URL").context("Missing DATABASE_URL")?;
    // let url = Arc::new(url);

    fn connect(db_url: &str) -> postgres::Client {
        let op = || {
            postgres::Client::connect(&db_url, NoTls).map_err(|e| {
                let inner = e.into_source().unwrap();
                if inner.is::<std::io::Error>() {
                    backoff::Error::transient(inner)
                } else {
                    backoff::Error::permanent(inner)
                }
            })
        };
        let backoff = ExponentialBackoff::default();
        retry_notify(backoff, op, |e, d| tracing::error!("[{d:8?}] {e}")).unwrap()
    }

    // Load
    let db_url = url.clone();
    let s2 = source
        .map(move |id| {
            let mut conn = connect(&db_url);
            map_get_product(&mut conn, id)
        })
        .flatten()
        .filter(|p| p.id % 101 < 57);

    // Recommend
    let db_url = url.clone();
    s2.map(move |p| {
        let mut conn = connect(&db_url);
        map_get_recommendation(&mut conn, p)
    })
    .for_each(inspect);

    env.execute_blocking();

    Ok(())
}

fn pipeline_pool(conf: EnvironmentConfig, lambda: f32, events: u64) -> Result<()> {
    let mut env = StreamEnvironment::new(conf);
    let source = make_source(lambda, &mut env, events)?;
    let pool = db::db_init_pool()?;

    // Load
    let db = pool.clone();
    let s2 = source
        .map(move |id| {
            let mut db = db.get().unwrap();
            map_get_product(&mut db, id)
        })
        .flatten()
        .filter(|p| p.id % 101 < 57);

    // Recommend
    let db = pool.clone();
    s2.map(move |p| {
        let mut db = db.get().unwrap();
        map_get_recommendation(&mut db, p)
    })
    .for_each(inspect);

    env.execute_blocking();

    Ok(())
}

// Process
// let db = pool.clone();
// s2.pop()
//     .unwrap()
//     .filter(|p| p.id % 101 < 3)
//     .group_by(|p| p.id ^ (p.id >> 3) % 256)
//     .window(CountWindow::sliding(2, 1))
//     .max_by_key(|k| k.id % 53)
//     .drop_key()
//     .map(move |p| map_mark_hit(db.clone(), p))
//     .for_each(std::mem::drop);
