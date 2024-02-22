use clap::Parser;
use std::{collections::HashSet, ops::Rem, time::Instant};

use noir_compute::{prelude::*, GroupHasherBuilder, Replication};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use eyre::Result;
use noir_compute::{operator::Operator, Stream};
use rand::prelude::*;
use rand_distr::Exp;

#[derive(Debug, Parser)]
struct Options {
    /// Number of generated events
    #[clap(short('n'), long, default_value_t = 1_000_000)]
    event_number: u64,

    /// 1 / lambda parameter for exponential id distribution
    #[clap(short('l'), long, default_value_t = 20_000)]
    lambda_inv: usize,

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

    // db::db_setup()?;

    let start = Instant::now();
    match opt.shared {
        true => unique_assoc(conf, opt)?,
        false => unique(conf, opt)?,
        // false => unique_new(conf, lambda, opt.event_number, n)?,
    }
    eprintln!("time: {:?}", start.elapsed());
    micrometer::summary_grouped();
    Ok(())
}

fn inspect(id: &i32) {
    let id = *id;
    if id % 50000 == 0 {
        println!("{}-{}", id % 123, id % 67);
    }
}

fn unique_assoc(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config);
    let lambda = 1. / opts.lambda_inv as f32;
    let source = make_source(lambda, &mut env, opts.event_number)?;

    let k = source.unique_assoc().inspect(inspect).collect_count();

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    println!("{:?}", k.get());
    eprintln!("{elapsed:?}");
    Ok(())
}

fn unique(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config);
    let lambda = 1. / opts.lambda_inv as f32;
    let source = make_source(lambda, &mut env, opts.event_number)?;

    let mut set = HashSet::<_, GroupHasherBuilder>::default();
    let k = source
        .replication(Replication::One)
        .rich_flat_map(move |el| {
            if !set.contains(&el) {
                set.insert(el.clone());
                Some(el)
            } else {
                None
            }
        })
        .inspect(inspect)
        .collect_count();

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    println!("{:?}", k.get());
    eprintln!("{elapsed:?}");
    Ok(())
}
