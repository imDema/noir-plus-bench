use ahash::AHashMap as HashMap;
use clap::Parser;
use std::fs::File;
use std::mem::replace;
use std::{io::BufReader, sync::Arc};
use std::time::Instant;

use noir_compute::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const EPS: f64 = 1e-8;
const DAMPENING: f64 = 0.85;

#[derive(clap::Parser)]
struct Options {
    #[clap(short, long)]
    iterations: usize,
    #[clap(short, long)]
    nodes_path: String,
    #[clap(short, long)]
    edges_path: String,
    #[clap(short('N'), long)]
    nodes: usize,

    #[clap(long, short)]
    shared: bool,
}

fn pagerank(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config);

    let num_pages = opts.nodes;
    let pages_source = CsvSource::<u64>::new(opts.nodes_path).has_headers(false);
    let links_source = CsvSource::<(u64, u64)>::new(opts.edges_path).has_headers(false);

    let adj_list = env
        .stream(links_source)
        // construct adjacency list
        .group_by_fold(
            |(x, _y)| *x,
            Vec::new(),
            |edges, (_x, y)| edges.push(y),
            |edges1, mut edges2| edges1.append(&mut edges2),
        )
        .unkey();

    let (dropme, result) = env
        .stream(pages_source)
        // distribute the ranks evenly
        .map(move |x| (x, 0.0, 1.0 / num_pages as f64))
        .iterate(
            opts.iterations,
            // state maintains whether a new iteration is needed
            false,
            move |s, _| {
                s
                    .map(|(x, _, rank)| (x, rank))
                    .join(adj_list, |(x, _rank)| *x, |(x, _adj)| *x)
                    .flat_map(|(_, ((_x, rank), (_, adj)))| {
                        // distribute the rank of the page between the connected pages
                        let rank_to_distribute = rank / adj.len() as f64;
                        adj.into_iter().map(move |y| (y, rank_to_distribute))
                    })
                    .drop_key()
                    .group_by_sum(|(y, _)| *y, |(_y, rank_to_distribute)| rank_to_distribute)
                    // apply dampening factor
                    .rich_map({
                        let mut prev = 0.0;
                        move |(x, rank)| {
                            let rank = rank * DAMPENING + (1.0 - DAMPENING) / num_pages as f64;
                            (*x, replace(&mut prev, rank), rank)
                        }
                    })
                    .drop_key()
            },
            // a new iteration is needed if at least one page's rank has changed
            |changed: &mut bool, (_x, old, new)| {
                *changed = *changed || (new - old).abs() / new > EPS
            },
            |state, changed| *state = *state || changed,
            |state| {
                let condition = *state;
                *state = false;
                condition
            },
        );
    let result = result.collect_vec();
    dropme.for_each(|_| {});

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    std::hint::black_box(result);

    eprintln!("{elapsed:?}");
    Ok(())
}

fn pagerank_shared(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config);

    let num_pages = opts.nodes;
    let pages_source = CsvSource::<u64>::new(opts.nodes_path).has_headers(false);
    // let links_source = CsvSource::<(u64, u64)>::new(opts.edges_path).has_headers(false);

    let links = BufReader::new(File::open(opts.edges_path).unwrap());
    let adjacency_list: HashMap<u64, Vec<u64>> =
        csv::ReaderBuilder::default()
            .from_reader(links)
            .into_deserialize()
            .map(|r| r.unwrap())
            .fold(
                HashMap::with_capacity_and_hasher(num_pages, Default::default()),
                |mut acc, (x, y)| {
                    acc.entry(x).or_default().push(y);
                    acc
                },
            );

    let adjacency_list = Arc::new(adjacency_list);

    let (dropme, result) = env
        .stream(pages_source)
        // distribute the ranks evenly
        .map(move |x| (x, 0.0, 1.0 / num_pages as f64))
        .iterate(
            opts.iterations,
            // state maintains whether a new iteration is needed
            false,
            move |s, _| {
                s.flat_map(move |(x, _, rank)| {
                    if let Some(adj) = adjacency_list.get(&x) {
                        // distribute the rank of the page between the connected pages
                        let distribute = rank / adj.len() as f64;
                        adj.iter().map(move |y| (*y, distribute)).collect()
                    } else {
                        vec![]
                    }
                })
                .group_by_sum(|(x, _)| *x, |(_, rank)| rank)
                .rich_map({
                    let mut prev = 0.0;
                    move |(x, rank)| {
                        let rank = rank * DAMPENING + (1.0 - DAMPENING) / num_pages as f64;
                        (*x, replace(&mut prev, rank), rank)
                    }
                })
                .drop_key()
            },
            // a new iteration is needed if at least one page's rank has changed
            |changed: &mut bool, (_x, old, new)| {
                *changed = *changed || (new - old).abs() / new > EPS
            },
            |state, changed| *state = *state || changed,
            |state| replace(state, false),
        );
    let result = result.collect_vec();
    dropme.for_each(|_| {});

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    std::hint::black_box(result);

    eprintln!("{elapsed:?}");
    Ok(())
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    let (config, args) = EnvironmentConfig::from_args();
    let opts = Options::parse_from(args);

    config.spawn_remote_workers();

    match opts.shared {
        true => pagerank_shared(config, opts),
        false => pagerank(config, opts),
    }?;

    Ok(())
}
