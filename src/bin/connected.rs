use ahash::AHashMap as HashMap;
use clap::Parser;
use std::sync::Arc;
use std::time::Instant;

use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(clap::Parser)]
struct Options {
    #[clap(long, short('i'))]
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

#[derive(Serialize, Deserialize, Clone, Default)]
struct State {
    component: Vec<u64>,
    updated: bool,
    iteration_count: usize,
}

impl State {
    fn new(num_nodes: usize) -> Self {
        Self {
            component: (0..num_nodes as u64).collect(),
            updated: false,
            iteration_count: 0,
        }
    }
}

fn connected_components_join(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config);

    let nodes_source = CsvSource::<u64>::new(opts.nodes_path).has_headers(false);
    let edges_source = CsvSource::<(u64, u64)>::new(opts.edges_path)
        .delimiter(b',')
        .has_headers(false);

    let edges = env
        .stream(edges_source)
        .flat_map(|(x, y)| vec![(x, y), (y, x)]);

    let (result, dropme) = env
        .stream(nodes_source)
        // put each node in its own component
        .map(|x| (x, x))
        .iterate(
            opts.iterations,
            State::new(opts.nodes),
            move |s, state| {
                s.join(edges, |&(x, _component)| x, |&(x, _y)| x)
                    .map(|(_, ((_x, component), (_, y)))| (y, component))
                    .drop_key()
                    .group_by_min_element(|(x, _component)| *x, |(_x, component)| *component)
                    .drop_key()
                    .filter_map(move |(x, component)| {
                        let old_component = state.get().component[x as usize];
                        if old_component <= component {
                            None
                        } else {
                            Some((x, component))
                        }
                    })
            },
            |delta: &mut Vec<(u64, u64)>, (x, component)| {
                delta.push((x, component));
            },
            |state, changes| {
                state.updated = state.updated || !changes.is_empty();
                for (x, component) in changes {
                    state.component[x as usize] = component;
                }
            },
            |state| {
                // stop if there were no changes
                let condition = state.updated;
                state.updated = false;
                state.iteration_count += 1;
                condition
            },
        );
    let result = result.collect_vec();
    dropme.for_each(std::mem::drop);

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    std::hint::black_box(result);

    eprintln!("{elapsed:?}");
    Ok(())
}

fn connected_components_shared(config: EnvironmentConfig, opts: Options) -> eyre::Result<()> {
    let mut env = StreamEnvironment::new(config.clone());
    let nodes_source = CsvSource::<u64>::new(opts.nodes_path).has_headers(false);

    let edges_source = CsvSource::<(u64, u64)>::new(opts.edges_path)
        .delimiter(b',')
        .has_headers(false);

    let edges = env
        .stream(edges_source)
        // edges are undirected
        .flat_map(|(x, y)| vec![(x, y), (y, x)])
        .group_by_fold(
            |t| t.0,
            Vec::<u64>::new(),
            |acc, x| acc.push(x.1),
            |a, b| a.extend(b),
        )
        .collect_all::<HashMap<_, _>>();

    let start = Instant::now();
    env.execute_blocking();

    let edges = Arc::new(edges.get().unwrap());
    
    let mut env = StreamEnvironment::new(config);
    let (result, dropme) = env
        .stream(nodes_source)
        // put each node in its own component
        .map(|x| (x, x))
        .iterate(
            opts.iterations,
            State::new(opts.nodes),
            move |s, state| {
                s.flat_map(move |(x, c)| {
                    edges[&x]
                        .iter()
                        .filter(|&&y| c < y)
                        .map(move |&y| (y, c))
                        .collect::<Vec<_>>()
                })
                .group_by_min_element(|&(x, _c)| x, |&(_x, c)| c)
                .drop_key()
                .filter_map(move |(x, c)| {
                    let old_component = state.get().component[x as usize];
                    if old_component <= c {
                        None
                    } else {
                        Some((x, c))
                    }
                })
            },
            |delta: &mut Vec<(u64, u64)>, (x, component)| {
                // collect all changes
                delta.push((x, component));
            },
            |state, changes| {
                // apply all changes
                state.updated = state.updated || !changes.is_empty();
                for (x, component) in changes {
                    state.component[x as usize] = component;
                }
            },
            |state| {
                // stop if there were no changes
                let condition = state.updated;
                state.updated = false;
                state.iteration_count += 1;
                condition
            },
        );
    let result = result.collect_vec();
    dropme.for_each(std::mem::drop);

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
        true => connected_components_shared(config, opts),
        false => connected_components_join(config, opts),
    }?;

    Ok(())
}
