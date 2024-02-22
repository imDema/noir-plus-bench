cargo build --release --bin enrich-pool &&\
 hyperfine './target/release/enrich-pool {conf} -- -n 100000 {v}'\
 -w 2 -L v -s, -L conf '-l8','-r noir-4.yml' --export-json "results/$(date -uIseconds)-enrich-pool.json"

cargo build --release --bin connected &&\
 hyperfine './target/release/connected {conf} -- -i 1000 -n ~/data/connected-components/nodes.txt -e ~/data/connected-components/edges.txt -N 200000 {v}'\
 -w 2 -L v -s, -L conf '-l8','-r noir-1.yml','-r noir-2.yml','-r noir-3.yml','-r noir-4.yml' --export-json "results/$(date -uIseconds)-connected.json"

cargo build --release --bin pagerank &&\
 hyperfine './target/release/pagerank {conf} -- -i 1000 -n ~/data/pagerank/nodes.txt -e ~/data/pagerank/edges.txt -N 81306 {v}'\
 -w 2 -L v -s, -L conf '-l8','-r noir-1.yml','-r noir-2.yml','-r noir-3.yml','-r noir-4.yml' --export-json "results/$(date -uIseconds)-pagerank.json"

cargo build --bin enrich-async --features async --release &&\
 hyperfine './target/release/enrich-async {conf} -- -n 100000 {v}'\
 -w 2 -L v -s, -L conf '-l8','-r noir-1.yml','-r noir-2.yml','-r noir-3.yml','-r noir-4.yml' --export-json "results/$(date -uIseconds)-enrich-async.json"

cargo build --bin enrich-memo --features async --release &&\
 hyperfine './target/release/enrich-memo {conf} -- -n 10000000 -m {memo}'\
 -w 1 -r 5 -L conf '-l8','-r noir-1.yml','-r noir-2.yml','-r noir-3.yml','-r noir-4.yml' -L memo 0,1024,1048576,16384,4096,65536,262144,256 --export-json "results/$(date -uIseconds)-enrich-memo.json"

cargo build --release --bin unique &&\
 hyperfine './target/release/unique {conf} -- -n 100000000 -l 200000 {v}'\
 -w 2 -L v -s, -L conf '-l8','-r noir-1.yml','-r noir-2.yml','-r noir-3.yml','-r noir-4.yml' --export-json "results/$(date -uIseconds)-unique.json"
