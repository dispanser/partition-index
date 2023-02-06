use partition_index::{
    self, benchmarks::BenchmarkPartition, index::poc::PersistentIndex, index::PartitionFilter,
};
use rand::{distributions::Uniform, Rng, SeedableRng};
use std::{sync::Arc, time::SystemTime};

async fn run_query(index: Arc<PersistentIndex<BenchmarkPartition>>, i: u64) -> (u64, u64) {
    let query_result = index.query(i).await.unwrap_or(vec![]);
    if query_result
        .iter()
        .any(|p| i >= p.start && i < p.start + p.length)
    {
        (query_result.len() as u64 - 1, 0)
    } else {
        (query_result.len() as u64, 1)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let num_queries: u64 = args[2].parse()?;

    let index = Arc::new(PersistentIndex::<BenchmarkPartition>::try_load_from_disk(
        file_path.to_string(),
    )?);
    let max_value = index
        .partitions()
        .next()
        .expect("invalid: empty index")
        .elements()
        * index.num_partitions() as u64;
    let value_distribution = Uniform::new(0, max_value);
    let mut data_rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(1337);

    let start_querying = SystemTime::now();
    let query_results: Vec<_> = (0..num_queries)
        // .map(|_| run_query(&index, data_rng.sample(value_distribution)))
        .map(|_| {
            let index_ = Arc::clone(&index);
            tokio::spawn(run_query(index_, data_rng.sample(value_distribution)))
        })
        .collect();
    let results: Vec<_> = futures::future::join_all(query_results)
        .await
        .into_iter()
        .map(|res| res.unwrap_or((0, 1)))
        .collect();
    let (false_positives, false_negatives): (Vec<u64>, Vec<u64>) = itertools::multiunzip(results);
    let false_negatives: u64 = false_negatives.iter().sum();
    let false_positives: u64 = false_positives.iter().sum();

    let query_duration = start_querying.elapsed()?;
    let fp_rate =
        false_positives as f64 / (num_queries as u128 * index.num_partitions() as u128) as f64;
    eprintln!(
        "tp;correctness: {} false positives, {} false negatives, fp-rate {}",
        false_positives, false_negatives, fp_rate,
    );
    eprintln!(
        "tp;bench query: queried {} elems in {:?} ({:?} ops)",
        num_queries,
        query_duration,
        num_queries as u128 * 1000 / query_duration.as_millis()
    );
    Ok(())
}
