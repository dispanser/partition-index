pub mod cuckoo;

/// The underlying assumption here is that we're indexing "partitions"
/// on a single, unknown column. The only representation we can retrieve
/// is a hashed representation of all values to be indexed.
/// This allows some flexibility down the road by allowing different types
/// of partitions (parquet files, row groups, hive partitions, ...) or even
/// heterogeneous data that has different shape or form.

/// TODO:
/// - how do we actually "reference" a partition that we identified?
///   - is there some sort of serializable representation we can store?
///   - do we just return an (increasing) number (our index / offset)?
///     - this would enable us to solve the problem elsewhere
/// - can we support indexing multiple columns?
///   - it feels wasteful to register a single partition multiple times
///   - metadata overlaps, and using the same offset in all indexes would be great
/// - we could think of the partition itself as just some identifier of theaactual
///   partition entity, and do `add(values: Iterator<Item = u64>, i: ID); in the Index
///   - I like that: we decouple the knowledge about structure, type, ... of partitions
///   - we pass in the minimum necessary thing to do its job.
/// - in terms of API design, can we split the API into multiple traits?
///   - `PartitionLookup`
///   - `PartitionIndexer`
///   - ... (delete) ...
///   - It may make sense to have higher-level constructs that manage mutations
///     - by maintaining a WAL + batching writes in memory
///     - tombstone handling
///     - they could even manage multiple Lookup sets
///     - e.g. a partition key (as in, hive partitioning) to split into multiple indexes

/// A trait that allows querying a data set for matching partitions.
/// TODO: P should probably be somehow serializable
pub trait PartitionFilter<P> {
    /// Query matching partitions for a given value
    /// TODO: what's the result type here? It's either the partition, or
    /// some kind of partition ID.
    fn query(value: u64) -> dyn Iterator<Item = P>;
}

pub trait PartitionIndex<P> {
    /// Add a partition to the index.
    /// @param values an iterator of the values stored in the partition
    /// @param partition the partition identifier to associate the values with
    fn add(values: &dyn Iterator<Item = u64>, partition: P);

    /// Remove a partition from the index.
    /// @param partition to remove
    fn remove(partition: P);
}
