use crate::{
    filter::cuckoo::{bucket, fingerprint, flip_bucket},
    index::{PartitionFilter, PartitionIndex},
};

use super::in_memory::{CuckooIndex, PartitionInfo};
use std::{fs, path::PathBuf, str::FromStr};

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersistentIndexData<P> {
    num_buckets: u64,
    slots: usize,
    partitions: Vec<PartitionInfo<P>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PersistentIndex<P> {
    storage_root: String,
    data: PersistentIndexData<P>,
    mem_index: CuckooIndex<P>,
}

impl<P> PersistentIndex<P>
where
    P: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn try_new(buckets: u64, storage_root: String) -> anyhow::Result<Self> {
        Ok(Self {
            storage_root,
            data: PersistentIndexData {
                num_buckets: buckets,
                slots: 0,
                partitions: vec![],
            },
            mem_index: CuckooIndex::new(buckets),
        })
    }

    pub fn try_load_from_disk(storage_root: String) -> anyhow::Result<Self> {
        // 1. figure out how to store the parts we're interested in on disk,
        //    while keeping the rest (In-Memory bits) out of serialization
        //    idea: have a sub-struct that constitutes the "persistent" bits, and
        //    one that constitutes the ephemeral bits (in_memory::CuckooIndex)
        let file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(PathBuf::from_str(&storage_root)?.join("partitions.data"))?;
        let data: PersistentIndexData<P> = bincode::deserialize_from(file)?;
        let num_buckets = data.num_buckets;
        Ok(Self {
            storage_root,
            data,
            mem_index: CuckooIndex::new(num_buckets),
        })
    }

    pub fn persist(self: &mut Self) -> anyhow::Result<()> {
        let data_root: PathBuf = [&self.storage_root, "index"].iter().collect();
        fs::create_dir_all(&data_root)?;
        for (idx, bucket) in self.mem_index.buckets.iter().enumerate() {
            let file = fs::OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .open(&data_root.join(format!("{:07}.bucket", idx)))?;
            bincode::serialize_into(file, bucket)?;
        }
        self.data.partitions.append(&mut self.mem_index.partitions);
        self.data.slots = self.mem_index.slots;
        self.mem_index = CuckooIndex::new(self.data.num_buckets);
        let file = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(PathBuf::from_str(&self.storage_root)?.join("partitions.data"))?;
        bincode::serialize_into(file, &self.data)?;
        Ok(())
    }

    fn load_bucket(self: &Self, data_root: &PathBuf, bucket: u64) -> anyhow::Result<Vec<u16>> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(&data_root.join(format!("{:07}.bucket", bucket)))?;
        eprintln!("tp;load_bucket from {:?}", file);
        let bucket = bincode::deserialize_from(file)?;
        Ok(bucket)
    }

    fn query_disk(self: &Self, key: u64) -> anyhow::Result<Vec<P>> {
        if self.data.partitions.is_empty() {
            eprintln!("tp;query_disk: no persisted partition data, short-circuiting");
            return Ok(vec![]);
        }
        let fingerprint = fingerprint(key);
        let bucket1 = bucket(key, self.data.num_buckets as u64);
        let bucket2 = flip_bucket(fingerprint, bucket1, self.data.num_buckets as u64);
        let data_root: PathBuf = [&self.storage_root, "index"].iter().collect();
        let b1_data = self.load_bucket(&data_root, bucket1)?;
        let b2_data = self.load_bucket(&data_root, bucket2)?;
        let mut pos = 0;
        let mut result = vec![];
        eprintln!(
            "tp;query_disk({}): {}@[{}, {}]",
            key, fingerprint, bucket1, bucket2
        );
        for p in &self.data.partitions {
            if p.active {
                for l in 0..p.entries {
                    if b1_data[pos + l] == fingerprint || b2_data[pos + l] == fingerprint {
                        result.push(p.partition.clone());
                    }
                }
            }
            pos += p.entries;
        }
        Ok(result)
    }
}

impl<P> PartitionFilter<P> for PersistentIndex<P>
where
    P: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn query(self: &Self, key: u64) -> anyhow::Result<Vec<P>> {
        let mut disk_results = self.query_disk(key)?;
        let mut mem_results = self.mem_index.query(key)?;
        disk_results.append(&mut mem_results);
        Ok(disk_results)
    }
}

impl<P> PartitionIndex<P> for PersistentIndex<P>
where
    P: PartialEq,
{
    fn add(self: &mut Self, values: impl Iterator<Item = u64>, partition: P) {
        self.mem_index.add(values, partition)
    }

    fn remove(self: &mut Self, to_be_removed: &P) {
        self.mem_index.remove(to_be_removed)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::{os::linux::fs::MetadataExt, path::PathBuf};

    use super::PersistentIndex;
    use crate::index::{
        tests::{self, TestPartition},
        PartitionFilter, PartitionIndex,
    };

    static SEED: u64 = 1337;

    #[test]
    fn query_in_memory_index() -> anyhow::Result<()> {
        // let partitions = &tests::create_test_data(10, (5, 17), SEED)[9..];
        let partitions = &tests::create_test_data(100, (999, 4999), SEED);
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(800, "".to_string())?;
        tests::fill_index(&mut index, &partitions);

        for p in partitions {
            if let Some(first_val) = tests::create_partition_data(&p).next() {
                assert!(
                    index.query(first_val)?.contains(&p),
                    "querying partitions for '{}' does not yield expected {:?}",
                    first_val,
                    &p.id
                );
            } else {
                panic!("could not create value for partition");
            }
        }
        Ok(())
    }

    #[test]
    fn dont_yield_removed_partitions() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(80, "".to_string())?;
        tests::fill_index(&mut index, &partitions);
        index.remove(&partitions[3]);
        if let Some(first_val) = tests::create_partition_data(&partitions[3]).next() {
            assert!(
                !index.query(first_val)?.contains(&partitions[3]),
                "querying partitions for '{}' should not yield deleted partition {:?}",
                first_val,
                &partitions[3].id
            );
        }
        Ok(())
    }

    #[ignore = "format changed, bucket size no longer stable"]
    #[test]
    fn verify_persisted_bucket_sizes() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let storage_root = std::env::temp_dir();
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(80, storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index, &partitions);
        index.persist()?;

        let expecetd_file_length = 2 * index.mem_index.slots + 8;
        let data_root: PathBuf = storage_root.join("index");
        for f in data_root.read_dir()? {
            let f = f?;
            let metadata = f.metadata()?;
            assert!(metadata.is_file(), "index should only contain files");
            assert_eq!(
                metadata.st_size(),
                expecetd_file_length as u64,
                "index file '{:?}' must have length {}",
                f.file_name(),
                expecetd_file_length
            );
        }
        Ok(())
    }

    #[test]
    fn deserialize_persisted_state() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let storage_root = std::env::temp_dir();
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(80, storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index, &partitions);
        index.persist()?;
        // reload from disk and run query tests
        let index_from_disk: PersistentIndex<TestPartition> =
            PersistentIndex::try_load_from_disk(storage_root.to_str().unwrap().to_string())?;
        assert_eq!(index.data, index_from_disk.data);
        Ok(())
    }

    #[test]
    fn serve_queries_from_disk() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let storage_root = PathBuf::from_str("/tmp/partition_index")?;
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(80, storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index, &partitions);
        index.persist()?;
        // reload from disk and run query tests
        let index_from_disk: PersistentIndex<TestPartition> =
            PersistentIndex::try_load_from_disk(storage_root.to_str().unwrap().to_string())?;
        drop(index);
        for p in partitions {
            if let Some(first_val) = tests::create_partition_data(&p).next() {
                assert!(
                    index_from_disk.query(first_val)?.contains(&p),
                    "querying partitions for '{}' does not yield expected {:?}",
                    first_val,
                    &p.id
                );
            } else {
                panic!("could not create value for partition");
            }
        }
        Ok(())
    }
}
