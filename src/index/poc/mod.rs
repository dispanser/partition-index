use crate::{
    filter::cuckoo::{bucket, fingerprint, flip_bucket},
    index::{PartitionFilter, PartitionIndex},
};

use super::in_memory::{CuckooIndex, PartitionInfo};
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    str::FromStr,
};

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
    data_root: PathBuf,
}

impl<P> PersistentIndex<P>
where
    P: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn try_new(buckets: u64, storage_root: String) -> anyhow::Result<Self> {
        let data_root: PathBuf = [&storage_root, "index"].iter().collect();
        Ok(Self {
            storage_root,
            data: PersistentIndexData {
                num_buckets: buckets,
                slots: 0,
                partitions: vec![],
            },
            mem_index: CuckooIndex::new(buckets),
            data_root,
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
        let data_root: PathBuf = [&storage_root, "index"].iter().collect();
        Ok(Self {
            storage_root,
            data,
            mem_index: CuckooIndex::new(num_buckets),
            data_root,
        })
    }

    pub fn persist(&mut self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.data_root)?;
        for (idx, bucket) in self.mem_index.buckets.iter().enumerate() {
            let mut file = fs::OpenOptions::new()
                .read(false)
                .create(true)
                .append(true)
                .open(&self.data_root.join(format!("{:07}.bucket", idx)))?;
            file.write_all(to_u8_slice(bucket))?;
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

    pub fn estimate_size(&self) -> usize {
        self.mem_index.partitions.len() * std::mem::size_of::<P>()
            + self.data.partitions.len() * std::mem::size_of::<P>()
            + self.mem_index.slots * self.data.num_buckets as usize * std::mem::size_of::<u16>()
    }

    pub fn estimate_disk_size(&self) -> usize {
        self.data.partitions.len() * std::mem::size_of::<P>()
            + self.data.slots * self.data.num_buckets as usize * std::mem::size_of::<u16>()
    }

    pub fn partitions(&self) -> impl Iterator<Item = P> + '_ {
        self.data
            .partitions
            .iter()
            .chain(self.mem_index.partitions.iter())
            .map(|pi| pi.partition.clone())
    }

    pub fn num_partitions(&self) -> usize {
        self.data.partitions.len() + self.mem_index.partitions.len()
    }

    fn load_bucket(&self, bucket: u64, buf: &mut Vec<u8>) -> anyhow::Result<()> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(&self.data_root.join(format!("{:07}.bucket", bucket)))?;
        file.read_to_end(buf)?;
        Ok(())
    }

    fn query_disk(&self, key: u64) -> anyhow::Result<Vec<P>> {
        if self.data.partitions.is_empty() {
            return Ok(vec![]);
        }
        let fingerprint = fingerprint(key);
        let bucket1 = bucket(key, self.data.num_buckets as u64);
        let bucket2 = flip_bucket(fingerprint, bucket1, self.data.num_buckets as u64);
        let mut b1_data = vec![];
        let mut b2_data = vec![];
        self.load_bucket(bucket1, &mut b1_data)?;
        self.load_bucket(bucket2, &mut b2_data)?;
        let b1_data_u16 = to_u16_slice(&b1_data);
        let b2_data_u16 = to_u16_slice(&b2_data);
        let mut pos = 0;
        let mut result = vec![];
        for p in &self.data.partitions {
            if p.active {
                for l in 0..p.entries {
                    if b1_data_u16[pos + l] == fingerprint || b2_data_u16[pos + l] == fingerprint {
                        result.push(p.partition.clone());
                    }
                }
            }
            pos += p.entries;
        }
        Ok(result)
    }
}

fn to_u8_slice(slice: &[u16]) -> &[u8] {
    let num_elems = 2 * slice.len();
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), num_elems) }
}

fn to_u16_slice(slice: &[u8]) -> &[u16] {
    let num_elems = slice.len() / 2;
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast::<u16>(), num_elems) }
}

impl<P> PartitionFilter<P> for PersistentIndex<P>
where
    P: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn query(&self, key: u64) -> anyhow::Result<Vec<P>> {
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
    fn add(&mut self, values: impl Iterator<Item = u64>, partition: P) {
        self.mem_index.add(values, partition)
    }

    fn remove(&mut self, to_be_removed: &P) {
        self.mem_index.remove(to_be_removed)
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn verify_persisted_bucket_sizes() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let temp_dir = tempfile::tempdir()?;
        let storage_root = temp_dir.path();
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(80, storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index, &partitions);
        let expected_file_length = 2 * index.mem_index.slots;
        index.persist()?;
        let data_root: PathBuf = storage_root.join("index");
        for f in data_root.read_dir()? {
            let f = f?;
            let metadata = f.metadata()?;
            assert!(metadata.is_file(), "index should only contain files");
            assert_eq!(
                metadata.st_size(),
                expected_file_length as u64,
                "index file '{:?}' must have length {}",
                f.file_name(),
                expected_file_length,
            );
        }
        Ok(())
    }

    #[test]
    fn deserialize_persisted_state() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let temp_dir = tempfile::tempdir()?;
        let storage_root = temp_dir.path();
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
        let partitions = &tests::create_test_data(3, (10, 20), SEED);
        let temp_dir = tempfile::tempdir()?;
        let storage_root = temp_dir.path();
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(8, storage_root.to_str().unwrap().to_string())?;
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

    // serve queries from a mixed of persisted and in-memory partitions
    #[test]
    fn serve_queries_mixed() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(3, (10, 20), SEED);
        let (first_half, second_half) = partitions.split_at(2);
        let temp_dir = tempfile::tempdir()?;
        let storage_root = temp_dir.path();
        let mut index: PersistentIndex<TestPartition> =
            PersistentIndex::try_new(8, storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index, first_half);
        index.persist()?;
        drop(index);
        let mut index_from_disk: PersistentIndex<TestPartition> =
            PersistentIndex::try_load_from_disk(storage_root.to_str().unwrap().to_string())?;
        tests::fill_index(&mut index_from_disk, second_half);
        // reload from disk and run query tests
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

    // serve queries from a mixed of persisted and in-memory partitions
    #[test]
    fn serve_queries_multiple_persist() -> anyhow::Result<()> {
        let partitions = &tests::create_test_data(10, (99, 499), SEED);
        let temp_dir = tempfile::tempdir()?;
        let storage_root = temp_dir.path().to_str().unwrap();
        let mut index = PersistentIndex::try_new(80, storage_root.to_string())?;
        for p in partitions {
            index.add(tests::create_partition_data(&p), p.clone());
            index.persist()?;
            index = PersistentIndex::try_load_from_disk(storage_root.to_string())?;
        }
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
}
