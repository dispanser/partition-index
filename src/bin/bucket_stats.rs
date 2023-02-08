use std::{fs, io::Read, os::unix::prelude::MetadataExt, path::PathBuf, str::FromStr};

// We're trying to find out why our Cuckoo Index has a false positive
// rate that is 10x above the expected value:
// - 10k partitions
// - 10k elements per partition
// - 4k buckets
// - => 3 entries per bucket
// - => (2*3) / 2^16 ~~~ 0.01% expected fp rate
// !! 0.125711% actual fp rate
//   - number seems very stable: increasing sample rate from 10k to 100k
//     changes fourth significant digit
// !! looking at a single bucket, distribution is far from the uniform:
//   - [179, 317, 0, 123, 0, 293, 141, 295, 0, 132, 0, 304, 176, 314, 0]
//   - I see a pattern: [... zero, high, low, high, zero, low, zero, ...]
//   - it's not _that_ simple, but it's clear that not every fingerprint
//     goes into every bucket so the probability of finding a fingerprint
//     in a bucket is far higher than it should be
//   - in this case, there should be ~ 11 mio values in a bucket,
//     so the average count per fingerprint should roughly be 150
fn fingerprint_distribution(index_root: &PathBuf) -> anyhow::Result<()> {
    let mut fingerprints = [0u32; 1 << 16];
    for bucket_file in index_root.read_dir()? {
        let bucket_file = bucket_file?;
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .open(bucket_file.path())?;
        let mut buf: Vec<u8> = vec![];
        buf.reserve_exact(bucket_file.metadata()?.size() as usize / 2);
        f.read_to_end(&mut buf)?;
        let num_elems = buf.len() / 2;
        let as_u16 = unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<u16>(), num_elems) };
        for fp in as_u16 {
            fingerprints[*fp as usize] += 1;
        }
        break;
    }
    eprintln!(
        "tp;fingerprint distribution:\n{:?}\nrange: [{}, {}]",
        &fingerprints[0..64],
        fingerprints[1..].iter().min().unwrap(),
        fingerprints[1..].iter().max().unwrap(),
    );
    // for fingerprint in fingerprints
    //     .iter()
    //     .enumerate()
    //     .filter(|(_, fp)| **fp != 0u32)
    // {
    //     eprintln!("tp;fingerprint[{}]: {}", fingerprint.0, fingerprint.1);
    // }
    Ok(())
}

/// collect some
fn main() -> anyhow::Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let index_root = PathBuf::from_str(&args[1])?.join("index");
    fingerprint_distribution(&index_root)?;
    Ok(())
}
