pub mod filter;
pub mod index;

use arrow2::array;
use arrow2::error::Error;
use arrow2::io::parquet::read;

use std::collections::HashSet;
use std::fs::File;

pub fn read_parquet(file_path: &String) -> Result<read::FileReader<File>, Error> {
    // say we have a file
    let mut reader = File::open(file_path)?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;

    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|_index, _field| true);

    // we can read the statistics of all parquet's row groups (here for each field)
    for field in schema.fields.iter().enumerate() {
        let statistics = read::statistics::deserialize(field.1, &metadata.row_groups);
        println!(
            "[{}] ** {}: {:?} -> {:#?}",
            field.0,
            field.1.name,
            field.1.data_type(),
            statistics
        );
    }

    // say we found that we only need to read the first two row groups, "0" and "1"
    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == 0 || *index == 1)
        .map(|(_, row_group)| row_group)
        .collect();

    Ok(read::FileReader::new(
        reader,
        row_groups,
        schema,
        Some(1024 * 8 * 8),
        None,
        None,
    ))
}

pub fn hash_column(chunks: read::FileReader<File>) -> Result<(), Error> {
    let mut rows = 0;
    let mut elems = 0;
    let mut distinct: HashSet<i32> = HashSet::new();
    for maybe_chunk in chunks.take(3) {
        let chunk = maybe_chunk?;
        let array = &chunk.columns()[19]
            .as_any()
            .downcast_ref::<array::Int32Array>()
            .unwrap();
        rows += array.len();
        for maybe_value in array.iter() {
            if let Some(value) = maybe_value {
                // let bb: u64 = hash.into();
                elems += 1;
                distinct.insert(*value);
            }
        }
        assert!(!chunk.is_empty());
    }
    Ok(())
}
