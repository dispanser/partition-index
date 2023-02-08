use partition_index;

fn main() {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let chunks = partition_index::read_parquet(file_path);
    let _hashed = partition_index::hash_column(
        chunks.expect("this shouldn't have failed, we're a prototype"),
    );
    println!("Hello, world!");
}
