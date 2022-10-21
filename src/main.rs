use bloom_lake;

fn main() {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let chunks = bloom_lake::read_parquet(file_path);
    let _hashed =
        bloom_lake::hash_column(chunks.expect("this shouldn't have failed, we're a prototype"));
    println!("Hello, world!");
}
