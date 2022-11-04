pub mod bloom;
pub mod cuckoo;

/// Basic Filter trait, not constrained to bloom filters
pub trait Filter {
    fn insert(self: &mut Self, key: u64);

    fn contains(self: &Self, key: u64) -> bool;
}
