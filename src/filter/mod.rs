pub mod bloom;

/// Basic Filter trait, not constrained to bloom filters
pub trait Filter {
    fn insert(self: &mut Self, key: u64);

    fn contains(self: &Self, key: u64) -> bool;

    fn fp_rate(self: &Self) -> f64;
}
