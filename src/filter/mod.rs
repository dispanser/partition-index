pub mod bloom;
pub mod cuckoo;

#[derive(PartialEq, Debug)]
pub enum InsertResult {
    Success,
    Duplicate,
    Rejected,
}

/// Basic Filter trait, not constrained to bloom filters
pub trait Filter {
    fn insert(self: &mut Self, key: u64) -> InsertResult;

    fn contains(self: &Self, key: u64) -> bool;
}

#[cfg(test)]
pub mod correctness_tests {
    use std::ops::Range;

    use super::Filter;

    pub fn fill_from_range(filter: &mut dyn Filter, inputs: Range<u64>) {
        inputs.for_each(|key| {
            filter.insert(key);
        });
    }

    pub fn check_false_negatives(filter: &mut dyn Filter, expected_inputs: Range<u64>) {
        expected_inputs
            .into_iter()
            .for_each(|key| assert!(filter.contains(key), "filter does not contain {}", key));
    }

    /// estimate the false positive rate based on a range that is not part of the filter
    pub fn estimate_false_positive_rate(filter: &mut dyn Filter, missing: Range<u64>) -> f64 {
        let mut pos = 0;
        let mut num = 0;
        for m in missing {
            num += 1;
            if filter.contains(m) {
                pos += 1;
            }
        }
        pos as f64 / num as f64
    }
}
