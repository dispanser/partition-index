// Simple partition that has a start value and a size.
// It covers the values in range [start, start + length).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BenchmarkPartition {
    pub start: u64,
    pub length: u64,
}

impl BenchmarkPartition {
    pub fn elements(&self) -> u64 {
        self.start * self.length
    }
}
