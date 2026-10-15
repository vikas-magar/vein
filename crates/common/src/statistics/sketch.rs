//! Mergeable distinct-value sketch implementations.

use super::*;

/// Estimate and merge contract for bounded distinct-value synopses.
///
/// Implementations can replace HyperLogLog without changing the batch
/// collector or catalog-facing statistics model.
pub trait MergeableDistinctSketch: Clone + Send + Sync + 'static {
    /// Construct a sketch from the validated collector configuration.
    fn with_config(config: &StatisticsConfig) -> Result<Self>;
    /// Add a stable, type-tagged value hash.
    fn update_hash(&mut self, hash: u64);
    /// Merge another compatible sketch.
    fn merge(&mut self, other: &Self) -> Result<()>;
    /// Estimated number of distinct values.
    fn estimate(&self) -> Option<u64>;
    /// Relative standard error for the estimate, if known.
    fn relative_error(&self) -> Option<f64>;
    /// Serialize the bounded state into the format-neutral catalog envelope.
    fn to_serialized(&self) -> SerializedSketch;
    /// Restore state from a catalog envelope.
    fn from_serialized(state: &SerializedSketch) -> Result<Self>;
}

/// HyperLogLog distinct-value sketch with a fixed, persisted hash function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    const ALGORITHM: &'static str = "hyperloglog";
    const VERSION: u16 = 1;

    pub(super) fn new(precision: u8) -> Result<Self> {
        if !(4..=16).contains(&precision) {
            return Err(VeinError::internal(format!(
                "distinct sketch precision must be between 4 and 16, got {precision}"
            )));
        }
        Ok(Self {
            precision,
            registers: vec![0; 1_usize << precision],
        })
    }

    fn validate_state(state: &SerializedSketch) -> Result<(u8, &[u8])> {
        if state.algorithm != Self::ALGORITHM || state.version != Self::VERSION {
            return Err(VeinError::internal(format!(
                "unsupported distinct sketch {} version {}",
                state.algorithm, state.version
            )));
        }
        let [precision] = state.parameters.as_slice() else {
            return Err(VeinError::internal("invalid HyperLogLog parameters"));
        };
        if !(4..=16).contains(precision) || state.payload.len() != (1_usize << precision) {
            return Err(VeinError::internal("invalid HyperLogLog register state"));
        }
        if state.payload.iter().any(|register| *register > 64) {
            return Err(VeinError::internal("HyperLogLog register exceeds hash width"));
        }
        Ok((*precision, &state.payload))
    }
}

impl MergeableDistinctSketch for HyperLogLog {
    fn with_config(config: &StatisticsConfig) -> Result<Self> {
        Self::new(config.distinct_precision)
    }

    fn update_hash(&mut self, hash: u64) {
        let index_bits = u32::from(self.precision);
        let index = (hash as usize) & (self.registers.len() - 1);
        let remaining = hash >> index_bits;
        let rank = remaining
            .leading_zeros()
            .saturating_sub(index_bits)
            .saturating_add(1)
            .min(64 - index_bits + 1) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.precision != other.precision || self.registers.len() != other.registers.len() {
            return Err(VeinError::internal(
                "cannot merge HyperLogLog sketches with different precision",
            ));
        }
        for (left, right) in self.registers.iter_mut().zip(&other.registers) {
            *left = (*left).max(*right);
        }
        Ok(())
    }

    fn estimate(&self) -> Option<u64> {
        let m = self.registers.len() as f64;
        if m == 0.0 {
            return Some(0);
        }
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let denominator = self
            .registers
            .iter()
            .map(|register| 2.0_f64.powi(-i32::from(*register)))
            .sum::<f64>();
        if denominator <= 0.0 || !denominator.is_finite() {
            return None;
        }
        let raw = alpha * m * m / denominator;
        let zero_registers = self.registers.iter().filter(|value| **value == 0).count();
        let estimate = if raw <= 2.5 * m && zero_registers > 0 {
            m * (m / zero_registers as f64).ln()
        } else {
            raw
        };
        (estimate.is_finite() && estimate >= 0.0)
            .then(|| estimate.round().min(u64::MAX as f64) as u64)
    }

    fn relative_error(&self) -> Option<f64> {
        Some(1.04 / (self.registers.len() as f64).sqrt())
    }

    fn to_serialized(&self) -> SerializedSketch {
        SerializedSketch {
            algorithm: Self::ALGORITHM.to_owned(),
            version: Self::VERSION,
            parameters: vec![self.precision],
            payload: self.registers.clone(),
        }
    }

    fn from_serialized(state: &SerializedSketch) -> Result<Self> {
        let (precision, registers) = Self::validate_state(state)?;
        Ok(Self {
            precision,
            registers: registers.to_vec(),
        })
    }
}
