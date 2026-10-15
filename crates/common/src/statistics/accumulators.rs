//! Bounded mergeable statistics synopses.

use super::*;

mod collector;
pub use collector::StatisticsCollector;

struct NumericAccumulator {
    finite_count: u64,
    non_finite_count: u64,
    sum: Option<f64>,
    moments: Option<(f64, f64)>,
}

impl Default for NumericAccumulator {
    fn default() -> Self {
        Self { finite_count: 0, non_finite_count: 0, sum: Some(0.0), moments: Some((0.0, 0.0)) }
    }
}

impl NumericAccumulator {
    fn update(&mut self, value: f64) -> Result<()> {
        if !value.is_finite() {
            self.non_finite_count = self.non_finite_count.checked_add(1)
                .ok_or_else(|| VeinError::internal("numeric non-finite count overflow"))?;
            return Ok(());
        }
        let count = self.finite_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("numeric finite count overflow"))?;
        self.sum = self.sum.and_then(|sum| {
            let next = sum + value;
            next.is_finite().then_some(next)
        });
        if let Some((mean, m2)) = self.moments {
            let delta = value - mean;
            let next_mean = mean + delta / count as f64;
            let next_m2 = m2 + delta * (value - next_mean);
            self.moments = (next_mean.is_finite() && next_m2.is_finite() && next_m2 >= 0.0)
                .then_some((next_mean, next_m2));
        }
        self.finite_count = count;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        let finite_count = self.finite_count.checked_add(other.finite_count)
            .ok_or_else(|| VeinError::internal("numeric finite count overflow"))?;
        let non_finite_count = self.non_finite_count.checked_add(other.non_finite_count)
            .ok_or_else(|| VeinError::internal("numeric non-finite count overflow"))?;
        self.sum = self.sum.zip(other.sum).and_then(|(left, right)| {
            let sum = left + right;
            sum.is_finite().then_some(sum)
        });
        self.moments = match (self.moments, other.moments) {
            (Some(_), Some(right)) if self.finite_count == 0 => Some(right),
            (Some(left), Some(_)) if other.finite_count == 0 => Some(left),
            (Some((left_mean, left_m2)), Some((right_mean, right_m2))) => {
                let total = finite_count as f64;
                let delta = right_mean - left_mean;
                let mean = left_mean + delta * (other.finite_count as f64 / total);
                let m2 = left_m2 + right_m2
                    + delta * delta * (self.finite_count as f64 * other.finite_count as f64 / total);
                (mean.is_finite() && m2.is_finite() && m2 >= 0.0).then_some((mean, m2))
            }
            _ => None,
        };
        self.finite_count = finite_count;
        self.non_finite_count = non_finite_count;
        Ok(())
    }

    fn restore(summary: &NumericStatistics) -> Self {
        Self {
            finite_count: summary.finite_count,
            non_finite_count: summary.non_finite_count,
            sum: summary.sum,
            moments: if summary.finite_count == 0 {
                Some((0.0, 0.0))
            } else {
                summary.mean.zip(summary.m2)
            },
        }
    }

    fn finish(&self) -> NumericStatistics {
        NumericStatistics {
            finite_count: self.finite_count,
            non_finite_count: self.non_finite_count,
            sum: self.sum,
            mean: (self.finite_count > 0).then(|| self.moments.map(|(mean, _)| mean)).flatten(),
            m2: (self.finite_count > 0).then(|| self.moments.map(|(_, m2)| m2)).flatten(),
        }
    }
}

#[derive(Clone)]
struct ByteLengthAccumulator {
    value_count: u64,
    total_bytes: Option<u64>,
    min_bytes: Option<u64>,
    max_bytes: Option<u64>,
    contains_non_ascii: Option<bool>,
}

impl Default for ByteLengthAccumulator {
    fn default() -> Self {
        Self { value_count: 0, total_bytes: Some(0), min_bytes: None, max_bytes: None, contains_non_ascii: None }
    }
}

impl ByteLengthAccumulator {
    fn for_type(tracks_non_ascii: bool) -> Self {
        Self {
            contains_non_ascii: tracks_non_ascii.then_some(false),
            ..Self::default()
        }
    }
}

impl ByteLengthAccumulator {
    fn update(&mut self, length: u64, contains_non_ascii: Option<bool>) -> Result<()> {
        self.value_count = self.value_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("variable-length value count overflow"))?;
        self.total_bytes = self.total_bytes.and_then(|total| total.checked_add(length));
        self.min_bytes = Some(self.min_bytes.map_or(length, |current| current.min(length)));
        self.max_bytes = Some(self.max_bytes.map_or(length, |current| current.max(length)));
        self.contains_non_ascii = match (self.contains_non_ascii, contains_non_ascii) {
            (Some(previous), Some(current)) => Some(previous || current),
            _ => None,
        };
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.value_count = self.value_count.checked_add(other.value_count)
            .ok_or_else(|| VeinError::internal("variable-length value count overflow"))?;
        self.total_bytes = self.total_bytes.zip(other.total_bytes).and_then(|(left, right)| left.checked_add(right));
        self.min_bytes = match (self.min_bytes, other.min_bytes) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.max_bytes = match (self.max_bytes, other.max_bytes) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.contains_non_ascii = match (self.contains_non_ascii, other.contains_non_ascii) {
            (Some(left), Some(right)) => Some(left || right),
            _ => None,
        };
        Ok(())
    }

    fn restore(summary: &ByteLengthStatistics) -> Self {
        Self {
            value_count: summary.value_count,
            total_bytes: summary.total_bytes,
            min_bytes: summary.min_bytes,
            max_bytes: summary.max_bytes,
            contains_non_ascii: summary.contains_non_ascii,
        }
    }

    fn finish(&self) -> ByteLengthStatistics {
        ByteLengthStatistics {
            value_count: self.value_count,
            total_bytes: self.total_bytes,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            contains_non_ascii: self.contains_non_ascii,
        }
    }
}

#[derive(Clone)]
struct MostCommonAccumulator {
    summary: MostCommonValuesStatistics,
}

impl MostCommonAccumulator {
    fn new(capacity: usize) -> Self {
        Self {
            summary: MostCommonValuesStatistics {
                capacity,
                ..MostCommonValuesStatistics::default()
            },
        }
    }

    fn update<E: ArrowValueEncoder>(
        &mut self,
        encoder: &E,
        array: &dyn Array,
        index: usize,
        max_value_bytes: usize,
    ) -> Result<bool> {
        let Some(hash) = encoder.hash_value(array, index) else {
            return Ok(false);
        };
        let next_count = self.summary.analyzed_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("most-common analyzed count overflow"))?;
        let mut matching = None;
        for (candidate_index, candidate) in self.summary.candidates.iter().enumerate() {
            if candidate.hash != hash {
                continue;
            }
            match encoder.equals_bound(array, index, &candidate.value) {
                Some(true) => {
                    matching = Some(candidate_index);
                    break;
                }
                Some(false) => {}
                None => return Ok(false),
            }
        }
        self.summary.analyzed_count = next_count;

        if let Some(candidate_index) = matching {
            let candidate = &mut self.summary.candidates[candidate_index];
            candidate.lower_count = candidate.lower_count.checked_add(1)
                .ok_or_else(|| VeinError::internal("most-common candidate count overflow"))?;
            return Ok(true);
        }

        if self.summary.candidates.len() < self.summary.capacity {
            let Some(value) = encoder.bound_value(array, index, max_value_bytes) else {
                return Ok(false);
            };
            self.summary.candidates.push(MostCommonValue {
                value,
                hash,
                lower_count: 1,
            });
            return Ok(true);
        }

        self.summary.error_bound = self.summary.error_bound.checked_add(1)
            .ok_or_else(|| VeinError::internal("most-common error bound overflow"))?;
        self.summary.candidates.retain_mut(|candidate| {
            candidate.lower_count -= 1;
            candidate.lower_count > 0
        });
        Ok(true)
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.summary.capacity != other.summary.capacity {
            return Err(VeinError::internal("cannot merge most-common synopses with different capacities"));
        }
        self.summary.analyzed_count = self.summary.analyzed_count.checked_add(other.summary.analyzed_count)
            .ok_or_else(|| VeinError::internal("most-common analyzed count overflow"))?;
        self.summary.error_bound = self.summary.error_bound.checked_add(other.summary.error_bound)
            .ok_or_else(|| VeinError::internal("most-common error bound overflow"))?;

        for right in &other.summary.candidates {
            if let Some(left) = self.summary.candidates.iter_mut().find(|candidate| {
                candidate.hash == right.hash && literals_equal(&candidate.value, &right.value)
            }) {
                left.lower_count = left.lower_count.checked_add(right.lower_count)
                    .ok_or_else(|| VeinError::internal("most-common candidate count overflow"))?;
            } else {
                self.summary.candidates.push(right.clone());
            }
        }

        while self.summary.candidates.len() > self.summary.capacity {
            let decrement = self.summary.candidates.iter().map(|candidate| candidate.lower_count).min()
                .ok_or_else(|| VeinError::internal("empty most-common candidate reduction"))?;
            self.summary.error_bound = self.summary.error_bound.checked_add(decrement)
                .ok_or_else(|| VeinError::internal("most-common error bound overflow"))?;
            self.summary.candidates.retain_mut(|candidate| {
                candidate.lower_count -= decrement;
                candidate.lower_count > 0
            });
        }
        self.summary.candidates.sort_by(|left, right| right.lower_count.cmp(&left.lower_count));
        Ok(())
    }

    fn restore(summary: &MostCommonValuesStatistics, expected_capacity: usize) -> Result<Self> {
        if summary.capacity != expected_capacity
            || summary.candidates.len() > summary.capacity
            || summary.error_bound > summary.analyzed_count
            || summary.candidates.iter().any(|candidate| candidate.lower_count == 0)
        {
            return Err(VeinError::internal("invalid most-common synopsis state"));
        }
        Ok(Self { summary: summary.clone() })
    }
}

#[derive(Clone)]
struct PrioritySample(HistogramSample);

impl PartialEq for PrioritySample {
    fn eq(&self, other: &Self) -> bool {
        (self.0.priority, self.0.tie_breaker) == (other.0.priority, other.0.tie_breaker)
    }
}

impl Eq for PrioritySample {}

impl PartialOrd for PrioritySample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritySample {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.priority, self.0.tie_breaker)
            .cmp(&(other.0.priority, other.0.tie_breaker))
    }
}

#[derive(Clone)]
struct HistogramAccumulator {
    population_count: u64,
    sample_capacity: usize,
    bucket_count: usize,
    confidence_ppm: u32,
    samples: BinaryHeap<PrioritySample>,
}

impl HistogramAccumulator {
    fn new(sample_capacity: usize, bucket_count: usize, confidence_ppm: u32) -> Self {
        Self {
            population_count: 0,
            sample_capacity,
            bucket_count,
            confidence_ppm,
            samples: BinaryHeap::with_capacity(sample_capacity),
        }
    }

    fn update(&mut self, value: impl FnOnce() -> Option<LogicalLiteral>) -> Result<bool> {
        self.population_count = self.population_count.checked_add(1)
            .ok_or_else(|| VeinError::internal("histogram population count overflow"))?;
        if self.sample_capacity == 0 {
            return Ok(false);
        }
        let priority = rand::random();
        let tie_breaker = rand::random();
        let sample_key = (priority, tie_breaker);
        if self.samples.len() < self.sample_capacity {
            let Some(value) = value() else { return Ok(false) };
            self.samples.push(PrioritySample(HistogramSample {
                priority,
                tie_breaker,
                value,
            }));
        } else if self.samples.peek().is_some_and(|largest| {
            sample_key < (largest.0.priority, largest.0.tie_breaker)
        }) {
            let Some(value) = value() else { return Ok(false) };
            self.samples.pop();
            self.samples.push(PrioritySample(HistogramSample {
                priority,
                tie_breaker,
                value,
            }));
        }
        Ok(true)
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.sample_capacity != other.sample_capacity
            || self.bucket_count != other.bucket_count
            || self.confidence_ppm != other.confidence_ppm
        {
            return Err(VeinError::internal("cannot merge histograms with different configurations"));
        }
        self.population_count = self.population_count.checked_add(other.population_count)
            .ok_or_else(|| VeinError::internal("histogram population count overflow"))?;
        for sample in &other.samples {
            if self.samples.len() < self.sample_capacity {
                self.samples.push(sample.clone());
            } else if self.samples.peek().is_some_and(|largest| sample < largest) {
                self.samples.pop();
                self.samples.push(sample.clone());
            }
        }
        Ok(())
    }

    fn restore(summary: &HistogramStatistics, config: &StatisticsConfig) -> Result<Self> {
        if summary.sample_capacity != config.histogram_sample_size
            || summary.bucket_count != config.histogram_bucket_count
            || summary.confidence_ppm != config.histogram_confidence_ppm
            || summary.samples.len() > summary.sample_capacity
            || summary.sample_count != summary.samples.len() as u64
            || summary.sample_count > summary.population_count
        {
            return Err(VeinError::internal("invalid or incompatible histogram synopsis state"));
        }
        let mut accumulator = Self::new(
            summary.sample_capacity,
            summary.bucket_count,
            summary.confidence_ppm,
        );
        accumulator.population_count = summary.population_count;
        accumulator.samples.extend(summary.samples.iter().cloned().map(PrioritySample));
        Ok(accumulator)
    }

    fn finish(self) -> Option<HistogramStatistics> {
        let mut samples = self.samples.into_vec();
        samples.sort_by(|left, right| {
            left.0.value.try_cmp(&right.0.value).unwrap_or(Ordering::Equal)
        });
        if samples.windows(2).any(|pair| pair[0].0.value.try_cmp(&pair[1].0.value).is_none()) {
            return None;
        }

        let sample_count = samples.len();
        if self.population_count > 0 && (sample_count == 0 || self.bucket_count == 0) {
            return None;
        }
        let bucket_count = self.bucket_count.min(sample_count);
        let mut buckets = Vec::with_capacity(bucket_count);
        for bucket_index in 0..bucket_count {
            let start = bucket_index * sample_count / bucket_count;
            let end = (bucket_index + 1) * sample_count / bucket_count;
            let estimate_at = |sample_index: usize| -> u64 {
                ((self.population_count as u128 * sample_index as u128) / sample_count as u128) as u64
            };
            let estimated_count = estimate_at(end).saturating_sub(estimate_at(start));
            buckets.push(HistogramBucket {
                lower_bound: samples[start].0.value.clone(),
                upper_bound: samples[end - 1].0.value.clone(),
                sample_count: (end - start) as u64,
                estimated_count,
            });
        }
        let rank_error_bound = (sample_count > 0).then(|| {
            let confidence = f64::from(self.confidence_ppm) / 1_000_000.0;
            ((2.0 / (1.0 - confidence)).ln() / (2.0 * sample_count as f64)).sqrt().min(1.0)
        });
        Some(HistogramStatistics {
            population_count: self.population_count,
            sample_count: sample_count as u64,
            sample_capacity: self.sample_capacity,
            bucket_count: self.bucket_count,
            confidence_ppm: self.confidence_ppm,
            samples: samples.into_iter().map(|sample| sample.0).collect(),
            buckets,
            rank_error_bound,
        })
    }
}

fn literals_equal(left: &LogicalLiteral, right: &LogicalLiteral) -> bool {
    match (left, right) {
        (LogicalLiteral::Float64(left), LogicalLiteral::Float64(right)) => {
            canonical_float(*left) == canonical_float(*right)
        }
        _ => left == right,
    }
}

