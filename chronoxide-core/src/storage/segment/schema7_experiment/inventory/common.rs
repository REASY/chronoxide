use std::io;

use super::super::super::invalid_segment_data;
use super::super::{ExperimentalHistogramBucket, ExperimentalPowerOfTwoHistogram};

pub(super) struct PowerOfTwoHistogramAccumulator {
    zero_count: u64,
    buckets: [u64; 64],
}

impl Default for PowerOfTwoHistogramAccumulator {
    fn default() -> Self {
        Self {
            zero_count: 0,
            buckets: [0; 64],
        }
    }
}

impl PowerOfTwoHistogramAccumulator {
    pub(super) fn observe(&mut self, value: u64) -> io::Result<()> {
        let slot = if value == 0 {
            &mut self.zero_count
        } else {
            let exponent = 63usize - value.leading_zeros() as usize;
            &mut self.buckets[exponent]
        };
        *slot = slot
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("histogram count overflows"))?;
        Ok(())
    }

    pub(super) fn finish(self) -> ExperimentalPowerOfTwoHistogram {
        let buckets = self
            .buckets
            .into_iter()
            .enumerate()
            .filter_map(|(exponent, count)| {
                if count == 0 {
                    return None;
                }
                let lower_inclusive = 1u64 << exponent;
                let upper_inclusive = if exponent == 63 {
                    u64::MAX
                } else {
                    (1u64 << (exponent + 1)) - 1
                };
                Some(ExperimentalHistogramBucket {
                    lower_inclusive,
                    upper_inclusive,
                    count,
                })
            })
            .collect();
        ExperimentalPowerOfTwoHistogram {
            zero_count: self.zero_count,
            buckets,
        }
    }
}
