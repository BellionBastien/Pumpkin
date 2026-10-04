use std::sync::atomic::{AtomicU64, Ordering};

/// Upper bounds of the tick duration buckets in nanoseconds. 50 ms is one tick at 20 TPS.
const BUCKET_UPPER_BOUNDS_NANOS: [u64; 13] = [
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    20_000_000,
    30_000_000,
    40_000_000,
    50_000_000,
    75_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
];

/// Lock-free histogram of server tick durations, read by the metrics endpoint.
#[derive(Default)]
pub struct TickDurationHistogram {
    /// Observations per bucket, not cumulative. The last slot holds everything above the
    /// highest bound.
    buckets: [AtomicU64; BUCKET_UPPER_BOUNDS_NANOS.len() + 1],
    sum_nanos: AtomicU64,
}

impl TickDurationHistogram {
    /// Records one tick. A negative duration counts as zero.
    pub fn observe(&self, duration_nanos: i64) {
        let duration_nanos = u64::try_from(duration_nanos).unwrap_or_default();
        // Bounds are inclusive (`le`), so a duration equal to a bound belongs to that bucket.
        let bucket = BUCKET_UPPER_BOUNDS_NANOS.partition_point(|&bound| bound < duration_nanos);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(duration_nanos, Ordering::Relaxed);
    }

    /// Yields `(upper bound in nanoseconds, cumulative count)` for every bucket. The last
    /// item is the `+Inf` bucket, which has no bound and holds the total count.
    pub fn cumulative_buckets(&self) -> impl Iterator<Item = (Option<u64>, u64)> {
        let bounds = BUCKET_UPPER_BOUNDS_NANOS
            .iter()
            .copied()
            .map(Some)
            .chain(std::iter::once(None));
        bounds
            .zip(&self.buckets)
            .scan(0, |cumulative, (bound, count)| {
                *cumulative += count.load(Ordering::Relaxed);
                Some((bound, *cumulative))
            })
    }

    /// Sum of all observed durations in nanoseconds.
    pub fn sum_nanos(&self) -> u64 {
        self.sum_nanos.load(Ordering::Relaxed)
    }
}
