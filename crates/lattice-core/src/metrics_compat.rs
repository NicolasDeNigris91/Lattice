//! Metric recording, conditionally compiled on the `metrics` feature.
//!
//! INVARIANT: every public function in this module has the same
//! signature in both the with-feature and without-feature
//! variants, so call sites do not need their own `cfg` guards.
//! Without the feature the functions are `const fn`s with empty
//! bodies; the optimiser deletes them entirely.

#[cfg(feature = "metrics")]
mod inner {
    use std::time::Duration;

    /// Counter: number of successful `put` calls (durable + non-durable).
    pub(crate) fn record_put(duration: Duration) {
        metrics::counter!("lattice_puts_total").increment(1);
        metrics::histogram!("lattice_put_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: number of `delete` calls.
    pub(crate) fn record_delete(duration: Duration) {
        metrics::counter!("lattice_deletes_total").increment(1);
        metrics::histogram!("lattice_delete_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: number of `get` calls and the hit/miss split.
    pub(crate) fn record_get(duration: Duration, hit: bool) {
        metrics::counter!("lattice_gets_total").increment(1);
        if hit {
            metrics::counter!("lattice_get_hits_total").increment(1);
        } else {
            metrics::counter!("lattice_get_misses_total").increment(1);
        }
        metrics::histogram!("lattice_get_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: number of memtable flushes that produced an `SSTable`.
    pub(crate) fn record_flush(duration: Duration) {
        metrics::counter!("lattice_flushes_total").increment(1);
        metrics::histogram!("lattice_flush_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: number of compaction rounds (one cascade level per round).
    pub(crate) fn record_compaction(duration: Duration) {
        metrics::counter!("lattice_compactions_total").increment(1);
        metrics::histogram!("lattice_compaction_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: transaction commits (only the `Ok` path increments).
    pub(crate) fn record_transaction_commit(duration: Duration) {
        metrics::counter!("lattice_transaction_commits_total").increment(1);
        metrics::histogram!("lattice_transaction_duration_seconds").record(duration.as_secs_f64());
    }

    /// Counter: transaction conflicts detected at commit time.
    pub(crate) fn record_transaction_conflict() {
        metrics::counter!("lattice_transaction_conflicts_total").increment(1);
    }
}

#[cfg(not(feature = "metrics"))]
mod inner {
    use std::time::Duration;

    pub(crate) const fn record_put(_d: Duration) {}
    pub(crate) const fn record_delete(_d: Duration) {}
    pub(crate) const fn record_get(_d: Duration, _hit: bool) {}
    pub(crate) const fn record_flush(_d: Duration) {}
    pub(crate) const fn record_compaction(_d: Duration) {}
    pub(crate) const fn record_transaction_commit(_d: Duration) {}
    pub(crate) const fn record_transaction_conflict() {}
}

pub(crate) use inner::*;
