use super::*;

/// Proof that one completed-window slot was reserved before a head mutation.
#[derive(Debug)]
pub struct RetainedWindowSlot {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct KindGuardKey {
    start_ms: u64,
    end_ms: u64,
    lane: FrozenHeadLane,
    series: SeriesRef,
}

impl KindGuardKey {
    fn new(start_ms: u64, end_ms: u64, lane: FrozenHeadLane, series: SeriesRef) -> Self {
        Self {
            start_ms,
            end_ms,
            lane,
            series,
        }
    }

    fn for_window(window: &HeadWindow, series: SeriesRef) -> Self {
        Self::new(
            window.start_ms,
            window.end_ms,
            if window.out_of_order {
                FrozenHeadLane::OutOfOrder
            } else {
                FrozenHeadLane::InOrder
            },
            series,
        )
    }
}

struct RecordSamplesOwned {
    flushed: Vec<HeadWindow>,
    recorded: u64,
}

struct PreparedWindowCoverageUpdate {
    coverage: CoverageLedger,
    order_range: RecordedSampleOrderRange,
    ownership_append: PreparedRecordedSampleAppend,
}

/// Result of recording one sample while retaining whether it was stored.
///
/// The compatibility `record_sample` method intentionally keeps its historical
/// return type. Live publication uses this explicit outcome so a kind mismatch
/// cannot be mistaken for a recorded sample.
#[derive(Debug)]
pub struct HeadRecordOutcome {
    pub recorded: bool,
    pub completed_window: Option<HeadWindow>,
}

pub struct HeadBuffer {
    pub(super) config: HeadConfig,
    pub(super) window: Option<HeadWindow>,
    pub(super) ooo_windows: BTreeMap<(u64, u64), HeadWindow>,
    pub(super) retained_windows: Vec<HeadWindow>,
    pub(super) last_timestamps: LastTimestampTable,
    pub(super) selector_index: Mutex<Option<CachedHeadSelectorIndex>>,
    kind_guards: Option<BTreeMap<KindGuardKey, SampleKind>>,
    next_fragment_sequence: u64,
    live_coverage_tracking: bool,
    #[cfg(test)]
    fail_next_retained_window_reservation: bool,
}

impl HeadBuffer {
    pub fn new(config: HeadConfig) -> io::Result<Self> {
        let _ = Self::window_duration_ms(&config)?;
        let _ = Self::out_of_order_time_window_ms(&config)?;
        Self::validate_block_size(&config)?;
        let adaptive_last_timestamp_table = config.adaptive_last_timestamp_table;
        Ok(Self {
            config,
            window: None,
            ooo_windows: BTreeMap::new(),
            retained_windows: Vec::new(),
            last_timestamps: LastTimestampTable::new(adaptive_last_timestamp_table),
            selector_index: Mutex::new(None),
            kind_guards: None,
            next_fragment_sequence: 1,
            live_coverage_tracking: false,
            #[cfg(test)]
            fail_next_retained_window_reservation: false,
        })
    }

    /// Returns whether this head has never accepted or retained a sample.
    pub fn is_empty(&self) -> bool {
        self.window.is_none()
            && self.ooo_windows.is_empty()
            && self.retained_windows.is_empty()
            && self.last_timestamps.is_empty()
    }

    /// Enables exact recorded-sample coverage before this head accepts data.
    ///
    /// Mixing tracked and untracked samples would make a later ownership proof
    /// ambiguous, so activation is intentionally startup-only.
    pub fn enable_live_coverage_tracking(&mut self) -> io::Result<()> {
        if self.live_coverage_tracking {
            return Ok(());
        }
        if !self.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live coverage tracking must be enabled before recording head samples",
            ));
        }
        self.live_coverage_tracking = true;
        Ok(())
    }

    pub fn live_coverage_tracking_enabled(&self) -> bool {
        self.live_coverage_tracking
    }

    pub fn record_sample(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: SampleValue,
    ) -> io::Result<Option<HeadWindow>> {
        Ok(self
            .record_sample_with_outcome(series, timestamp_ms, value)?
            .completed_window)
    }

    pub fn record_sample_with_outcome(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: SampleValue,
    ) -> io::Result<HeadRecordOutcome> {
        let RecordSamplesOwned {
            mut flushed,
            recorded,
        } = self.record_samples_owned::<false, _>(
            series,
            std::iter::once((timestamp_ms, value, None)),
        )?;
        let completed_window = if flushed.len() > 1 {
            // One input sample can rotate at most one active window. Preserve
            // ownership if that invariant is ever violated instead of
            // silently dropping a completed window.
            self.retained_windows.append(&mut flushed);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "one head sample unexpectedly rotated multiple windows",
            ));
        } else if self.retained_windows.is_empty() {
            flushed.pop()
        } else {
            // A prior batched call may have rotated a complete window before a
            // later sample failed. Return that oldest recoverable window now.
            // Removing first creates enough capacity to park a window rotated
            // by this call without another allocation or changing FIFO order.
            let retained = self.retained_windows.remove(0);
            if let Some(completed) = flushed.pop() {
                self.retained_windows.push(completed);
            }
            Some(retained)
        };
        Ok(HeadRecordOutcome {
            recorded: recorded == 1,
            completed_window,
        })
    }

    /// Records one sample and atomically assigns its exact coverage identity to
    /// the same mutable window. A rejected kind mismatch contributes nothing.
    pub fn record_sample_with_coverage(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: SampleValue,
        contribution: RecordedSampleContribution,
    ) -> io::Result<HeadRecordOutcome> {
        let RecordSamplesOwned {
            mut flushed,
            recorded,
        } = self.record_samples_owned::<true, _>(
            series,
            std::iter::once((timestamp_ms, value, Some(contribution))),
        )?;
        let completed_window = if flushed.len() <= 1 {
            flushed.pop()
        } else {
            self.retained_windows.append(&mut flushed);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "one tracked head sample unexpectedly rotated multiple windows",
            ));
        };
        Ok(HeadRecordOutcome {
            recorded: recorded == 1,
            completed_window,
        })
    }

    pub fn record_samples(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, SampleValue)],
    ) -> io::Result<Vec<HeadWindow>> {
        let RecordSamplesOwned {
            mut flushed,
            recorded: _,
        } = self.record_samples_owned::<false, _>(
            series,
            samples
                .iter()
                .map(|(timestamp_ms, value)| (*timestamp_ms, value.clone(), None)),
        )?;
        let mut retained = std::mem::take(&mut self.retained_windows);
        retained.append(&mut flushed);
        Ok(retained)
    }

    fn record_samples_owned<const TRACKED: bool, I>(
        &mut self,
        series: SeriesRef,
        samples: I,
    ) -> io::Result<RecordSamplesOwned>
    where
        I: IntoIterator<Item = (u64, SampleValue, Option<RecordedSampleContribution>)>,
    {
        if TRACKED != self.live_coverage_tracking {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                if TRACKED {
                    "tracked sample supplied to a head without live coverage enabled"
                } else {
                    "untracked sample supplied to a live-coverage head"
                },
            ));
        }
        let duration_ms = Self::window_duration_ms(&self.config)?;
        let mut flushed = Vec::new();
        let mut recorded = 0u64;
        let Self {
            config,
            window,
            ooo_windows,
            retained_windows,
            last_timestamps,
            selector_index,
            kind_guards,
            next_fragment_sequence: _,
            live_coverage_tracking,
            #[cfg(test)]
                fail_next_retained_window_reservation: _,
        } = self;

        for (ts, value, contribution) in samples {
            let timestamp_slot = last_timestamps.get_mut(series);
            let previous_timestamp_ms = timestamp_slot.as_deref().copied();
            if let Err(error) = Self::validate_sample_timestamp(config, previous_timestamp_ms, ts) {
                retained_windows.append(&mut flushed);
                return Err(error);
            }
            let (start_ms, end_ms) = window_for(ts, duration_ms);
            let route_to_ooo = previous_timestamp_ms.is_some_and(|last| ts < last)
                || window.as_ref().is_some_and(|active| ts < active.start_ms);
            let lane = if route_to_ooo {
                FrozenHeadLane::OutOfOrder
            } else {
                FrozenHeadLane::InOrder
            };
            let guard_key = KindGuardKey::new(start_ms, end_ms, lane, series);
            let value_kind = value.kind();

            let accepted = if let Some(expected) = kind_guards
                .as_ref()
                .and_then(|guards| guards.get(&guard_key))
                .copied()
                && expected != value_kind
            {
                warn!(
                    "Head frozen-range series type mismatch series={} range=[{}, {}) lane={lane:?} expected={expected:?} got={value_kind:?}; dropping sample",
                    series.get(),
                    start_ms,
                    end_ms,
                );
                Ok(false)
            } else {
                let contribution = if TRACKED {
                    Some(contribution.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "tracked head sample omitted its coverage contribution",
                        )
                    })?)
                } else {
                    debug_assert!(contribution.is_none());
                    None
                };
                (|| -> io::Result<bool> {
                    if route_to_ooo {
                        if let Some(target) = ooo_windows.get_mut(&(start_ms, end_ms)) {
                            let coverage_update = contribution
                                .map(|contribution| {
                                    Self::checked_coverage_update(target, contribution)
                                })
                                .transpose()?;
                            let accepted =
                                Self::push_sample_to_window(config, target, series, ts, value)?;
                            if accepted && let Some(update) = coverage_update {
                                target.commit_coverage(
                                    update.coverage,
                                    update.order_range,
                                    update.ownership_append,
                                );
                            }
                            Ok(accepted)
                        } else {
                            // Keep first-OOO-window insertion transactional just like
                            // active-window rotation: publish the map entry only after
                            // its first sample has encoded successfully.
                            let mut next = HeadWindow::new_with_lane_and_coverage(
                                start_ms,
                                end_ms,
                                config.adaptive_series_table,
                                true,
                                *live_coverage_tracking,
                            );
                            let coverage_update = contribution
                                .map(|contribution| {
                                    Self::checked_coverage_update(&mut next, contribution)
                                })
                                .transpose()?;
                            let accepted =
                                Self::push_sample_to_window(config, &mut next, series, ts, value)?;
                            if accepted && let Some(update) = coverage_update {
                                next.commit_coverage(
                                    update.coverage,
                                    update.order_range,
                                    update.ownership_append,
                                );
                            }
                            ooo_windows.insert((start_ms, end_ms), next);
                            Ok(accepted)
                        }
                    } else {
                        let rotate = match window.as_ref() {
                            None => true,
                            Some(active) => ts >= active.end_ms,
                        };

                        if rotate {
                            // Encode the rotating sample before replacing the current
                            // window. A rejected first sample must not discard the
                            // completed window that the caller still needs to publish.
                            let mut next = HeadWindow::new_with_lane_and_coverage(
                                start_ms,
                                end_ms,
                                config.adaptive_series_table,
                                false,
                                *live_coverage_tracking,
                            );
                            let coverage_update = contribution
                                .map(|contribution| {
                                    Self::checked_coverage_update(&mut next, contribution)
                                })
                                .transpose()?;
                            let accepted =
                                Self::push_sample_to_window(config, &mut next, series, ts, value)?;
                            if accepted && let Some(update) = coverage_update {
                                next.commit_coverage(
                                    update.coverage,
                                    update.order_range,
                                    update.ownership_append,
                                );
                            }
                            if let Some(mut completed) = window.replace(next) {
                                if !*live_coverage_tracking {
                                    completed.seal_all_series();
                                }
                                flushed.push(completed);
                            }
                            Ok(accepted)
                        } else {
                            let Some(active) = window.as_mut() else {
                                return Ok(false);
                            };
                            let coverage_update = contribution
                                .map(|contribution| {
                                    Self::checked_coverage_update(active, contribution)
                                })
                                .transpose()?;
                            let accepted =
                                Self::push_sample_to_window(config, active, series, ts, value)?;
                            if accepted && let Some(update) = coverage_update {
                                active.commit_coverage(
                                    update.coverage,
                                    update.order_range,
                                    update.ownership_append,
                                );
                            }
                            Ok(accepted)
                        }
                    }
                })()
            };
            let accepted = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    retained_windows.append(&mut flushed);
                    return Err(error);
                }
            };

            if accepted {
                if let Some(guards) = kind_guards.as_mut() {
                    match guards.get(&guard_key) {
                        Some(existing) if *existing != value_kind => {
                            retained_windows.append(&mut flushed);
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "head kind guard changed during exclusive recording",
                            ));
                        }
                        Some(_) => {}
                        None => {
                            guards.insert(guard_key, value_kind);
                        }
                    }
                }
                recorded = recorded.checked_add(1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recorded sample count overflows",
                    )
                })?;
                match timestamp_slot {
                    Some(previous) if ts > *previous => *previous = ts,
                    None => {
                        last_timestamps.insert(series, ts);
                    }
                    Some(_) => {}
                }
                if let Ok(cache) = selector_index.get_mut() {
                    *cache = None;
                }
            }
        }

        Ok(RecordSamplesOwned { flushed, recorded })
    }

    pub fn drain(&mut self) -> Option<HeadWindow> {
        self.clear_selector_index_cache();
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            Some(window)
        } else {
            None
        }
    }

    pub fn drain_windows(&mut self) -> Vec<HeadWindow> {
        self.clear_selector_index_cache();
        let mut windows = std::mem::take(&mut self.retained_windows);
        for (_range, mut window) in std::mem::take(&mut self.ooo_windows) {
            window.seal_all_series();
            windows.push(window);
        }
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            windows.push(window);
        }
        windows.sort_by_key(|window| (window.start_ms, window.end_ms));
        windows
    }

    /// Removes the out-of-order lane for exactly one aligned head range.
    ///
    /// A caller rotating an in-order window uses this to co-seal OOO samples
    /// that arrived before that window became immutable. OOO windows for older
    /// ranges remain in the head for the post-seal late-data path.
    pub fn take_out_of_order_window(&mut self, start_ms: u64, end_ms: u64) -> Option<HeadWindow> {
        let removed = self.ooo_windows.remove(&(start_ms, end_ms));
        if removed.is_some() {
            self.clear_selector_index_cache();
        }
        removed
    }

    /// Returns a rotated in-order window to the buffer until the next live
    /// publication boundary freezes it.
    ///
    /// The ordinary ingestion path consumes the window returned by
    /// [`HeadRecordOutcome`]. Live publication instead needs the window to
    /// remain under the head's recoverable ownership until it has entered the
    /// publisher's pending queue. Keeping that transition here also lets
    /// [`try_freeze_for_publication`](Self::try_freeze_for_publication) assign
    /// the same monotonically increasing fragment sequence as active and OOO
    /// tails.
    pub fn try_reserve_retained_window_for_publication(
        &mut self,
    ) -> io::Result<RetainedWindowSlot> {
        if !self.live_coverage_tracking {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "completed-window reservation requires live coverage tracking",
            ));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_retained_window_reservation) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "injected completed-window retention reservation failure",
            ));
        }
        self.retained_windows
            .try_reserve(1)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        Ok(RetainedWindowSlot { _private: () })
    }

    /// Injects one deterministic failure before the completed-window
    /// retention vector is reserved or any head state is changed.
    #[cfg(test)]
    pub(crate) fn fail_next_retained_window_reservation(&mut self) {
        self.fail_next_retained_window_reservation = true;
    }

    pub fn retain_completed_window_for_publication(
        &mut self,
        _slot: RetainedWindowSlot,
        window: HeadWindow,
    ) -> io::Result<()> {
        if !self.live_coverage_tracking || !window.coverage_tracking_enabled() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "completed-window retention requires live coverage tracking",
            ));
        }
        if window.is_out_of_order() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only a rotated in-order window may be retained for publication",
            ));
        }
        if self.retained_windows.iter().any(|retained| {
            retained.start_ms == window.start_ms
                && retained.end_ms == window.end_ms
                && !retained.out_of_order
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate retained in-order head window",
            ));
        }
        debug_assert!(self.retained_windows.len() < self.retained_windows.capacity());
        self.retained_windows.push(window);
        self.clear_selector_index_cache();
        Ok(())
    }

    pub fn window_range(&self) -> Option<(u64, u64)> {
        self.window.as_ref().map(|w| (w.start_ms, w.end_ms))
    }

    pub fn last_timestamp_table_stats(&self) -> LastTimestampTableStats {
        self.last_timestamps.stats()
    }

    /// Returns the maximum number of fragments produced by the next freeze.
    ///
    /// The publisher uses this exact non-empty-window count to reserve durable
    /// ownership slots before any window is removed from the mutable head.
    pub fn publication_fragment_count(&self) -> usize {
        self.retained_windows
            .iter()
            .filter(|window| Self::window_has_samples(window))
            .count()
            .saturating_add(
                self.window
                    .iter()
                    .filter(|window| Self::window_has_samples(window))
                    .count(),
            )
            .saturating_add(
                self.ooo_windows
                    .values()
                    .filter(|window| Self::window_has_samples(window))
                    .count(),
            )
    }

    /// Freezes every currently queryable non-empty window for publication.
    ///
    /// Completed/retained windows are removed. The active window and each OOO
    /// window are replaced by empty tails with the identical range and lane.
    /// `LastTimestampTable` is deliberately untouched. If a later window
    /// fails, that window is restored and the returned error owns every
    /// fragment completed earlier in this call.
    pub fn try_freeze_for_publication(
        &mut self,
    ) -> Result<Vec<FrozenHeadFragment>, FreezeHeadBufferError> {
        if let Err(error) = self.activate_kind_guards() {
            return Err(FreezeHeadBufferError::new(error, Vec::new()));
        }

        let fragment_count = self.publication_fragment_count();

        let count_u64 = match u64::try_from(fragment_count) {
            Ok(count) => count,
            Err(error) => {
                return Err(FreezeHeadBufferError::new(
                    io::Error::new(io::ErrorKind::InvalidData, error),
                    Vec::new(),
                ));
            }
        };
        if self.next_fragment_sequence.checked_add(count_u64).is_none() {
            return Err(FreezeHeadBufferError::new(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "head fragment publication sequence overflows u64",
                ),
                Vec::new(),
            ));
        }

        let mut fragments = Vec::new();
        if let Err(error) = fragments.try_reserve_exact(fragment_count) {
            return Err(FreezeHeadBufferError::new(
                io::Error::new(io::ErrorKind::OutOfMemory, error),
                fragments,
            ));
        }

        let adaptive = self.config.adaptive_series_table;
        let mut next_sequence = self.next_fragment_sequence;

        for window in &mut self.retained_windows {
            match Self::try_freeze_window_slot(window, adaptive, next_sequence) {
                Ok(Some(fragment)) => {
                    fragments.push(fragment);
                    next_sequence += 1;
                }
                Ok(None) => {}
                Err(error) => {
                    self.next_fragment_sequence = next_sequence;
                    self.retained_windows.retain(Self::window_has_samples);
                    self.clear_selector_index_cache();
                    return Err(FreezeHeadBufferError::new(error, fragments));
                }
            }
        }
        self.retained_windows.retain(Self::window_has_samples);

        if let Some(window) = self.window.as_mut() {
            match Self::try_freeze_window_slot(window, adaptive, next_sequence) {
                Ok(Some(fragment)) => {
                    fragments.push(fragment);
                    next_sequence += 1;
                }
                Ok(None) => {}
                Err(error) => {
                    self.next_fragment_sequence = next_sequence;
                    self.clear_selector_index_cache();
                    return Err(FreezeHeadBufferError::new(error, fragments));
                }
            }
        }

        for window in self.ooo_windows.values_mut() {
            match Self::try_freeze_window_slot(window, adaptive, next_sequence) {
                Ok(Some(fragment)) => {
                    fragments.push(fragment);
                    next_sequence += 1;
                }
                Ok(None) => {}
                Err(error) => {
                    self.next_fragment_sequence = next_sequence;
                    self.clear_selector_index_cache();
                    return Err(FreezeHeadBufferError::new(error, fragments));
                }
            }
        }

        self.next_fragment_sequence = next_sequence;
        if !fragments.is_empty() {
            self.clear_selector_index_cache();
        }
        Ok(fragments)
    }

    /// Retires kind guards after the caller has completed the exact
    /// range/lane handoff.
    ///
    /// The operation refuses to retire while this buffer still has samples in
    /// that key. Frozen fragments are owned by the publisher, so the caller
    /// must invoke this only after those fragments are no longer an active
    /// accumulation.
    pub fn retire_kind_guards(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        lane: FrozenHeadLane,
    ) -> io::Result<usize> {
        self.validate_kind_guard_retirement(start_ms, end_ms, lane)?;

        let Some(guards) = self.kind_guards.as_mut() else {
            return Ok(0);
        };
        let old_len = guards.len();
        guards.retain(|key, _kind| {
            !(key.start_ms == start_ms && key.end_ms == end_ms && key.lane == lane)
        });
        Ok(old_len.saturating_sub(guards.len()))
    }

    /// Validates a kind-guard retirement without mutating the head.
    ///
    /// Publication uses this in a complete first pass over every handed-off
    /// range. Because the ingestion thread exclusively owns the mutable
    /// heads, a subsequent retirement pass cannot acquire new samples between
    /// validation and application.
    pub fn validate_kind_guard_retirement(
        &self,
        start_ms: u64,
        end_ms: u64,
        lane: FrozenHeadLane,
    ) -> io::Result<()> {
        let has_samples = self
            .retained_windows
            .iter()
            .chain(self.window.iter())
            .chain(self.ooo_windows.values())
            .any(|window| {
                window.start_ms == start_ms
                    && window.end_ms == end_ms
                    && window.out_of_order == lane.is_out_of_order()
                    && Self::window_has_samples(window)
            });
        if has_samples {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot retire a head kind guard while its mutable window has samples",
            ));
        }
        Ok(())
    }

    pub fn kind_guard_count(&self) -> usize {
        self.kind_guards.as_ref().map_or(0, BTreeMap::len)
    }

    pub fn query_selector<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(labels, selector, start_ms, end_ms, &mut budget)
    }

    pub(crate) fn query_selector_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            results.extend(self.query_window_selector_with_budget(
                labels,
                window,
                &matchers,
                selector.projection(),
                start_ms,
                end_ms,
                budget,
            )?);
        }

        Ok(merge_head_query_results(results))
    }

    pub(crate) fn query_native_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            let index = self.selector_index(labels, window)?;
            let candidate_series = index.matching_series(&matchers, budget, false)?;

            for series in candidate_series {
                let Some(encoded) = window.series.get(series) else {
                    continue;
                };
                if encoded.kind() != SampleKind::Histogram {
                    continue;
                }
                let Some(indexed) = index.series(&series) else {
                    continue;
                };
                budget.observe_matched_series(indexed.series_id)?;

                let SeriesSamples::Histogram { samples } =
                    encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms < start_ms || timestamp_ms > end_ms {
                        continue;
                    }
                    result.push_sample(PromqlHistogramSample::from_histogram_value(
                        timestamp_ms,
                        value,
                    ));
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_histogram_query_results(results))
    }

    pub(crate) fn query_native_exponential_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            let index = self.selector_index(labels, window)?;
            let candidate_series = index.matching_series(&matchers, budget, false)?;

            for series in candidate_series {
                let Some(encoded) = window.series.get(series) else {
                    continue;
                };
                if encoded.kind() != SampleKind::ExponentialHistogram {
                    continue;
                }
                let Some(indexed) = index.series(&series) else {
                    continue;
                };
                budget.observe_matched_series(indexed.series_id)?;

                let SeriesSamples::ExponentialHistogram { samples } =
                    encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlExponentialHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms < start_ms || timestamp_ms > end_ms {
                        continue;
                    }
                    result.push_sample(
                        PromqlExponentialHistogramSample::from_exponential_histogram_value(
                            timestamp_ms,
                            value,
                        ),
                    );
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_exponential_histogram_query_results(results))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "window selection needs explicit labels, bounds, projection, matchers, and budget"
    )]
    pub(super) fn query_window_selector_with_budget<R>(
        &self,
        labels: &R,
        window: &HeadWindow,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let index = self.selector_index(labels, window)?;
        let candidate_series = index.matching_series(
            matchers,
            budget,
            projection_matches_promql_metric_name_regex(projection),
        )?;
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);

        for series in candidate_series {
            let Some(encoded) = window.series.get(series) else {
                continue;
            };
            if !sample_kind_matches_projection(projection, encoded.kind()) {
                continue;
            }
            let Some(indexed) = index.series(&series) else {
                continue;
            };
            budget.observe_matched_series(indexed.series_id)?;

            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            match (projection, samples) {
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Float { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples,
                    ));
                }
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Int64 { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples
                            .into_iter()
                            .map(|(timestamp_ms, value)| (timestamp_ms, value as f64))
                            .collect(),
                    ));
                }
                (SegmentProjection::None, _) => {}
                (projection, samples) => {
                    let decoded_count = series_samples_len(&samples);
                    let mut projected = project_head_series_samples(
                        projection,
                        &indexed.labels,
                        samples,
                        start_ms,
                        end_ms,
                    )?;
                    budget.observe_samples_decoded(decoded_count as u64)?;
                    if let Some(filter) = &projected_label_filter {
                        projected
                            .retain(|result| query_labels_match_compiled(&result.labels, filter));
                    }
                    results.append(&mut projected);
                }
            }
        }

        results.sort_by_key(|result| result.series_id);
        budget.observe_projected_results(&results)?;
        Ok(results)
    }

    pub fn metric_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values<R>(
        &self,
        labels: &R,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        let label_name = if label_name == METRIC_NAME_LABEL {
            METRIC_NAME_LABEL.to_string()
        } else {
            crate::promql::normalize_label_name(label_name)
        };
        Ok(metadata.label_values(&label_name))
    }

    pub(crate) fn collect_metadata<R>(
        &self,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(());
        }

        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            Self::collect_window_metadata(labels, window, start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    pub(super) fn collect_window_metadata<R>(
        labels: &R,
        window: &HeadWindow,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        let range_end_ms = end_ms.saturating_add(1);
        for (series, encoded) in window.series.iter() {
            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            if samples.is_empty() {
                continue;
            }
            let Some((_, canonical_labels)) = canonical_head_labelset(labels, series) else {
                continue;
            };
            metadata.add_labelset(&canonical_labels);
        }

        Ok(())
    }

    pub(super) fn query_windows(&self) -> Vec<&HeadWindow> {
        let mut windows: Vec<(u8, &HeadWindow)> = Vec::new();
        for window in &self.retained_windows {
            windows.push((0, window));
        }
        if let Some(window) = &self.window {
            windows.push((0, window));
        }
        for window in self.ooo_windows.values() {
            windows.push((1, window));
        }
        windows.sort_by_key(|(lane_precedence, window)| {
            (window.start_ms, window.end_ms, *lane_precedence)
        });
        windows.into_iter().map(|(_, window)| window).collect()
    }

    pub(super) fn window_overlaps_range(window: &HeadWindow, start_ms: u64, end_ms: u64) -> bool {
        window.end_ms > start_ms && window.start_ms <= end_ms
    }

    pub(super) fn selector_index<R>(
        &self,
        labels: &R,
        window: &HeadWindow,
    ) -> io::Result<HeadSelectorIndex>
    where
        R: SeriesLabelResolver,
    {
        let key = HeadSelectorIndexKey::new(window, labels.len());
        {
            let cache = self
                .selector_index
                .lock()
                .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
            if let Some(cached) = cache.as_ref()
                && cached.key == key
            {
                return Ok(cached.index.clone());
            }
        }

        let index = HeadSelectorIndex::build(window, labels)?;
        let mut cache = self
            .selector_index
            .lock()
            .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
        *cache = Some(CachedHeadSelectorIndex {
            key,
            index: index.clone(),
        });
        Ok(index)
    }

    pub(super) fn clear_selector_index_cache(&mut self) {
        if let Ok(cache) = self.selector_index.get_mut() {
            *cache = None;
        }
    }

    fn activate_kind_guards(&mut self) -> io::Result<()> {
        if self.kind_guards.is_some() {
            return Ok(());
        }

        let mut guards = BTreeMap::new();
        for window in self
            .retained_windows
            .iter()
            .chain(self.window.iter())
            .chain(self.ooo_windows.values())
        {
            for (series, encoded) in window.series.iter() {
                let key = KindGuardKey::for_window(window, series);
                match guards.insert(key, encoded.kind()) {
                    Some(existing) if existing != encoded.kind() => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "head range/lane series {} contains conflicting sample kinds",
                                series.get()
                            ),
                        ));
                    }
                    Some(_) | None => {}
                }
            }
        }
        self.kind_guards = Some(guards);
        Ok(())
    }

    fn try_freeze_window_slot(
        window: &mut HeadWindow,
        adaptive_series_table: bool,
        publication_sequence: u64,
    ) -> io::Result<Option<FrozenHeadFragment>> {
        if !Self::window_has_samples(window) {
            return Ok(None);
        }

        let replacement = window.empty_tail_like(adaptive_series_table);
        let owned = std::mem::replace(window, replacement);
        match owned.try_freeze() {
            Ok(mut fragment) => {
                fragment.set_publication_sequence(publication_sequence);
                Ok(Some(fragment))
            }
            Err(error) => {
                let (source, recovered) = error.into_parts();
                *window = recovered;
                Err(source)
            }
        }
    }

    fn window_has_samples(window: &HeadWindow) -> bool {
        window.datapoints != 0 || !window.series.is_empty()
    }

    fn checked_coverage_update(
        target: &mut HeadWindow,
        contribution: RecordedSampleContribution,
    ) -> io::Result<PreparedWindowCoverageUpdate> {
        if !target.coverage_tracking {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tracked sample targeted an untracked head window",
            ));
        }
        if target.coverage.sample_count() != target.datapoints
            || target.recorded_orders.sample_count() != target.datapoints
            || (target.datapoints == 0) != target.recorded_order_range.is_none()
            || (target.datapoints == 0) != target.recorded_orders.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "head exact coverage no longer matches recorded datapoints",
            ));
        }
        let coverage = target.coverage.checked_with_contribution(contribution)?;
        let order_range = match target.recorded_order_range {
            Some(range) => range.checked_extend(contribution.order())?,
            None => RecordedSampleOrderRange::one(contribution.order()),
        };
        let ownership_append = target
            .recorded_orders
            .try_prepare_append(contribution.order())?;
        Ok(PreparedWindowCoverageUpdate {
            coverage,
            order_range,
            ownership_append,
        })
    }

    pub(super) fn push_sample_to_window(
        config: &HeadConfig,
        window: &mut HeadWindow,
        series: SeriesRef,
        timestamp_ms: u64,
        value: SampleValue,
    ) -> io::Result<bool> {
        let base_ms = window.start_ms;
        let block_size = config.block_size;
        let value_kind = value.kind();
        let encoding = match value_kind {
            SampleKind::Float => SeriesEncoding::Float(config.float_encoding),
            SampleKind::Int64 => SeriesEncoding::Int(config.int_encoding),
            SampleKind::Histogram => SeriesEncoding::Histogram(config.varlen_encoding),
            SampleKind::ExponentialHistogram => {
                SeriesEncoding::ExponentialHistogram(config.varlen_encoding)
            }
            SampleKind::Summary => SeriesEncoding::Summary(config.varlen_encoding),
        };
        if let Some(encoded) = window.series.get_mut(series) {
            if encoded.kind() != value_kind {
                warn!(
                    "Head series type mismatch series={} expected={:?} got={:?}; dropping sample",
                    series.get(),
                    encoded.kind(),
                    value_kind
                );
                return Ok(false);
            }
            encoded.push_sample(
                series,
                base_ms,
                timestamp_ms,
                value,
                block_size,
                &mut window.arena,
            )?;
        } else {
            // Keep first-series insertion transactional: a failed first encode
            // must not leave an empty series in the head table.
            let mut encoded =
                EncodedSeries::new(encoding, config.compact_numeric_series, block_size);
            encoded.push_sample(
                series,
                base_ms,
                timestamp_ms,
                value,
                block_size,
                &mut window.arena,
            )?;
            if window.series.insert_new(series, encoded).is_err() {
                return Err(io::Error::other(
                    "head series appeared during exclusive insertion",
                ));
            }
        }
        window.datapoints = window.datapoints.saturating_add(1);
        Ok(true)
    }

    pub(super) fn window_duration_ms(config: &HeadConfig) -> io::Result<u64> {
        let ms = config.window_duration.as_millis();
        if ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "window_duration must be > 0",
            ));
        }
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "window_duration is too large",
            ));
        }
        Ok(ms as u64)
    }

    pub(super) fn out_of_order_time_window_ms(config: &HeadConfig) -> io::Result<u64> {
        let ms = config.out_of_order_time_window.as_millis();
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "out_of_order_time_window is too large",
            ));
        }
        Ok(ms as u64)
    }

    pub(super) fn validate_sample_timestamp(
        config: &HeadConfig,
        last_timestamp_ms: Option<u64>,
        timestamp_ms: u64,
    ) -> io::Result<()> {
        let Some(last_timestamp_ms) = last_timestamp_ms else {
            return Ok(());
        };
        if timestamp_ms >= last_timestamp_ms {
            return Ok(());
        }

        let window_ms = Self::out_of_order_time_window_ms(config)?;
        let lower_bound_ms = last_timestamp_ms.saturating_sub(window_ms);
        if timestamp_ms < lower_bound_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sample is outside out_of_order_time_window",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_block_size(config: &HeadConfig) -> io::Result<()> {
        if config.block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_size must be > 0",
            ));
        }
        if config.block_size > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_size is too large",
            ));
        }
        Ok(())
    }
}

pub(super) fn canonical_head_labelset<R>(
    labels: &R,
    series: SeriesRef,
) -> Option<(u64, Vec<(String, String)>)>
where
    R: SeriesLabelResolver,
{
    if series.get() as usize >= labels.len() {
        return None;
    }

    let mut metric_name = String::new();
    let mut attributes = Vec::new();
    labels.visit_labelset(series, &mut |key, value| {
        if key == METRIC_NAME_LABEL {
            metric_name = value.to_string();
        } else {
            attributes.push((key.to_string(), value.to_string()));
        }
    });

    let attribute_refs: Vec<(&str, &str)> = attributes
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let canonical = canonicalize_labelset(&metric_name, &attribute_refs);
    let id = series_id(&canonical);
    let labels = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    Some((id, labels))
}

pub(super) fn merge_head_query_results(
    results: Vec<SegmentQueryResult>,
) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let entry = merged.entry(result.series_id).or_insert_with(|| {
            SegmentQueryResult::with_shared_labels(result.series_id, result.labels.clone())
        });
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

pub(super) fn sample_kind_matches_projection(
    projection: &SegmentProjection,
    kind: SampleKind,
) -> bool {
    match projection {
        SegmentProjection::None => matches!(kind, SampleKind::Float | SampleKind::Int64),
        SegmentProjection::AllPromql { .. } => true,
        SegmentProjection::Count | SegmentProjection::Sum => matches!(
            kind,
            SampleKind::Histogram | SampleKind::ExponentialHistogram | SampleKind::Summary
        ),
        SegmentProjection::HistogramBucket { .. } => matches!(
            kind,
            SampleKind::Histogram | SampleKind::ExponentialHistogram
        ),
        SegmentProjection::NativeHistogram => kind == SampleKind::Histogram,
        SegmentProjection::NativeExponentialHistogram => kind == SampleKind::ExponentialHistogram,
        SegmentProjection::SummaryQuantile { .. } => kind == SampleKind::Summary,
    }
}

#[cfg(test)]
mod publication_sequence_tests {
    use super::*;

    #[test]
    fn publication_sequence_exhaustion_is_detected_before_freezing_or_losing_samples() {
        let mut head = HeadBuffer::new(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ))
        .unwrap();
        let series = SeriesRef::new(7);

        head.next_fragment_sequence = u64::MAX - 1;
        head.record_sample(series, 1_000, SampleValue::Float(1.0))
            .unwrap();
        let first = head.try_freeze_for_publication().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].publication_sequence(), u64::MAX - 1);
        assert_eq!(head.next_fragment_sequence, u64::MAX);

        head.record_sample(series, 2_000, SampleValue::Float(2.0))
            .unwrap();
        let error = head.try_freeze_for_publication().unwrap_err();
        assert_eq!(error.error().kind(), io::ErrorKind::InvalidData);
        let (source, completed) = error.into_parts();
        assert!(
            source
                .to_string()
                .contains("fragment publication sequence overflows")
        );
        assert!(completed.is_empty());
        assert_eq!(head.next_fragment_sequence, u64::MAX);
        assert_eq!(head.publication_fragment_count(), 1);
        assert_eq!(head.window.as_ref().unwrap().datapoints, 1);

        // Reset only the synthetic test counter and prove the retained tail is
        // still complete and can be frozen on a later attempt.
        head.next_fragment_sequence = 7;
        let retry = head.try_freeze_for_publication().unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].publication_sequence(), 7);
        assert_eq!(
            retry[0].series_samples_in_range(0, 10_000).unwrap(),
            vec![(
                series,
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(2_000, 2.0)],
                },
            )]
        );
    }
}
