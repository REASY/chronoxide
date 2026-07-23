use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataRuntimeReport {
    pub(in super::super) counters_delta: QueryBenchmarkMetadataRuntimeCounterDeltas,
    pub(in super::super) start_gauges: QueryBenchmarkMetadataRuntimeGauges,
    pub(in super::super) end_gauges: QueryBenchmarkMetadataRuntimeGauges,
    pub(in super::super) lifetime_peaks_after_run: QueryBenchmarkMetadataRuntimeLifetimePeaks,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataRuntimeCounterDeltas {
    pub(in super::super) cache: QueryBenchmarkMetadataCacheCounterDeltas,
    pub(in super::super) governor: QueryBenchmarkMetadataGovernorCounterDeltas,
    pub(in super::super) file_manager: QueryBenchmarkMetadataFileManagerCounterDeltas,
    pub(in super::super) reads: QueryBenchmarkMetadataReadDeltas,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataCacheCounterDeltas {
    pub(in super::super) hits: u64,
    pub(in super::super) misses: u64,
    pub(in super::super) evictions: u64,
    pub(in super::super) single_flight_waits: u64,
    pub(in super::super) successful_loads: u64,
    pub(in super::super) failed_loads: u64,
    pub(in super::super) corruption_detections: u64,
    pub(in super::super) corruption_hits: u64,
    pub(in super::super) resident_admissions: u64,
    pub(in super::super) resident_admission_refusals: u64,
    pub(in super::super) resident_admission_bypasses: u64,
    pub(in super::super) class_admissions: Vec<QueryBenchmarkMetadataCacheClassAdmissionDeltas>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataCacheClassAdmissionDeltas {
    pub(in super::super) class: &'static str,
    pub(in super::super) resident_admissions: u64,
    pub(in super::super) resident_admission_refusals: u64,
    pub(in super::super) resident_admission_bypasses: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataGovernorCounterDeltas {
    pub(in super::super) retained_refusals: u64,
    pub(in super::super) in_flight_refusals: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataFileManagerCounterDeltas {
    pub(in super::super) preflight_calls: u64,
    pub(in super::super) successful_preflights: u64,
    pub(in super::super) preflight_failures: u64,
    pub(in super::super) acquire_calls: u64,
    pub(in super::super) successful_acquires: u64,
    pub(in super::super) requested_handles: u64,
    pub(in super::super) deduplicated_handles: u64,
    pub(in super::super) descriptor_opens: u64,
    pub(in super::super) descriptor_closes: u64,
    pub(in super::super) descriptor_reuses: u64,
    pub(in super::super) lease_clones: u64,
    pub(in super::super) idle_evictions: u64,
    pub(in super::super) capacity_waits: u64,
    pub(in super::super) capacity_refusals: u64,
    pub(in super::super) open_failures: u64,
    pub(in super::super) structural_replacements: u64,
    pub(in super::super) acquisition_rollbacks: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataReadDeltas {
    pub(in super::super) issued: QueryBenchmarkMetadataReadCount,
    pub(in super::super) unclassified: QueryBenchmarkMetadataReadCount,
    pub(in super::super) by_file: Vec<QueryBenchmarkMetadataFileRead>,
    pub(in super::super) by_class: Vec<QueryBenchmarkMetadataClassRead>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataReadCount {
    pub(in super::super) calls: u64,
    pub(in super::super) bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataFileRead {
    pub(in super::super) file: &'static str,
    pub(in super::super) calls: u64,
    pub(in super::super) bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataClassRead {
    pub(in super::super) class: &'static str,
    pub(in super::super) calls: u64,
    pub(in super::super) bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataRuntimeGauges {
    pub(in super::super) cache: QueryBenchmarkMetadataCacheEndGauges,
    pub(in super::super) governor: QueryBenchmarkMetadataGovernorEndGauges,
    pub(in super::super) file_manager: QueryBenchmarkMetadataFileManagerEndGauges,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataCacheEndGauges {
    pub(in super::super) resident_entries: u64,
    pub(in super::super) live_allocations: u64,
    pub(in super::super) active_loads: u64,
    pub(in super::super) registered_artifacts: u64,
    pub(in super::super) ledger_reserved_bytes: u64,
    pub(in super::super) ledger_in_flight_bytes: u64,
    pub(in super::super) ledger_retained_bytes: u64,
    pub(in super::super) sticky_artifacts: u64,
    pub(in super::super) sticky_charged_bytes: u64,
    pub(in super::super) class_charges: Vec<QueryBenchmarkMetadataCacheClassEndGauge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataCacheClassEndGauge {
    pub(in super::super) class: &'static str,
    pub(in super::super) in_flight_bytes: u64,
    pub(in super::super) retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataGovernorEndGauges {
    pub(in super::super) retained_max_bytes: u64,
    pub(in super::super) in_flight_max_bytes: u64,
    pub(in super::super) retained_bytes: u64,
    pub(in super::super) in_flight_bytes: u64,
    pub(in super::super) usage_charges: Vec<QueryBenchmarkMetadataUsageEndGauge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataUsageEndGauge {
    pub(in super::super) usage: &'static str,
    pub(in super::super) in_flight_bytes: u64,
    pub(in super::super) retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataFileManagerEndGauges {
    pub(in super::super) max_open_files: u32,
    pub(in super::super) max_cached_open_files: u32,
    pub(in super::super) open_files: u32,
    pub(in super::super) occupied_open_slots: u32,
    pub(in super::super) active_open_files: u32,
    pub(in super::super) cached_open_files: u32,
    pub(in super::super) opening_files: u32,
    pub(in super::super) pending_open_files: u32,
    pub(in super::super) preflighting_files: u32,
    pub(in super::super) closing_files: u32,
    pub(in super::super) active_leases: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataRuntimeLifetimePeaks {
    pub(in super::super) cache_class_charges: Vec<QueryBenchmarkMetadataCacheClassLifetimePeak>,
    pub(in super::super) governor: QueryBenchmarkMetadataGovernorLifetimePeaks,
    pub(in super::super) file_manager: QueryBenchmarkMetadataFileManagerLifetimePeaks,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataCacheClassLifetimePeak {
    pub(in super::super) class: &'static str,
    pub(in super::super) peak_in_flight_bytes: u64,
    pub(in super::super) peak_retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataGovernorLifetimePeaks {
    pub(in super::super) peak_retained_bytes: u64,
    pub(in super::super) peak_in_flight_bytes: u64,
    pub(in super::super) usage_charges: Vec<QueryBenchmarkMetadataUsageLifetimePeak>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataUsageLifetimePeak {
    pub(in super::super) usage: &'static str,
    pub(in super::super) peak_in_flight_bytes: u64,
    pub(in super::super) peak_retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(in super::super) struct QueryBenchmarkMetadataFileManagerLifetimePeaks {
    pub(in super::super) peak_open_files: u32,
    pub(in super::super) peak_occupied_open_slots: u32,
    pub(in super::super) peak_active_open_files: u32,
    pub(in super::super) peak_cached_open_files: u32,
    pub(in super::super) peak_active_leases: u32,
    pub(in super::super) peak_preflighting_files: u32,
}

impl QueryBenchmarkMetadataRuntimeReport {
    pub(in super::super) fn between(
        before: StoreMetadataRuntimeSnapshot,
        after: StoreMetadataRuntimeSnapshot,
    ) -> Self {
        let reads = after.reads.delta_since(before.reads);
        Self {
            counters_delta: QueryBenchmarkMetadataRuntimeCounterDeltas {
                cache: QueryBenchmarkMetadataCacheCounterDeltas {
                    hits: after.cache.hits.saturating_sub(before.cache.hits),
                    misses: after.cache.misses.saturating_sub(before.cache.misses),
                    evictions: after.cache.evictions.saturating_sub(before.cache.evictions),
                    single_flight_waits: after
                        .cache
                        .single_flight_waits
                        .saturating_sub(before.cache.single_flight_waits),
                    successful_loads: after
                        .cache
                        .successful_loads
                        .saturating_sub(before.cache.successful_loads),
                    failed_loads: after
                        .cache
                        .failed_loads
                        .saturating_sub(before.cache.failed_loads),
                    corruption_detections: after
                        .cache
                        .corruption_detections
                        .saturating_sub(before.cache.corruption_detections),
                    corruption_hits: after
                        .cache
                        .corruption_hits
                        .saturating_sub(before.cache.corruption_hits),
                    resident_admissions: after
                        .cache
                        .resident_admissions
                        .saturating_sub(before.cache.resident_admissions),
                    resident_admission_refusals: after
                        .cache
                        .resident_admission_refusals
                        .saturating_sub(before.cache.resident_admission_refusals),
                    resident_admission_bypasses: after
                        .cache
                        .resident_admission_bypasses
                        .saturating_sub(before.cache.resident_admission_bypasses),
                    class_admissions: after
                        .cache
                        .class_admissions
                        .into_iter()
                        .enumerate()
                        .map(|(index, counters)| {
                            let before = before.cache.class_admissions[index];
                            QueryBenchmarkMetadataCacheClassAdmissionDeltas {
                                class: metadata_cache_class_name(counters.class),
                                resident_admissions: counters
                                    .resident_admissions
                                    .saturating_sub(before.resident_admissions),
                                resident_admission_refusals: counters
                                    .resident_admission_refusals
                                    .saturating_sub(before.resident_admission_refusals),
                                resident_admission_bypasses: counters
                                    .resident_admission_bypasses
                                    .saturating_sub(before.resident_admission_bypasses),
                            }
                        })
                        .collect(),
                },
                governor: QueryBenchmarkMetadataGovernorCounterDeltas {
                    retained_refusals: after
                        .governor
                        .retained_refusals
                        .saturating_sub(before.governor.retained_refusals),
                    in_flight_refusals: after
                        .governor
                        .in_flight_refusals
                        .saturating_sub(before.governor.in_flight_refusals),
                },
                file_manager: QueryBenchmarkMetadataFileManagerCounterDeltas {
                    preflight_calls: after
                        .files
                        .preflight_calls
                        .saturating_sub(before.files.preflight_calls),
                    successful_preflights: after
                        .files
                        .successful_preflights
                        .saturating_sub(before.files.successful_preflights),
                    preflight_failures: after
                        .files
                        .preflight_failures
                        .saturating_sub(before.files.preflight_failures),
                    acquire_calls: after
                        .files
                        .acquire_calls
                        .saturating_sub(before.files.acquire_calls),
                    successful_acquires: after
                        .files
                        .successful_acquires
                        .saturating_sub(before.files.successful_acquires),
                    requested_handles: after
                        .files
                        .requested_handles
                        .saturating_sub(before.files.requested_handles),
                    deduplicated_handles: after
                        .files
                        .deduplicated_handles
                        .saturating_sub(before.files.deduplicated_handles),
                    descriptor_opens: after
                        .files
                        .descriptor_opens
                        .saturating_sub(before.files.descriptor_opens),
                    descriptor_closes: after
                        .files
                        .descriptor_closes
                        .saturating_sub(before.files.descriptor_closes),
                    descriptor_reuses: after
                        .files
                        .descriptor_reuses
                        .saturating_sub(before.files.descriptor_reuses),
                    lease_clones: after
                        .files
                        .lease_clones
                        .saturating_sub(before.files.lease_clones),
                    idle_evictions: after
                        .files
                        .idle_evictions
                        .saturating_sub(before.files.idle_evictions),
                    capacity_waits: after
                        .files
                        .capacity_waits
                        .saturating_sub(before.files.capacity_waits),
                    capacity_refusals: after
                        .files
                        .capacity_refusals
                        .saturating_sub(before.files.capacity_refusals),
                    open_failures: after
                        .files
                        .open_failures
                        .saturating_sub(before.files.open_failures),
                    structural_replacements: after
                        .files
                        .structural_replacements
                        .saturating_sub(before.files.structural_replacements),
                    acquisition_rollbacks: after
                        .files
                        .acquisition_rollbacks
                        .saturating_sub(before.files.acquisition_rollbacks),
                },
                reads: QueryBenchmarkMetadataReadDeltas {
                    issued: QueryBenchmarkMetadataReadCount {
                        calls: reads.issued.calls,
                        bytes: reads.issued.bytes,
                    },
                    unclassified: QueryBenchmarkMetadataReadCount {
                        calls: reads.unclassified.calls,
                        bytes: reads.unclassified.bytes,
                    },
                    by_file: reads
                        .files
                        .into_iter()
                        .map(|entry| QueryBenchmarkMetadataFileRead {
                            file: entry.file.filename(),
                            calls: entry.issued.calls,
                            bytes: entry.issued.bytes,
                        })
                        .collect(),
                    by_class: reads
                        .classes
                        .into_iter()
                        .map(|entry| QueryBenchmarkMetadataClassRead {
                            class: metadata_cache_class_name(entry.class),
                            calls: entry.issued.calls,
                            bytes: entry.issued.bytes,
                        })
                        .collect(),
                },
            },
            start_gauges: metadata_runtime_gauges(&before),
            end_gauges: metadata_runtime_gauges(&after),
            lifetime_peaks_after_run: QueryBenchmarkMetadataRuntimeLifetimePeaks {
                cache_class_charges: after
                    .cache
                    .class_charges
                    .into_iter()
                    .map(|charge| QueryBenchmarkMetadataCacheClassLifetimePeak {
                        class: metadata_cache_class_name(charge.class),
                        peak_in_flight_bytes: charge.peak_in_flight_bytes,
                        peak_retained_bytes: charge.peak_retained_bytes,
                    })
                    .collect(),
                governor: QueryBenchmarkMetadataGovernorLifetimePeaks {
                    peak_retained_bytes: after.governor.peak_retained_bytes,
                    peak_in_flight_bytes: after.governor.peak_in_flight_bytes,
                    usage_charges: after
                        .governor
                        .usage
                        .into_iter()
                        .map(|charge| QueryBenchmarkMetadataUsageLifetimePeak {
                            usage: metadata_usage_class_name(charge.usage),
                            peak_in_flight_bytes: charge.peak_in_flight_bytes,
                            peak_retained_bytes: charge.peak_retained_bytes,
                        })
                        .collect(),
                },
                file_manager: QueryBenchmarkMetadataFileManagerLifetimePeaks {
                    peak_open_files: after.files.peak_open_files,
                    peak_occupied_open_slots: after.files.peak_occupied_open_slots,
                    peak_active_open_files: after.files.peak_active_open_files,
                    peak_cached_open_files: after.files.peak_cached_open_files,
                    peak_active_leases: after.files.peak_active_leases,
                    peak_preflighting_files: after.files.peak_preflighting_files,
                },
            },
        }
    }
}

fn metadata_runtime_gauges(
    snapshot: &StoreMetadataRuntimeSnapshot,
) -> QueryBenchmarkMetadataRuntimeGauges {
    QueryBenchmarkMetadataRuntimeGauges {
        cache: QueryBenchmarkMetadataCacheEndGauges {
            resident_entries: snapshot.cache.resident_entries,
            live_allocations: snapshot.cache.live_allocations,
            active_loads: snapshot.cache.active_loads,
            registered_artifacts: snapshot.cache.registered_artifacts,
            ledger_reserved_bytes: snapshot.cache.ledger_reserved_bytes,
            ledger_in_flight_bytes: snapshot.cache.ledger_in_flight_bytes,
            ledger_retained_bytes: snapshot.cache.ledger_retained_bytes,
            sticky_artifacts: snapshot.cache.sticky_artifacts,
            sticky_charged_bytes: snapshot.cache.sticky_charged_bytes,
            class_charges: snapshot
                .cache
                .class_charges
                .iter()
                .map(|charge| QueryBenchmarkMetadataCacheClassEndGauge {
                    class: metadata_cache_class_name(charge.class),
                    in_flight_bytes: charge.in_flight_bytes,
                    retained_bytes: charge.retained_bytes,
                })
                .collect(),
        },
        governor: QueryBenchmarkMetadataGovernorEndGauges {
            retained_max_bytes: snapshot.governor.retained_max_bytes,
            in_flight_max_bytes: snapshot.governor.in_flight_max_bytes,
            retained_bytes: snapshot.governor.retained_bytes,
            in_flight_bytes: snapshot.governor.in_flight_bytes,
            usage_charges: snapshot
                .governor
                .usage
                .iter()
                .map(|charge| QueryBenchmarkMetadataUsageEndGauge {
                    usage: metadata_usage_class_name(charge.usage),
                    in_flight_bytes: charge.in_flight_bytes,
                    retained_bytes: charge.retained_bytes,
                })
                .collect(),
        },
        file_manager: QueryBenchmarkMetadataFileManagerEndGauges {
            max_open_files: snapshot.files.max_open_files,
            max_cached_open_files: snapshot.files.max_cached_open_files,
            open_files: snapshot.files.open_files,
            occupied_open_slots: snapshot.files.occupied_open_slots,
            active_open_files: snapshot.files.active_open_files,
            cached_open_files: snapshot.files.cached_open_files,
            opening_files: snapshot.files.opening_files,
            pending_open_files: snapshot.files.pending_open_files,
            preflighting_files: snapshot.files.preflighting_files,
            closing_files: snapshot.files.closing_files,
            active_leases: snapshot.files.active_leases,
        },
    }
}

fn metadata_cache_class_name(class: MetadataCacheClass) -> &'static str {
    match class {
        MetadataCacheClass::SymbolRoot => "symbol_root",
        MetadataCacheClass::SymbolPage => "symbol_page",
        MetadataCacheClass::IndexRoot => "index_root",
        MetadataCacheClass::IndexDirectory => "index_directory",
        MetadataCacheClass::IndexPage => "index_page",
        MetadataCacheClass::MetricRange => "metric_range",
        MetadataCacheClass::SeriesRoot => "series_root",
        MetadataCacheClass::SeriesHotPage => "series_hot_page",
        MetadataCacheClass::SeriesColdPage => "series_cold_page",
        MetadataCacheClass::OverflowRoot => "overflow_root",
        MetadataCacheClass::OverflowBlob => "overflow_blob",
        MetadataCacheClass::Postings => "postings",
        MetadataCacheClass::FullValidation => "full_validation",
    }
}

fn metadata_usage_class_name(class: MetadataUsageClass) -> &'static str {
    match class {
        MetadataUsageClass::Unclassified => "unclassified",
        MetadataUsageClass::Scratch => "scratch",
        MetadataUsageClass::CorruptionLedger => "corruption_ledger",
        MetadataUsageClass::Cache(class) => metadata_cache_class_name(class),
    }
}
