use chronoxide_core::storage::segment::{
    DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES, RangeScalarCacheGovernorStats,
    configure_range_scalar_cache_governor, range_scalar_cache_governor_stats,
};

#[test]
fn reading_stats_does_not_prevent_later_governor_configuration() {
    assert_eq!(
        range_scalar_cache_governor_stats(),
        RangeScalarCacheGovernorStats {
            limit_bytes: DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
            current_leased_bytes: 0,
            peak_leased_bytes: 0,
        }
    );

    let configured_limit = DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES / 2;
    assert_eq!(
        configure_range_scalar_cache_governor(configured_limit),
        Ok(())
    );
    assert_eq!(
        range_scalar_cache_governor_stats(),
        RangeScalarCacheGovernorStats {
            limit_bytes: configured_limit,
            current_leased_bytes: 0,
            peak_leased_bytes: 0,
        }
    );
}
