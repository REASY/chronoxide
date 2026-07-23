use std::io;
use std::path::Path;

use chronoxide_core::storage::manifest::read_manifest_inventory;
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentStoreOpenOptions, SegmentStoreReader,
};

use super::StorageLayoutArg;

#[cfg(test)]
pub(super) fn open_segment_store(
    segments_dir: &Path,
    validate_segment_footers: bool,
    query_projection_config: QueryProjectionConfig,
) -> io::Result<SegmentStoreReader> {
    open_segment_store_for_layout_ab(
        segments_dir,
        validate_segment_footers,
        query_projection_config,
        StorageLayoutArg::Schema8,
    )
}

pub(super) fn open_segment_store_for_layout_ab(
    segments_dir: &Path,
    validate_segment_footers: bool,
    query_projection_config: QueryProjectionConfig,
    storage_layout: StorageLayoutArg,
) -> io::Result<SegmentStoreReader> {
    let manifest_dir = segments_dir.join("manifest");
    let store = if read_manifest_inventory(&manifest_dir)?.is_some() {
        SegmentStoreReader::open_manifest_published_with_options(
            segments_dir,
            &manifest_dir,
            SegmentStoreOpenOptions {
                validate_segment_footers,
                storage_schema_policy: storage_layout.core_policy(),
                ..SegmentStoreOpenOptions::default()
            },
        )
    } else {
        SegmentStoreReader::open_with_options(
            segments_dir,
            SegmentStoreOpenOptions {
                validate_segment_footers,
                storage_schema_policy: storage_layout.core_policy(),
                ..SegmentStoreOpenOptions::default()
            },
        )
    }?;
    Ok(store.with_query_projection_config(query_projection_config))
}

pub(super) fn query_projection_config(
    exponential_histogram_bucket_boundaries: &[f64],
) -> QueryProjectionConfig {
    QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(
        exponential_histogram_bucket_boundaries.to_vec(),
    )
}
