use std::{io, net::SocketAddr, path::PathBuf};

use chronoxide_api::{ApiConfig, StoreOpenConfig, open_store, parse_chunk_read_mode, router};
use chronoxide_core::storage::{
    io::ChunkReadConfig,
    segment::{
        PRODUCTION_QUERY_MAX_BYTES_READ, PRODUCTION_QUERY_MAX_CHUNKS_READ,
        PRODUCTION_QUERY_MAX_PROJECTED_SERIES, PRODUCTION_QUERY_MAX_SAMPLES,
        PRODUCTION_QUERY_MAX_SERIES_MATCHED, PRODUCTION_REGEX_MAX_EXPANDED_VALUES, QueryLimits,
        QueryProjectionConfig, SegmentStoreSchemaPolicy,
    },
};
use clap::{Parser, ValueEnum};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Serve sealed Chronoxide segments through the Prometheus HTTP API")]
struct Args {
    #[arg(long)]
    segments_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:9091")]
    listen: SocketAddr,
    #[arg(long, default_value = "auto", value_parser = parse_chunk_read_mode)]
    chunk_read_mode: chronoxide_core::storage::io::ChunkReadMode,
    #[arg(long, default_value_t = 256)]
    chunk_read_queue_depth: u32,
    #[arg(long)]
    experimental_cross_segment_chunk_reads: bool,
    #[arg(long, default_value_t = PRODUCTION_QUERY_MAX_SERIES_MATCHED)]
    query_max_series_matched: u64,
    #[arg(long, default_value_t = PRODUCTION_QUERY_MAX_PROJECTED_SERIES)]
    query_max_projected_series: u64,
    #[arg(long, default_value_t = PRODUCTION_QUERY_MAX_CHUNKS_READ)]
    query_max_chunks_read: u64,
    #[arg(long, default_value_t = PRODUCTION_QUERY_MAX_BYTES_READ)]
    query_max_bytes_read: u64,
    #[arg(long, default_value_t = PRODUCTION_QUERY_MAX_SAMPLES)]
    query_max_samples: u64,
    #[arg(long, default_value_t = PRODUCTION_REGEX_MAX_EXPANDED_VALUES)]
    query_max_regex_values_examined: u64,
    #[arg(long, default_value_t = chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES)]
    range_scalar_cache_max_bytes: u64,
    #[arg(long, default_value_t = default_concurrency())]
    max_concurrent_queries: usize,
    #[arg(long)]
    validate_segment_footers: bool,
    #[arg(
        long,
        value_enum,
        default_value_t = StorageSchemaArg::Schema8,
        help = "Exact sealed-segment schema required for the complete corpus"
    )]
    storage_schema: StorageSchemaArg,
    #[arg(long = "exponential-histogram-bucket-boundary")]
    exponential_histogram_bucket_boundaries: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum StorageSchemaArg {
    Schema7,
    Schema8,
}

impl StorageSchemaArg {
    const fn policy(self) -> SegmentStoreSchemaPolicy {
        match self {
            Self::Schema7 => SegmentStoreSchemaPolicy::StrictSchema7,
            Self::Schema8 => SegmentStoreSchemaPolicy::StrictSchema8,
        }
    }
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let projection = QueryProjectionConfig::default()
        .with_exponential_histogram_bucket_boundaries(args.exponential_histogram_bucket_boundaries);
    let store = open_store(
        &args.segments_dir,
        StoreOpenConfig {
            validate_segment_footers: args.validate_segment_footers,
            storage_schema_policy: args.storage_schema.policy(),
            query_projection_config: projection,
        },
    )?;
    let app = router(
        store,
        ApiConfig {
            query_limits: QueryLimits {
                max_matched_series: Some(args.query_max_series_matched),
                max_projected_series: Some(args.query_max_projected_series),
                max_chunk_reads: Some(args.query_max_chunks_read),
                max_bytes_read: Some(args.query_max_bytes_read),
                max_samples_decoded: Some(args.query_max_samples),
                max_regex_values_examined: Some(args.query_max_regex_values_examined),
            },
            chunk_read_config: ChunkReadConfig {
                mode: args.chunk_read_mode,
                queue_depth: args.chunk_read_queue_depth,
            },
            experimental_cross_segment_chunk_reads: args.experimental_cross_segment_chunk_reads,
            range_scalar_cache_max_bytes: args.range_scalar_cache_max_bytes,
            max_concurrent_queries: args.max_concurrent_queries,
        },
    )?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    info!(listen = %args.listen, segments_dir = %args.segments_dir.display(), "Chronoxide Prometheus API ready");
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_to_schema8_and_retains_explicit_schema7() {
        let defaults = Args::parse_from([
            "chronoxide-api",
            "--segments-dir",
            "/tmp/chronoxide-api-schema-test",
        ]);
        assert_eq!(defaults.storage_schema, StorageSchemaArg::Schema8);
        assert_eq!(
            defaults.storage_schema.policy(),
            SegmentStoreSchemaPolicy::StrictSchema8
        );

        let schema7 = Args::parse_from([
            "chronoxide-api",
            "--segments-dir",
            "/tmp/chronoxide-api-schema-test",
            "--storage-schema",
            "schema7",
        ]);
        assert_eq!(schema7.storage_schema, StorageSchemaArg::Schema7);
        assert_eq!(
            schema7.storage_schema.policy(),
            SegmentStoreSchemaPolicy::StrictSchema7
        );
    }
}
