use std::path::PathBuf;

use chronoxide_core::storage::segment::{
    SegmentStorageSchema, verify_experimental_storage_corpus,
    verify_experimental_storage_corpus_with_decoded_semantics,
    verify_experimental_storage_corpus_with_exact_postings,
};
use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Schema {
    Schema6,
    Schema7,
    Schema8,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    segments_dir: PathBuf,
    #[arg(long, value_enum)]
    schema: Schema,
    #[arg(long)]
    validate_segment_footers: bool,
    /// Decode and fingerprint every integrity-checked exact-postings list. This
    /// exhaustive check is available for schema 7 and 8.
    #[arg(long)]
    verify_exact_postings: bool,
    /// Verify this many evenly spaced series from every segment. Omit for an
    /// exhaustive full-corpus decode.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    sample_series_per_segment: Option<u32>,
    /// Additionally fingerprint all decoded logical samples independently of
    /// segment IDs, local refs, chunks, offsets, and in-order/OOO lanes. The
    /// ordered codec/replay semantic fingerprint remains present separately.
    /// Implies --verify-exact-postings and forbids sampling.
    #[arg(long, conflicts_with = "sample_series_per_segment")]
    decoded_semantic_fingerprint: bool,
}

fn main() {
    let args = Args::parse();
    let schema = match args.schema {
        Schema::Schema6 => SegmentStorageSchema::Schema6,
        Schema::Schema7 => SegmentStorageSchema::Schema7,
        Schema::Schema8 => SegmentStorageSchema::Schema8,
    };
    let verification = if args.decoded_semantic_fingerprint {
        verify_experimental_storage_corpus_with_decoded_semantics(
            &args.segments_dir,
            schema,
            args.validate_segment_footers,
        )
    } else if args.verify_exact_postings {
        verify_experimental_storage_corpus_with_exact_postings(
            &args.segments_dir,
            schema,
            args.validate_segment_footers,
            args.sample_series_per_segment,
        )
    } else {
        verify_experimental_storage_corpus(
            &args.segments_dir,
            schema,
            args.validate_segment_footers,
            args.sample_series_per_segment,
        )
    };
    match verification {
        Ok(report) => match serde_json::to_string_pretty(&report) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!("storage verification report encoding failed: {error}");
                std::process::exit(1);
            }
        },
        Err(error) => {
            eprintln!("storage verification failed: {error}");
            std::process::exit(1);
        }
    }
}
