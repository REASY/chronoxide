use std::fs;
use std::io;
use std::path::PathBuf;

use tempfile::TempDir;
use ulid::Ulid;

use super::*;
use crate::storage::metadata_cache::MetadataCacheError;
use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataUsageClass};
use crate::storage::segment::write_segment_footer_for_schema;

struct SegmentFixture {
    _root: TempDir,
    dir: PathBuf,
    meta: SegmentMeta,
}

fn runtime(max_open_files: u32) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes: 0,
        in_flight_max_bytes: 2 * 1024 * 1024,
        max_open_files,
        max_cached_open_files: 0,
    })
    .expect("valid full-validation runtime")
}

fn segment_fixture(schema_version: u16, meta_override: Option<SegmentMeta>) -> SegmentFixture {
    let root = TempDir::new().expect("create full-validation fixture root");
    let id = SegmentId::with_ulid(
        100,
        200,
        Ulid::from(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef_u128),
    )
    .expect("valid segment id");
    let dir = root.path().join(id.dir_name());
    fs::create_dir(&dir).expect("create canonical segment directory");
    let meta = meta_override.unwrap_or_else(|| SegmentMeta {
        segment_id: id.dir_name(),
        start_ms: id.start_ms(),
        end_ms: id.end_ms(),
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    });

    for file in SEGMENT_FOOTER_TRACKED_FILES {
        let bytes = if file == SegmentFile::MetaJson {
            serde_json::to_vec(&meta).expect("encode fixture meta")
        } else {
            format!("registered-full-validation:{}", file.filename()).into_bytes()
        };
        fs::write(dir.join(file.filename()), bytes).expect("write tracked fixture artifact");
    }
    write_segment_footer_for_schema(&dir, schema_version).expect("write fixture footer");

    SegmentFixture {
        _root: root,
        dir,
        meta,
    }
}

fn assert_structural(error: RegisteredSegmentValidationError, expected: &str) {
    let RegisteredSegmentValidationError::Cache(MetadataCacheError::Structural(corruption)) = error
    else {
        panic!("expected structural cache error, got {error:?}");
    };
    assert!(
        corruption.message.contains(expected),
        "unexpected corruption: {corruption}"
    );
}

#[test]
fn registered_schema7_preflight_checksums_the_captured_generation() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);

    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight schema 7 fixture");
    let validated = preflight
        .validate_footer_checksums()
        .expect("validate registered fixture");

    assert_eq!(validated.footer.schema_version, SEGMENT_SCHEMA_VERSION_V7);
    assert_eq!(
        validated.segment_id,
        SegmentId::parse_dir_name(&fixture.meta.segment_id).unwrap()
    );
    assert_eq!(validated.meta, fixture.meta);
    assert_eq!(validated.policy, RegisteredSegmentValidationPolicy::Schema7);
    assert!(validated.matches_registered_generation());

    let snapshot = runtime.snapshot();
    assert_eq!(
        snapshot
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    assert_eq!(snapshot.files.open_files, 0);
    assert_eq!(snapshot.files.peak_open_files, 1);
    assert_eq!(snapshot.reads.unclassified.calls, 0);
    assert_eq!(
        snapshot.reads.issued.calls,
        u64::try_from(SEGMENT_FOOTER_TRACKED_FILES.len()).unwrap() + 1
    );
}

#[test]
fn registered_schema8_preflight_accepts_only_schema8_footer() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V8, None);
    let runtime = runtime(1);

    let validated = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema8,
    )
    .expect("preflight schema 8 fixture")
    .validate_footer_checksums()
    .expect("validate schema 8 fixture");

    assert_eq!(validated.footer.schema_version, SEGMENT_SCHEMA_VERSION_V8);
    assert_eq!(validated.policy, RegisteredSegmentValidationPolicy::Schema8);
}

#[test]
fn lightweight_registered_meta_read_touches_only_the_captured_meta_file() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let expected_meta_bytes = fs::metadata(fixture.dir.join(SegmentFile::MetaJson.filename()))
        .expect("stat fixture meta")
        .len();
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight schema 7 fixture");

    let (registered, footer, meta) = preflight
        .read_registered_meta()
        .expect("read registered meta without full checksums");

    assert_eq!(registered.segment_identity(), fixture.meta.segment_id);
    assert_eq!(footer.schema_version, SEGMENT_SCHEMA_VERSION_V7);
    assert_eq!(meta, fixture.meta);
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.reads.issued.calls, 1);
    assert_eq!(snapshot.reads.issued.bytes, expected_meta_bytes);
    assert_eq!(snapshot.reads.unclassified, snapshot.reads.issued);
    assert_eq!(
        snapshot.reads.classes[MetadataCacheClass::FullValidation.stable_index()]
            .issued
            .calls,
        0
    );
    assert_eq!(snapshot.files.open_files, 0);
}

#[test]
fn exact_schema_policy_rejects_the_other_footer_before_registration() {
    let schema6 = segment_fixture(SEGMENT_SCHEMA_VERSION_V6, None);
    let schema7 = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let schema8 = segment_fixture(SEGMENT_SCHEMA_VERSION_V8, None);
    let runtime = runtime(1);

    let schema6_error = preflight_registered_segment(
        &runtime,
        &schema6.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .err()
    .expect("schema 7 policy must reject schema 6");
    let schema7_error = preflight_registered_segment(
        &runtime,
        &schema7.dir,
        RegisteredSegmentValidationPolicy::ValidatedSchema6,
    )
    .err()
    .expect("schema 6 policy must reject schema 7");
    let schema8_error = preflight_registered_segment(
        &runtime,
        &schema8.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .err()
    .expect("schema 7 policy must reject schema 8");
    let schema7_as_schema8_error = preflight_registered_segment(
        &runtime,
        &schema7.dir,
        RegisteredSegmentValidationPolicy::Schema8,
    )
    .err()
    .expect("schema 8 policy must reject schema 7");

    for error in [
        schema6_error,
        schema7_error,
        schema8_error,
        schema7_as_schema8_error,
    ] {
        let RegisteredSegmentValidationError::Io(error) = error else {
            panic!("expected footer I/O error, got {error:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("unsupported segment footer schema")
        );
    }
    assert_eq!(runtime.snapshot().files.preflight_calls, 0);
}

#[test]
fn checksum_corruption_is_sticky_and_retry_performs_no_more_reads() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight schema 7 fixture");
    let registered = preflight.registered().clone();
    let symbols_path = fixture.dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).expect("read fixture symbols");
    symbols[0] ^= 0xff;
    fs::write(&symbols_path, symbols).expect("corrupt fixture symbols in place");

    let first = preflight
        .validate_footer_checksums()
        .err()
        .expect("checksum corruption must fail");
    assert_structural(first, "checksum mismatch");
    let reads_after_first = runtime.snapshot().reads.issued;

    let retry = registered
        .reader(SegmentFile::Symbols)
        .expect("read retained registered generation")
        .check_recorded_error()
        .expect_err("sticky corruption must gate retry");
    let MetadataCacheError::Structural(corruption) = retry else {
        panic!("expected sticky structural error, got {retry:?}");
    };
    assert!(corruption.message.contains("checksum mismatch"));
    assert_eq!(runtime.snapshot().reads.issued, reads_after_first);
}

#[test]
fn same_length_path_replacement_is_rejected_before_hashing_new_bytes() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight schema 7 fixture");
    let symbols_path = fixture.dir.join(SegmentFile::Symbols.filename());
    let replacement_path = fixture.dir.join("replacement.tmp");
    let replacement = vec![b'x'; fs::metadata(&symbols_path).unwrap().len() as usize];
    fs::write(&replacement_path, replacement).expect("write same-length replacement");
    fs::rename(&replacement_path, &symbols_path).expect("replace registered path");

    let error = preflight
        .validate_footer_checksums()
        .err()
        .expect("registered identity replacement must fail");
    assert_structural(error, "replacement");
    assert_eq!(runtime.snapshot().files.structural_replacements, 1);
}

#[test]
fn checksummed_meta_must_match_the_canonical_directory_identity() {
    let wrong_meta = SegmentMeta {
        segment_id: "seg-100-201-00000000000000000000000000".to_owned(),
        start_ms: 100,
        end_ms: 201,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, Some(wrong_meta));
    let runtime = runtime(1);
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight fixture with checksummed mismatched meta");

    let error = preflight
        .validate_footer_checksums()
        .err()
        .expect("mismatched meta identity must fail");
    assert_structural(error, "does not match its directory identity");
}

#[test]
fn lightweight_registered_meta_read_records_identity_corruption() {
    let wrong_meta = SegmentMeta {
        segment_id: "seg-100-201-00000000000000000000000000".to_owned(),
        start_ms: 100,
        end_ms: 201,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, Some(wrong_meta));
    let runtime = runtime(1);
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("preflight fixture with mismatched meta");
    let retained_generation = preflight.registered().clone();

    let error = preflight
        .read_registered_meta()
        .err()
        .expect("ordinary registered meta read must validate directory identity");

    assert_structural(error, "does not match its directory identity");
    assert_eq!(runtime.snapshot().cache.sticky_artifacts, 1);
    let retained_guard = retained_generation
        .read_guard()
        .expect("retained generation remains readable");
    let sticky_error = retained_guard
        .reader(SegmentFile::MetaJson)
        .expect("registered meta reader")
        .check_recorded_error()
        .err()
        .expect("identity corruption must remain sticky");
    assert!(matches!(sticky_error, MetadataCacheError::Structural(_)));
}

#[cfg(unix)]
#[test]
fn footer_preflight_does_not_follow_symlinks() {
    use std::os::unix::fs::symlink;

    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let footer_path = fixture.dir.join(SegmentFile::Footer.filename());
    let target = fixture.dir.join("footer-target.bin");
    fs::rename(&footer_path, &target).expect("move canonical footer");
    symlink(&target, &footer_path).expect("replace footer with symlink");

    let error = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .err()
    .expect("symlinked footer must fail");
    let RegisteredSegmentValidationError::Io(error) = error else {
        panic!("expected immutable-open I/O error, got {error:?}");
    };
    assert_ne!(error.kind(), io::ErrorKind::NotFound);
    assert_eq!(runtime.snapshot().files.preflight_calls, 0);
}

#[test]
fn noncanonical_directory_name_is_rejected_before_artifact_registration() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let noncanonical = fixture._root.path().join("segment-not-canonical");
    fs::rename(&fixture.dir, &noncanonical).expect("rename fixture directory");

    let error = preflight_registered_segment(
        &runtime,
        &noncanonical,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .err()
    .expect("noncanonical directory identity must fail");
    let RegisteredSegmentValidationError::Io(error) = error else {
        panic!("expected identity I/O error, got {error:?}");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("invalid segment directory identity")
    );
    assert_eq!(runtime.snapshot().files.preflight_calls, 0);
}

#[test]
fn oversized_meta_is_rejected_before_artifact_registration() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = runtime(1);
    let meta_path = fixture.dir.join(SegmentFile::MetaJson.filename());
    let mut bytes = fs::read(&meta_path).expect("read fixture meta");
    bytes.resize(usize::try_from(SEGMENT_META_MAX_BYTES).unwrap() + 1, b' ');
    fs::write(&meta_path, bytes).expect("write oversized meta");
    write_segment_footer_for_schema(&fixture.dir, SEGMENT_SCHEMA_VERSION_V7)
        .expect("rewrite footer for oversized meta");

    let error = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .err()
    .expect("oversized meta must fail");
    let RegisteredSegmentValidationError::Io(error) = error else {
        panic!("expected meta limit I/O error, got {error:?}");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("operational limit"));
    assert_eq!(runtime.snapshot().files.preflight_calls, 0);
}

#[test]
fn insufficient_scratch_budget_releases_registration_and_io_resources() {
    let fixture = segment_fixture(SEGMENT_SCHEMA_VERSION_V7, None);
    let runtime = StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes: 0,
        in_flight_max_bytes: 512 * 1024,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid constrained runtime");
    let preflight = preflight_registered_segment(
        &runtime,
        &fixture.dir,
        RegisteredSegmentValidationPolicy::Schema7,
    )
    .expect("registration fits below the hash scratch requirement");

    let error = preflight
        .validate_footer_checksums()
        .err()
        .expect("one MiB checksum scratch must exceed the configured budget");
    assert!(matches!(error, RegisteredSegmentValidationError::Budget(_)));

    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.governor.in_flight_bytes, 0);
    assert_eq!(snapshot.governor.retained_bytes, 0);
    assert_eq!(snapshot.files.open_files, 0);
    assert_eq!(snapshot.files.active_leases, 0);
}

#[test]
fn transient_meta_reader_error_keeps_its_retryable_io_kind() {
    struct RetryableReader;

    impl io::Read for RetryableReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "injected retryable metadata read",
            ))
        }
    }

    let serde_error = serde_json::from_reader::<_, SegmentMeta>(RetryableReader)
        .expect_err("injected reader must fail");
    let error = registered_meta_decode_error(serde_error);

    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(
        error
            .to_string()
            .contains("injected retryable metadata read")
    );
}
