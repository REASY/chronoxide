use std::fs;

use tempfile::TempDir;

use crate::storage::metadata_governor::MetadataGovernorConfig;
use crate::storage::metadata_runtime::{SegmentArtifactRegistration, StoreMetadataRuntime};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;

use super::super::write_symbols_bin_v3;
use super::*;

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: RegisteredSegment,
    reader: GovernedSymbolReader,
}

fn fixture(
    identity: &str,
    values: &[&str],
    retained_max_bytes: u64,
    in_flight_max_bytes: u64,
    corrupt_page: bool,
) -> Result<Fixture, GovernedSymbolReaderError> {
    let directory = tempfile::tempdir().unwrap();
    let mut symbols = Vec::new();
    write_symbols_bin_v3(&mut symbols, values).unwrap();
    if corrupt_page {
        let descriptor_offset = SYMBOLS_V3_HEADER_LEN;
        let page_offset = u64::from_le_bytes(
            symbols[descriptor_offset + 8..descriptor_offset + 16]
                .try_into()
                .unwrap(),
        );
        symbols[usize::try_from(page_offset).unwrap()] ^= 0xff;
    }
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        let bytes = if file == SegmentFile::Symbols {
            symbols.as_slice()
        } else {
            &[]
        };
        fs::write(directory.path().join(file.filename()), bytes).unwrap();
    }
    let runtime = StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .unwrap();
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        SegmentArtifactRegistration::new(file, path.clone(), fs::metadata(path).unwrap().len())
    });
    let registered = runtime.register_segment(identity, &artifacts)?;
    let reader = GovernedSymbolReader::open(&registered)?;
    Ok(Fixture {
        _directory: directory,
        runtime,
        registered,
        reader,
    })
}

fn symbols_issued(runtime: &StoreMetadataRuntime) -> (u64, u64) {
    let stats = runtime.snapshot().reads;
    let file = stats
        .files
        .iter()
        .find(|entry| entry.file == SegmentFile::Symbols)
        .unwrap();
    (file.issued.calls, file.issued.bytes)
}

#[test]
fn governed_symbols_match_scalar_and_batched_v3_semantics() {
    let fixture = fixture(
        "symbols-roundtrip",
        &["", "__name__", "alpha", "omega"],
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    assert_eq!(fixture.reader.segment_identity(), "symbols-roundtrip");
    assert_eq!(fixture.reader.len(), 4);
    assert!(!fixture.reader.is_empty());

    let session = fixture.reader.query_session().unwrap();
    assert_eq!(session.len(), 4);
    assert!(!session.is_empty());
    assert_eq!(session.lookup("alpha").unwrap(), Some(2));
    assert_eq!(session.lookup("missing").unwrap(), None);
    let batch = session
        .lookup_many(&["omega", "", "missing", "alpha", "alpha"])
        .unwrap();
    assert_eq!(batch.values(), &[Some(3), Some(0), None, Some(2), Some(2)]);
    assert_eq!(
        batch.charged_bytes(),
        u64::try_from(batch.values.capacity() * std::mem::size_of::<Option<u32>>()).unwrap()
    );

    let mut resolved = vec![String::new(); 3];
    assert!(
        session
            .visit_resolved_many(&[3, 0, 2], |index, value| {
                resolved[index] = value.to_string();
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(resolved, ["omega", "", "alpha"]);
    let stats = session.logical_stats();
    assert_eq!(stats.returned_values, 8);
    assert_eq!(stats.returned_utf8_bytes, 30);
}

#[test]
fn required_scalar_visit_borrows_value_and_keeps_callback_errors_transient() {
    let fixture = fixture(
        "symbols-required-scalar",
        &["alpha", "beta"],
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    let session = fixture.reader.query_session().unwrap();

    let callback_error = session
        .visit_required_resolved(0, |value| {
            assert_eq!(value, "alpha");
            Err(io::Error::other("stop visiting"))
        })
        .unwrap_err();
    match callback_error {
        GovernedSymbolReaderError::Planning(error) => {
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(error.to_string(), "stop visiting");
        }
        other => panic!("unexpected callback error: {other}"),
    }
    assert_eq!(
        session.logical_stats(),
        GovernedSymbolLogicalStats::default()
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let mut visited = false;
    session
        .visit_required_resolved(1, |value| {
            visited = true;
            assert_eq!(value, "beta");
            Ok(())
        })
        .unwrap();
    assert!(visited);
    assert_eq!(
        session.logical_stats(),
        GovernedSymbolLogicalStats {
            returned_values: 1,
            returned_utf8_bytes: 4,
        }
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn required_out_of_range_id_is_sticky_without_page_io_or_callback() {
    let fixture = fixture(
        "symbols-required-invalid",
        &["alpha", "beta"],
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    let session = fixture.reader.query_session().unwrap();
    let before = symbols_issued(&fixture.runtime);
    let mut visited = false;

    let error = session
        .visit_required_resolved(2, |_| {
            visited = true;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(
        error,
        GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert!(!visited);
    assert_eq!(symbols_issued(&fixture.runtime), before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    let repeated = session.visit_required_resolved(2, |_| Ok(())).unwrap_err();
    assert!(matches!(
        repeated,
        GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(symbols_issued(&fixture.runtime), before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn symbol_session_rejects_a_foreign_generation_without_io_or_poisoning() {
    let first = fixture(
        "symbols-generation-first",
        &["alpha"],
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    let second = fixture(
        "symbols-generation-second",
        &["alpha"],
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    let session = first.reader.query_session().unwrap();
    let own_guard = first.registered.read_guard().unwrap();
    let foreign_guard = second.registered.read_guard().unwrap();
    let before_first = symbols_issued(&first.runtime);
    let before_second = symbols_issued(&second.runtime);

    session.ensure_same_generation(&own_guard).unwrap();
    assert!(matches!(
        session.ensure_same_generation(&foreign_guard),
        Err(GovernedSymbolReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(symbols_issued(&first.runtime), before_first);
    assert_eq!(symbols_issued(&second.runtime), before_second);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn empty_dictionary_resolves_without_page_io() {
    let fixture = fixture("symbols-empty", &[], 1024 * 1024, 1024 * 1024, false).unwrap();
    assert!(fixture.reader.is_empty());
    let before = symbols_issued(&fixture.runtime);
    let session = fixture.reader.query_session().unwrap();
    assert!(session.is_empty());
    assert_eq!(session.lookup("missing").unwrap(), None);
    assert_eq!(session.lookup_many(&["missing"]).unwrap().values(), &[None]);
    assert!(!session.visit_resolved_many(&[0], |_, _| Ok(())).unwrap());
    assert!(session.visit_resolved_many(&[], |_, _| Ok(())).unwrap());
    assert_eq!(symbols_issued(&fixture.runtime), before);
    assert_eq!(
        session.logical_stats(),
        GovernedSymbolLogicalStats::default()
    );
}

#[test]
fn cross_page_batches_preserve_request_order_and_load_each_page_once() {
    let values = (0..600)
        .map(|index| format!("symbol-{index:04}-{}", "x".repeat(96)))
        .collect::<Vec<_>>();
    let value_refs = values.iter().map(String::as_str).collect::<Vec<_>>();
    let fixture = fixture(
        "symbols-cross-page",
        &value_refs,
        1024 * 1024,
        1024 * 1024,
        false,
    )
    .unwrap();
    let session = fixture.reader.query_session().unwrap();
    assert!(session.root.descriptors.len() > 1);
    let second_page_id = session.root.descriptors[1].first_symbol_id;
    let second_page_index = usize::try_from(second_page_id).unwrap();
    let before = symbols_issued(&fixture.runtime);

    let batch = session
        .lookup_many(&[
            values[second_page_index].as_str(),
            values[0].as_str(),
            values[second_page_index].as_str(),
            "!before-first",
            values[0].as_str(),
        ])
        .unwrap();
    assert_eq!(
        batch.values(),
        &[
            Some(second_page_id),
            Some(0),
            Some(second_page_id),
            None,
            Some(0),
        ]
    );
    assert_eq!(symbols_issued(&fixture.runtime).0 - before.0, 2);

    let before_resolve = symbols_issued(&fixture.runtime);
    let mut resolved = vec![String::new(); 4];
    assert!(
        session
            .visit_resolved_many(&[second_page_id, 0, second_page_id, 0], |slot, value| {
                resolved[slot] = value.to_owned();
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(
        resolved,
        [
            values[second_page_index].clone(),
            values[0].clone(),
            values[second_page_index].clone(),
            values[0].clone(),
        ]
    );
    assert_eq!(symbols_issued(&fixture.runtime), before_resolve);
}

#[test]
fn zero_retention_batches_one_page_into_one_physical_read() {
    let fixture = fixture(
        "symbols-zero-retention",
        &["alpha", "beta", "gamma"],
        0,
        1024 * 1024,
        false,
    )
    .unwrap();
    let before = symbols_issued(&fixture.runtime);
    let session = fixture.reader.query_session().unwrap();
    let after_root = symbols_issued(&fixture.runtime);
    assert_eq!(after_root.0 - before.0, 1);
    let batch = session
        .lookup_many(&["alpha", "beta", "gamma", "alpha"])
        .unwrap();
    assert_eq!(batch.values(), &[Some(0), Some(1), Some(2), Some(0)]);
    let after = symbols_issued(&fixture.runtime);
    assert_eq!(after.0 - after_root.0, 1);
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
}

#[test]
fn touched_page_corruption_is_sticky_across_eviction() {
    let fixture = fixture(
        "symbols-corrupt-page",
        &["alpha", "beta"],
        1024 * 1024,
        1024 * 1024,
        true,
    )
    .unwrap();
    let session = fixture.reader.query_session().unwrap();
    let error = session.lookup("alpha").unwrap_err();
    assert!(matches!(
        error,
        GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_first = symbols_issued(&fixture.runtime);
    fixture.runtime.evict_all_resident_metadata();
    let repeated = session.lookup("alpha").unwrap_err();
    assert!(matches!(
        repeated,
        GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(symbols_issued(&fixture.runtime), after_first);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn tiny_in_flight_budget_refuses_root_before_suffix_io_without_poisoning() {
    let directory = tempfile::tempdir().unwrap();
    let mut symbols = Vec::new();
    write_symbols_bin_v3(&mut symbols, ["alpha", "beta"]).unwrap();
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            directory.path().join(file.filename()),
            if file == SegmentFile::Symbols {
                symbols.as_slice()
            } else {
                &[]
            },
        )
        .unwrap();
    }
    const IN_FLIGHT_LIMIT: u64 = 4096;
    let runtime = StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes: 1024 * 1024,
        in_flight_max_bytes: IN_FLIGHT_LIMIT,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .unwrap();
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        SegmentArtifactRegistration::new(file, path.clone(), fs::metadata(path).unwrap().len())
    });
    let registered = runtime
        .register_segment("symbols-tiny", &artifacts)
        .unwrap();
    let _blocker = runtime
        .governor()
        .reserve_in_flight_for_usage(
            IN_FLIGHT_LIMIT - SYMBOLS_V3_HEADER_LEN as u64,
            MetadataUsageClass::Scratch,
        )
        .unwrap();
    let error = GovernedSymbolReader::open(&registered)
        .err()
        .expect("tiny budget must reject root loading");
    assert!(matches!(
        error,
        GovernedSymbolReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(symbols_issued(&runtime), (1, SYMBOLS_V3_HEADER_LEN as u64));
    assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn reader_owner_keeps_registration_alive_until_drop() {
    let fixture = fixture("symbols-owner", &["alpha"], 1024 * 1024, 1024 * 1024, false).unwrap();
    let Fixture {
        _directory,
        runtime,
        registered,
        reader,
    } = fixture;
    drop(registered);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
    drop(reader);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
}
