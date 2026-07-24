use std::io::{self, Cursor, Seek, SeekFrom, Write};

use crate::storage::chunk::ChunkIndexRange;
use crate::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueTimeRangeIndex, MetricSeriesRangeIndex,
    SegmentIndexes, SegmentRoutingIndex,
};
use crate::storage::series::{SERIES_KIND_FLOAT, SegmentSymbols, SeriesEntry, SeriesEntryView};

use super::*;

struct Fixture {
    symbols: SegmentSymbols,
    series: Vec<SeriesEntry>,
    indexes: SegmentIndexes,
}

struct CompactSeriesEntry {
    series_id: u64,
    kind_mask: u8,
    labels: Vec<(u32, u32)>,
}

impl SeriesEntryView for CompactSeriesEntry {
    fn series_id(&self) -> u64 {
        self.series_id
    }

    fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    fn labels(&self) -> &[(u32, u32)] {
        &self.labels
    }
}

fn fixture() -> Fixture {
    fixture_for_host_values(&["a".to_string(), "b".to_string()])
}

fn fixture_for_host_values(host_values: &[String]) -> Fixture {
    assert!(!host_values.is_empty());
    let mut unsorted_symbols = SegmentSymbols::default();
    unsorted_symbols.intern("host");
    unsorted_symbols.intern("cpu");
    unsorted_symbols.intern(METRIC_NAME_LABEL);
    for value in host_values {
        unsorted_symbols.intern(value);
    }
    let (symbols, _remap) = unsorted_symbols.sorted_remap().expect("sort symbols");
    let metric_name = symbols.lookup(METRIC_NAME_LABEL).expect("metric label");
    let metric = symbols.lookup("cpu").expect("metric value");
    let host = symbols.lookup("host").expect("host label");

    let mut series = Vec::new();
    for (series_ref, host_value) in host_values.iter().enumerate() {
        let mut labels = vec![
            (metric_name, metric),
            (host, symbols.lookup(host_value).expect("host value")),
        ];
        labels.sort_unstable_by_key(|(name, _value)| *name);
        series.push(SeriesEntry {
            series_id: series_ref as u64 + 100,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: ChunkIndexRange::default(),
            labels,
        });
    }

    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (series_ref, entry) in series.iter().enumerate() {
        let series_ref = u32::try_from(series_ref).expect("small fixture");
        let start = 10 + u64::from(series_ref) * 10;
        for &(name, value) in &entry.labels {
            exact_postings.insert_monotonic(name, value, series_ref);
            label_value_time_ranges.insert(name, value, start, start + 9);
        }
    }
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).expect("build FSTs");
    let metric_series_ranges =
        MetricSeriesRangeIndex::from_series(&series, &symbols, &label_value_time_ranges)
            .expect("build metric ranges");
    let routing_index = Some(
        SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &label_value_time_ranges)
            .expect("build routing"),
    );
    let indexes = SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index,
    };

    Fixture {
        symbols,
        series,
        indexes,
    }
}

fn build_fst(values: &[&str]) -> Vec<u8> {
    let mut values = values.to_vec();
    values.sort_unstable();
    let mut builder = fst::SetBuilder::memory();
    for value in values {
        builder.insert(value).expect("ordered FST value");
    }
    builder.into_inner().expect("finish FST")
}

fn encode(fixture: &Fixture) -> io::Result<Vec<u8>> {
    let mut bytes = Cursor::new(Vec::new());
    write_segment_indexes_v8_for_roots(
        &mut bytes,
        &fixture.indexes,
        u32::try_from(fixture.series.len()).expect("small fixture"),
        &fixture.symbols,
        &fixture.series,
    )?;
    Ok(bytes.into_inner())
}

#[test]
fn same_seal_writer_accepts_complete_authoritative_inventory_deterministically() {
    let fixture = fixture();
    let first = encode(&fixture).expect("encode authorized v8 index");
    let second = encode(&fixture).expect("repeat authorized v8 index");

    assert_eq!(first, second);
    let trailer = &first[first.len() - super::super::TRAILER_LEN..];
    assert_eq!(
        super::super::read_u32(trailer, super::super::TRAILER_SERIES_COUNT_OFFSET),
        fixture.series.len() as u32
    );
    assert_eq!(
        super::super::read_u32(trailer, super::super::TRAILER_SYMBOL_COUNT_OFFSET),
        fixture.symbols.len() as u32
    );
}

#[test]
fn compact_rows_preserve_authenticated_v8_and_v9_bytes() {
    let mut fixture = fixture();
    let compact = fixture
        .series
        .iter()
        .map(|entry| CompactSeriesEntry {
            series_id: entry.series_id,
            kind_mask: entry.kind_mask,
            labels: entry.labels.clone(),
        })
        .collect::<Vec<_>>();

    let expected_v8 = encode(&fixture).unwrap();
    let mut compact_v8 = Cursor::new(Vec::new());
    write_segment_indexes_v8_for_roots(
        &mut compact_v8,
        &fixture.indexes,
        compact.len() as u32,
        &fixture.symbols,
        &compact,
    )
    .unwrap();
    assert_eq!(compact_v8.into_inner(), expected_v8);

    fixture.indexes.routing_index = Some(
        SegmentRoutingIndex::from_indexes_adaptive(
            &fixture.symbols,
            &fixture.indexes.exact_postings,
            &fixture.indexes.label_value_time_ranges,
        )
        .unwrap(),
    );
    let mut expected_v9 = Cursor::new(Vec::new());
    write_segment_indexes_v9_for_roots(
        &mut expected_v9,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .unwrap();
    let mut compact_v9 = Cursor::new(Vec::new());
    write_segment_indexes_v9_for_roots(
        &mut compact_v9,
        &fixture.indexes,
        compact.len() as u32,
        &fixture.symbols,
        &compact,
    )
    .unwrap();
    assert_eq!(compact_v9.into_inner(), expected_v9.into_inner());
}

#[test]
fn malformed_compact_rows_fail_closed_before_v8_or_v9_writes() {
    let mut fixture = fixture();
    let mut compact = fixture
        .series
        .iter()
        .map(|entry| CompactSeriesEntry {
            series_id: entry.series_id,
            kind_mask: entry.kind_mask,
            labels: entry.labels.clone(),
        })
        .collect::<Vec<_>>();
    compact[0].kind_mask = 0;

    let mut v8_sink = CountingSink::default();
    let v8_error = write_segment_indexes_v8_for_roots(
        &mut v8_sink,
        &fixture.indexes,
        compact.len() as u32,
        &fixture.symbols,
        &compact,
    )
    .expect_err("zero-kind compact row must not authorize v8 output");
    assert_eq!(v8_error.kind(), io::ErrorKind::InvalidData);
    assert!(v8_error.to_string().contains("kind mask"));
    assert_eq!(v8_sink.total, 0);

    fixture.indexes.routing_index = Some(
        SegmentRoutingIndex::from_indexes_adaptive(
            &fixture.symbols,
            &fixture.indexes.exact_postings,
            &fixture.indexes.label_value_time_ranges,
        )
        .expect("build adaptive routing"),
    );
    let mut v9_sink = CountingSink::default();
    let v9_error = write_segment_indexes_v9_for_roots(
        &mut v9_sink,
        &fixture.indexes,
        compact.len() as u32,
        &fixture.symbols,
        &compact,
    )
    .expect_err("zero-kind compact row must not authorize v9 output");
    assert_eq!(v9_error.kind(), io::ErrorKind::InvalidData);
    assert!(v9_error.to_string().contains("kind mask"));
    assert_eq!(v9_sink.total, 0);
}

#[test]
fn same_seal_proof_preserves_private_encoder_bytes_exactly() {
    let fixture = fixture();
    let authorized = encode(&fixture).expect("encode authorized v8 index");
    let counts = RootCounts {
        series: fixture.series.len() as u32,
        symbols: fixture.symbols.len() as u32,
    };
    let mut private = Cursor::new(Vec::new());
    super::super::encode_segment_indexes_v8(&mut private, &fixture.indexes, counts)
        .expect("encode structurally validated v8 index");

    assert_eq!(authorized, private.into_inner());
}

#[test]
fn v9_same_seal_writer_requires_adaptive_routing_lengths() {
    let mut fixture = fixture();
    let mut stale_sink = CountingSink::default();
    let error = write_segment_indexes_v9_for_roots(
        &mut stale_sink,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect_err("schema-7 raw routing lengths must not authorize v9 postings");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("routing index metadata"));
    assert_eq!(stale_sink.total, 0);

    fixture.indexes.routing_index = Some(
        SegmentRoutingIndex::from_indexes_adaptive(
            &fixture.symbols,
            &fixture.indexes.exact_postings,
            &fixture.indexes.label_value_time_ranges,
        )
        .expect("build adaptive routing"),
    );
    let mut encoded = Cursor::new(Vec::new());
    write_segment_indexes_v9_for_roots(
        &mut encoded,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect("adaptive routing authorizes v9 postings");
    assert_eq!(
        super::super::read_u16(&encoded.get_ref()[4..], 0),
        super::super::VERSION_V9
    );
}

#[test]
fn same_seal_bytes_match_across_exact_page_boundary() {
    let host_values = (0..342)
        .map(|value| format!("host-{value:03}"))
        .collect::<Vec<_>>();
    let fixture = fixture_for_host_values(&host_values);
    let authorized = encode(&fixture).expect("encode multi-page authorized v8 index");
    let counts = RootCounts {
        series: fixture.series.len() as u32,
        symbols: fixture.symbols.len() as u32,
    };
    let mut private = Cursor::new(Vec::new());
    super::super::encode_segment_indexes_v8(&mut private, &fixture.indexes, counts)
        .expect("encode multi-page structurally validated v8 index");

    assert_eq!(authorized, private.into_inner());
    let trailer = &authorized[authorized.len() - super::super::TRAILER_LEN..];
    assert!(super::super::read_u32(trailer, super::super::TRAILER_EXACT_PAGE_COUNT_OFFSET) > 1);
}

#[test]
fn same_seal_writer_rejects_unresolved_fst_value_before_writing() {
    let mut fixture = fixture();
    let host = fixture.symbols.lookup("host").expect("host label");
    fixture
        .indexes
        .label_values
        .insert_fst(host, build_fst(&["a", "b", "foreign"]));
    let mut sink = CountingSink::default();

    let error = write_segment_indexes_v8_for_roots(
        &mut sink,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect_err("foreign FST value must fail");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("cannot be resolved"));
    assert_eq!(sink.total, 0);
}

#[test]
fn same_seal_writer_rejects_equal_count_different_fst_inventory() {
    let mut fixture = fixture();
    let host = fixture.symbols.lookup("host").expect("host label");
    fixture
        .indexes
        .label_values
        .insert_fst(host, build_fst(&["a", "cpu"]));

    let error = encode(&fixture).expect_err("equal counts do not prove inventory equality");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("FST inventory"));
}

#[test]
fn same_seal_writer_rejects_extra_time_range_key_before_writing() {
    let mut fixture = fixture();
    let host = fixture.symbols.lookup("host").expect("host label");
    let metric = fixture.symbols.lookup("cpu").expect("metric value");
    fixture
        .indexes
        .label_value_time_ranges
        .insert(host, metric, 10, 20);
    let mut sink = CountingSink::default();

    let error = write_segment_indexes_v8_for_roots(
        &mut sink,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect_err("foreign time-range key must fail");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("different key inventories"));
    assert_eq!(sink.total, 0);
}

#[test]
fn same_seal_writer_rejects_substituted_exact_membership() {
    let mut fixture = fixture();
    fixture.indexes.routing_index = None;
    let host = fixture.symbols.lookup("host").expect("host label");
    let a = fixture.symbols.lookup("a").expect("host value a");
    let b = fixture.symbols.lookup("b").expect("host value b");
    fixture
        .indexes
        .exact_postings
        .postings
        .insert((host, a), vec![1]);
    fixture
        .indexes
        .exact_postings
        .postings
        .insert((host, b), vec![0]);

    let error = encode(&fixture).expect_err("same-count membership swap must fail");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("omit series"));
}

#[test]
fn same_seal_writer_rejects_foreign_refs_and_symbols_before_writing() {
    let base = fixture();
    let host = base.symbols.lookup("host").expect("host label");
    let a = base.symbols.lookup("a").expect("host value a");

    let mut foreign_ref = base.clone_fixture();
    foreign_ref
        .indexes
        .exact_postings
        .insert(host, a, foreign_ref.series.len() as u32);
    let mut ref_sink = CountingSink::default();
    let ref_error = write_segment_indexes_v8_for_roots(
        &mut ref_sink,
        &foreign_ref.indexes,
        foreign_ref.series.len() as u32,
        &foreign_ref.symbols,
        &foreign_ref.series,
    )
    .expect_err("foreign ref must fail");
    assert_eq!(ref_error.kind(), io::ErrorKind::InvalidData);
    assert!(ref_error.to_string().contains("series count"));
    assert_eq!(ref_sink.total, 0);

    let mut foreign_symbol = base.clone_fixture();
    foreign_symbol
        .indexes
        .exact_postings
        .insert(foreign_symbol.symbols.len() as u32, a, 0);
    let mut symbol_sink = CountingSink::default();
    let symbol_error = write_segment_indexes_v8_for_roots(
        &mut symbol_sink,
        &foreign_symbol.indexes,
        foreign_symbol.series.len() as u32,
        &foreign_symbol.symbols,
        &foreign_symbol.series,
    )
    .expect_err("foreign symbol must fail");
    assert_eq!(symbol_error.kind(), io::ErrorKind::InvalidData);
    assert!(symbol_error.to_string().contains("symbol count"));
    assert_eq!(symbol_sink.total, 0);
}

#[test]
fn same_seal_writer_propagates_sink_failure() {
    let fixture = fixture();

    let error = write_segment_indexes_v8_for_roots(
        FailingSink,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect_err("sink failure must propagate");

    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(error.to_string().contains("injected v8 sink failure"));
}

#[test]
fn same_seal_writer_bounds_individual_sink_writes() {
    let large_value = "v".repeat(super::super::OUTPUT_BUFFER_LEN * 3);
    let fixture = fixture_for_host_values(&[large_value]);
    let mut sink = CountingSink::default();

    write_segment_indexes_v8_for_roots(
        &mut sink,
        &fixture.indexes,
        fixture.series.len() as u32,
        &fixture.symbols,
        &fixture.series,
    )
    .expect("write large authorized FST");

    assert!(sink.total > super::super::OUTPUT_BUFFER_LEN);
    assert!(sink.max_write <= super::super::OUTPUT_BUFFER_LEN);
}

impl Fixture {
    fn clone_fixture(&self) -> Self {
        Self {
            symbols: self.symbols.clone(),
            series: self.series.clone(),
            indexes: self.indexes.clone(),
        }
    }
}

#[derive(Default)]
struct CountingSink {
    total: usize,
    max_write: usize,
    position: u64,
}

impl Write for CountingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.total = self
            .total
            .checked_add(bytes.len())
            .expect("fixture byte count");
        self.max_write = self.max_write.max(bytes.len());
        self.position = self
            .position
            .checked_add(bytes.len() as u64)
            .expect("fixture position");
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for CountingSink {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let SeekFrom::Start(position) = position else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "counting sink supports only absolute seeks",
            ));
        };
        self.position = position;
        Ok(position)
    }
}

struct FailingSink;

impl Seek for FailingSink {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        match position {
            SeekFrom::Start(position) => Ok(position),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "failing sink supports only absolute seeks",
            )),
        }
    }
}

impl Write for FailingSink {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "injected v8 sink failure",
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
