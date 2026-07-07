use std::io::Cursor;

use chronoxide_core::storage::chunk::ChunkIndexRange;
use chronoxide_core::storage::series::{
    SERIES_KIND_FLOAT, SegmentSymbols, SeriesEntry, SeriesReader, read_series_bin,
    read_symbols_bin, write_series_bin, write_symbols_bin,
};

#[test]
fn symbols_bin_roundtrips_segment_local_strings() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("__name__");
    let pod_value = symbols.intern("backend-1");
    let metric = symbols.intern("cpu_usage_seconds_total");
    let pod = symbols.intern("pod");

    assert_eq!(name, symbols.intern("__name__"));
    assert_eq!(symbols.lookup("pod"), Some(pod));
    assert_eq!(symbols.resolve(metric), Some("cpu_usage_seconds_total"));

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();

    let restored = read_symbols_bin(&mut Cursor::new(bytes)).unwrap();
    assert_eq!(restored.lookup("__name__"), Some(name));
    assert_eq!(restored.resolve(pod), Some("pod"));
    assert_eq!(restored.resolve(pod_value), Some("backend-1"));
    assert_eq!(restored.len(), 4);
}

#[test]
fn series_bin_v2_roundtrips_keyset_encoded_series_entries() {
    let mut symbols = SegmentSymbols::default();
    let name_key = symbols.intern("__name__");
    let name_val = symbols.intern("cpu_usage_seconds_total");
    let namespace_key = symbols.intern("namespace");
    let namespace_val = symbols.intern("default");
    let pod_key = symbols.intern("pod");
    let backend_1 = symbols.intern("backend-1");
    let backend_2 = symbols.intern("backend-2");

    let entries = vec![
        SeriesEntry {
            series_id: 0x1111,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: ChunkIndexRange {
                offset: 128,
                len: 40,
            },
            labels: vec![
                (name_key, name_val),
                (namespace_key, namespace_val),
                (pod_key, backend_1),
            ],
        },
        SeriesEntry {
            series_id: 0x2222,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: ChunkIndexRange {
                offset: 168,
                len: 80,
            },
            labels: vec![
                (name_key, name_val),
                (namespace_key, namespace_val),
                (pod_key, backend_2),
            ],
        },
    ];

    let mut bytes = Vec::new();
    write_series_bin(&mut bytes, &entries).unwrap();

    assert_eq!(&bytes[0..4], b"SERI");
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 2);

    let restored = read_series_bin(&mut Cursor::new(bytes)).unwrap();
    assert_eq!(restored, entries);
}

#[test]
fn series_reader_fetches_single_entry_by_series_ref() {
    let mut symbols = SegmentSymbols::default();
    let name_key = symbols.intern("__name__");
    let cpu = symbols.intern("cpu_usage_seconds_total");
    let memory = symbols.intern("memory_usage_bytes");
    let namespace_key = symbols.intern("namespace");
    let default = symbols.intern("default");
    let infra = symbols.intern("infra");
    let pod_key = symbols.intern("pod");
    let backend = symbols.intern("backend-1");
    let frontend = symbols.intern("frontend-1");

    let entries = vec![
        SeriesEntry {
            series_id: 0x1111,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![
                (name_key, cpu),
                (namespace_key, default),
                (pod_key, backend),
            ],
        },
        SeriesEntry {
            series_id: 0x2222,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![
                (name_key, memory),
                (namespace_key, infra),
                (pod_key, frontend),
            ],
        },
    ];

    let mut bytes = Vec::new();
    write_series_bin(&mut bytes, &entries).unwrap();
    let mut reader = SeriesReader::open(Cursor::new(bytes)).unwrap();

    assert_eq!(reader.len(), 2);
    assert_eq!(reader.read_entry(1).unwrap(), Some(entries[1].clone()));
    assert_eq!(reader.read_entry(99).unwrap(), None);
}

#[test]
fn series_bin_rejects_legacy_v1() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"SERI");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.resize(64, 0);

    let err = read_series_bin(&mut Cursor::new(bytes)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("unsupported series version"));
}
