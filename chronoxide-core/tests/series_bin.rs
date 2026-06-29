use std::io::Cursor;

use chronoxide_core::storage::series::{
    SERIES_KIND_FLOAT, SegmentSymbols, SeriesEntry, read_series_bin_v1, read_symbols_bin,
    write_series_bin_v1, write_symbols_bin,
};

#[test]
fn symbols_bin_roundtrips_segment_local_strings() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("__name__");
    let metric = symbols.intern("cpu_usage_seconds_total");
    let pod = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");

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
fn series_bin_v1_roundtrips_series_entries() {
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
            labels: vec![
                (name_key, name_val),
                (namespace_key, namespace_val),
                (pod_key, backend_1),
            ],
        },
        SeriesEntry {
            series_id: 0x2222,
            kind_mask: SERIES_KIND_FLOAT,
            labels: vec![
                (name_key, name_val),
                (namespace_key, namespace_val),
                (pod_key, backend_2),
            ],
        },
    ];

    let mut bytes = Vec::new();
    write_series_bin_v1(&mut bytes, &entries).unwrap();

    let restored = read_series_bin_v1(&mut Cursor::new(bytes)).unwrap();
    assert_eq!(restored, entries);
}
