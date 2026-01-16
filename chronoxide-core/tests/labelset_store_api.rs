use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, SeriesRef,
};

fn collect_labels(store: &impl LabelSetStore, series: SeriesRef) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())));
    out.sort();
    out
}

#[test]
fn labelset_store_roundtrip_intern_and_visit() {
    let labels = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("pod", "backend-123")),
    ];

    let mut interned = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let s0 = interned.intern(&labels).unwrap();
    assert_eq!(s0, SeriesRef::new(0));
    assert_eq!(interned.len(), 1);
    assert_eq!(collect_labels(&interned, s0), {
        let mut expected = vec![
            (
                "__name__".to_string(),
                "pod_cpu_usage_seconds_total".to_string(),
            ),
            ("cluster".to_string(), "prod".to_string()),
            ("pod".to_string(), "backend-123".to_string()),
        ];
        expected.sort();
        expected
    });

    let mut keyset = KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default();
    let s0 = keyset.intern(&labels).unwrap();
    assert_eq!(s0, SeriesRef::new(0));
    assert_eq!(keyset.len(), 1);
    assert_eq!(collect_labels(&keyset, s0), {
        let mut expected = vec![
            (
                "__name__".to_string(),
                "pod_cpu_usage_seconds_total".to_string(),
            ),
            ("cluster".to_string(), "prod".to_string()),
            ("pod".to_string(), "backend-123".to_string()),
        ];
        expected.sort();
        expected
    });
}

#[test]
#[should_panic(expected = "LabelSet must be canonical")]
fn interned_store_requires_canonical_labels() {
    let mut store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let labels = [KeyValueRef::from(("b", "1")), KeyValueRef::from(("a", "2"))];
    let _ = store.intern(&labels);
}

#[test]
#[should_panic(expected = "LabelSet must be canonical")]
fn keyset_store_requires_canonical_labels() {
    let mut store = KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default();
    let labels = [KeyValueRef::from(("b", "1")), KeyValueRef::from(("a", "2"))];
    let _ = store.intern(&labels);
}
