use chronoxide_core::alloc_tracking::{
    TrackingAllocator, allocation_stats, reset_allocation_counters,
};
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, LabelSetStoreError, SeriesRef, TmpLabel,
};
use chronoxide_core::otlp_labelset::{
    CanonicalLabelSet, OtlpLabelSetInterner, PreparedOtlpLabelSetScratch,
    PreparedOtlpResourceLabels, intern_labelset, intern_prepared_labelset,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

#[derive(Default)]
struct FlatInterner {
    store: FlatInternedLabelSetStore<DefaultSymbolTable>,
}

impl OtlpLabelSetInterner for FlatInterner {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {}

    fn on_intern_error(&mut self, error: Self::Error) {
        panic!("unexpected label-set interning error: {error}");
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        self.store.intern_prepared_otlp(labels)
    }
}

#[test]
fn warmed_prepared_and_legacy_labelset_hits_do_not_allocate() {
    assert_warmed_prepared_labelset_hits_with_formatted_resource_value_do_not_allocate();
    assert_warmed_repeated_string_labelset_hits_do_not_allocate();
}

fn assert_warmed_prepared_labelset_hits_with_formatted_resource_value_do_not_allocate() {
    let resource_attributes = [
        string_attribute("cluster", "prod"),
        string_attribute("namespace", "payments"),
        string_attribute("service_name", "checkout"),
        KeyValue {
            key: "service_instance".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::IntValue(42)),
            }),
            key_strindex: 0,
        },
    ];
    let datapoint_attributes = [
        string_attribute("container", "web"),
        string_attribute("pod", "checkout-7d9f4"),
    ];
    let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let metric = resource.metric("http.server.request.duration");
    let mut scratch = PreparedOtlpLabelSetScratch::default();
    let mut interner = FlatInterner::default();

    let expected =
        intern_prepared_labelset(&mut interner, &metric, &datapoint_attributes, &mut scratch)
            .expect("warm-up prepared label set must intern");

    reset_allocation_counters();
    for _ in 0..1_000 {
        let actual =
            intern_prepared_labelset(&mut interner, &metric, &datapoint_attributes, &mut scratch);
        assert_eq!(actual, Some(expected));
    }
    let stats = allocation_stats();

    assert_eq!(stats.alloc_calls, 0);
    assert_eq!(stats.realloc_calls, 0);
    assert_eq!(stats.dealloc_calls, 0);
    assert_eq!(stats.requested_total, 0);
    assert_eq!(stats.requested_freed_total, 0);
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(OtlpAnyValue {
            value: Some(AnyValue::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn assert_warmed_repeated_string_labelset_hits_do_not_allocate() {
    let resource_attributes = [
        string_attribute("cluster", "prod"),
        string_attribute("namespace", "payments"),
        string_attribute("service_name", "checkout"),
    ];
    let datapoint_attributes = [
        string_attribute("container", "web"),
        string_attribute("pod", "checkout-7d9f4"),
    ];
    let mut interner = FlatInterner::default();
    let mut scratch_values = Vec::new();
    let mut tmp_labels: Vec<TmpLabel<'_>> = Vec::new();

    let expected = intern_labelset(
        &mut interner,
        &resource_attributes,
        "http.server.request.duration",
        &datapoint_attributes,
        &mut scratch_values,
        &mut tmp_labels,
    )
    .expect("warm-up label set must intern");

    reset_allocation_counters();
    for _ in 0..1_000 {
        let actual = intern_labelset(
            &mut interner,
            &resource_attributes,
            "http.server.request.duration",
            &datapoint_attributes,
            &mut scratch_values,
            &mut tmp_labels,
        );
        assert_eq!(actual, Some(expected));
    }
    let stats = allocation_stats();

    assert_eq!(stats.alloc_calls, 0);
    assert_eq!(stats.realloc_calls, 0);
    assert_eq!(stats.dealloc_calls, 0);
    assert_eq!(stats.requested_total, 0);
    assert_eq!(stats.requested_freed_total, 0);
}
