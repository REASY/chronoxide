use std::cell::Cell;

use crate::labels::{
    KeyValueRef, METRIC_NAME_LABEL, PreparedInternedKeyValue, SeriesRef, TmpLabel, TmpValue,
};
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

pub trait OtlpLabelSetInterner {
    type Error;

    fn on_skipped_non_scalar(&mut self);
    fn on_intern_error(&mut self, error: Self::Error);
    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error>;
}

#[derive(Clone, Copy)]
pub struct CanonicalLabelSet<'view, 'input> {
    labels: CanonicalLabels<'view, 'input>,
}

#[derive(Clone, Copy)]
enum CanonicalLabels<'view, 'input> {
    FullSort {
        labels: &'view [TmpLabel<'input>],
        scratch_values: &'view [Box<str>],
    },
    Prepared {
        labels: &'view [PreparedCanonicalLabel<'input>],
        resource_values: &'view [Box<str>],
        datapoint_values: &'view [Box<str>],
        resource_symbols: &'view [Cell<Option<PreparedInternedKeyValue>>],
        metric_symbols: &'view Cell<Option<PreparedInternedKeyValue>>,
    },
}

pub struct CanonicalLabelSetIter<'view, 'input> {
    inner: CanonicalLabelSetIterInner<'view, 'input>,
}

enum CanonicalLabelSetIterInner<'view, 'input> {
    FullSort {
        labels: std::slice::Iter<'view, TmpLabel<'input>>,
        scratch_values: &'view [Box<str>],
    },
    Prepared {
        labels: std::slice::Iter<'view, PreparedCanonicalLabel<'input>>,
        resource_values: &'view [Box<str>],
        datapoint_values: &'view [Box<str>],
    },
}

impl<'view, 'input> CanonicalLabelSet<'view, 'input>
where
    'input: 'view,
{
    pub fn iter(self) -> CanonicalLabelSetIter<'view, 'input> {
        match self.labels {
            CanonicalLabels::FullSort {
                labels,
                scratch_values,
            } => CanonicalLabelSetIter {
                inner: CanonicalLabelSetIterInner::FullSort {
                    labels: labels.iter(),
                    scratch_values,
                },
            },
            CanonicalLabels::Prepared {
                labels,
                resource_values,
                datapoint_values,
                ..
            } => CanonicalLabelSetIter {
                inner: CanonicalLabelSetIterInner::Prepared {
                    labels: labels.iter(),
                    resource_values,
                    datapoint_values,
                },
            },
        }
    }

    pub(crate) fn prepared_parts(self) -> Option<PreparedCanonicalParts<'view, 'input>> {
        match self.labels {
            CanonicalLabels::FullSort { .. } => None,
            CanonicalLabels::Prepared {
                labels,
                resource_values,
                datapoint_values,
                resource_symbols,
                metric_symbols,
            } => Some(PreparedCanonicalParts {
                labels,
                resource_values,
                datapoint_values,
                resource_symbols,
                metric_symbols,
            }),
        }
    }
}

pub(crate) struct PreparedCanonicalParts<'view, 'input> {
    labels: &'view [PreparedCanonicalLabel<'input>],
    resource_values: &'view [Box<str>],
    datapoint_values: &'view [Box<str>],
    resource_symbols: &'view [Cell<Option<PreparedInternedKeyValue>>],
    metric_symbols: &'view Cell<Option<PreparedInternedKeyValue>>,
}

impl<'view, 'input> PreparedCanonicalParts<'view, 'input>
where
    'input: 'view,
{
    pub(crate) fn iter(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (
            KeyValueRef<'_>,
            Option<&Cell<Option<PreparedInternedKeyValue>>>,
        ),
    > {
        self.labels.iter().map(|label| {
            let value = label
                .value
                .as_str(self.resource_values, self.datapoint_values);
            let symbols = match label.cache {
                PreparedSymbolCache::None => None,
                PreparedSymbolCache::Resource(index) => Some(&self.resource_symbols[index]),
                PreparedSymbolCache::Metric => Some(self.metric_symbols),
            };
            (
                KeyValueRef {
                    key: label.key,
                    value,
                },
                symbols,
            )
        })
    }
}

impl<'view, 'input> Iterator for CanonicalLabelSetIter<'view, 'input>
where
    'input: 'view,
{
    type Item = KeyValueRef<'view>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            CanonicalLabelSetIterInner::FullSort {
                labels,
                scratch_values,
            } => labels.next().map(|label| KeyValueRef {
                key: label.key,
                value: label.value.as_str(scratch_values),
            }),
            CanonicalLabelSetIterInner::Prepared {
                labels,
                resource_values,
                datapoint_values,
            } => labels.next().map(|label| KeyValueRef {
                key: label.key,
                value: label.value.as_str(resource_values, datapoint_values),
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for CanonicalLabelSetIter<'_, '_> {
    fn len(&self) -> usize {
        match &self.inner {
            CanonicalLabelSetIterInner::FullSort { labels, .. } => labels.len(),
            CanonicalLabelSetIterInner::Prepared { labels, .. } => labels.len(),
        }
    }
}

impl std::iter::FusedIterator for CanonicalLabelSetIter<'_, '_> {}

#[derive(Clone, Copy)]
struct PreparedCanonicalLabel<'a> {
    key: &'a str,
    value: PreparedCanonicalValue<'a>,
    cache: PreparedSymbolCache,
}

#[derive(Clone, Copy)]
enum PreparedSymbolCache {
    None,
    Resource(usize),
    Metric,
}

#[derive(Clone, Copy)]
enum PreparedCanonicalValue<'a> {
    Borrowed(&'a str),
    ResourceScratch(usize),
    DatapointScratch(usize),
}

impl<'a> PreparedCanonicalValue<'a> {
    fn as_str<'view>(
        self,
        resource_values: &'view [Box<str>],
        datapoint_values: &'view [Box<str>],
    ) -> &'view str
    where
        'a: 'view,
    {
        match self {
            Self::Borrowed(value) => value,
            Self::ResourceScratch(index) => resource_values[index].as_ref(),
            Self::DatapointScratch(index) => datapoint_values[index].as_ref(),
        }
    }
}

/// Request-local canonical resource labels reused by every metric datapoint.
///
/// Preparation formats scalar resource values and resolves raw-key duplicate
/// precedence, but deliberately does not mutate a label store or report
/// skipped values. Those observable effects still occur only after event-time
/// policy accepts an individual datapoint.
pub struct PreparedOtlpResourceLabels<'input> {
    labels: Vec<TmpLabel<'input>>,
    scratch_values: Vec<Box<str>>,
    symbol_cache: Vec<Cell<Option<PreparedInternedKeyValue>>>,
    skipped_non_scalar: usize,
}

impl<'input> PreparedOtlpResourceLabels<'input> {
    pub fn new(resource_attrs: &'input [KeyValue]) -> Self {
        let mut labels = Vec::new();
        let mut scratch_values = Vec::new();
        let mut skipped_non_scalar = 0usize;
        push_kvs(
            &mut labels,
            &mut scratch_values,
            resource_attrs,
            0,
            &mut || skipped_non_scalar = skipped_non_scalar.saturating_add(1),
        );
        sort_and_dedup_labels(&mut labels);
        let symbol_cache = (0..labels.len()).map(|_| Cell::new(None)).collect();
        Self {
            labels,
            scratch_values,
            symbol_cache,
            skipped_non_scalar,
        }
    }

    pub fn metric<'plan>(
        &'plan self,
        metric_name: &'input str,
    ) -> PreparedOtlpMetricLabels<'plan, 'input> {
        PreparedOtlpMetricLabels {
            resource: self,
            metric_name,
            symbol_cache: Cell::new(None),
        }
    }
}

pub struct PreparedOtlpMetricLabels<'plan, 'input> {
    resource: &'plan PreparedOtlpResourceLabels<'input>,
    metric_name: &'input str,
    symbol_cache: Cell<Option<PreparedInternedKeyValue>>,
}

#[derive(Default)]
pub struct PreparedOtlpLabelSetScratch<'input> {
    datapoint_values: Vec<Box<str>>,
    datapoint_labels: Vec<TmpLabel<'input>>,
    merged_labels: Vec<PreparedCanonicalLabel<'input>>,
}

pub fn intern_prepared_labelset<'plan, 'input, I: OtlpLabelSetInterner>(
    interner: &mut I,
    metric: &PreparedOtlpMetricLabels<'plan, 'input>,
    datapoint_attrs: &'input [KeyValue],
    scratch: &mut PreparedOtlpLabelSetScratch<'input>,
) -> Option<SeriesRef> {
    let labels = build_prepared_canonical_labelset(interner, metric, datapoint_attrs, scratch);

    match interner.intern(labels) {
        Ok(series) => Some(series),
        Err(err) => {
            interner.on_intern_error(err);
            None
        }
    }
}

pub fn intern_labelset<'a, 's, I: OtlpLabelSetInterner>(
    interner: &mut I,
    resource_attrs: &'a [KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [KeyValue],
    scratch_values: &'s mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Option<SeriesRef>
where
    'a: 's,
{
    let labels = build_canonical_labelset(
        interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    );

    match interner.intern(labels) {
        Ok(series) => Some(series),
        Err(err) => {
            interner.on_intern_error(err);
            None
        }
    }
}

fn build_canonical_labelset<'a, 's, I: OtlpLabelSetInterner>(
    interner: &mut I,
    resource_attrs: &'a [KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [KeyValue],
    scratch_values: &'s mut Vec<Box<str>>,
    tmp_labels: &'s mut Vec<TmpLabel<'a>>,
) -> CanonicalLabelSet<'s, 'a>
where
    'a: 's,
{
    tmp_labels.clear();
    scratch_values.clear();

    tmp_labels.push(TmpLabel {
        key: METRIC_NAME_LABEL,
        value: TmpValue::Borrowed(metric_name),
        rank: 3,
        ordinal: 0,
    });

    let mut on_skipped = || interner.on_skipped_non_scalar();
    push_kvs(
        tmp_labels,
        scratch_values,
        resource_attrs,
        0,
        &mut on_skipped,
    );
    push_kvs(
        tmp_labels,
        scratch_values,
        datapoint_attrs,
        2,
        &mut on_skipped,
    );

    sort_and_dedup_labels(tmp_labels);

    CanonicalLabelSet {
        labels: CanonicalLabels::FullSort {
            labels: tmp_labels.as_slice(),
            scratch_values: scratch_values.as_slice(),
        },
    }
}

fn build_prepared_canonical_labelset<'view, 'plan, 'input, I: OtlpLabelSetInterner>(
    interner: &mut I,
    metric: &'view PreparedOtlpMetricLabels<'plan, 'input>,
    datapoint_attrs: &'input [KeyValue],
    scratch: &'view mut PreparedOtlpLabelSetScratch<'input>,
) -> CanonicalLabelSet<'view, 'input>
where
    'plan: 'view,
    'input: 'view,
{
    scratch.datapoint_values.clear();
    scratch.datapoint_labels.clear();
    scratch.merged_labels.clear();

    let mut datapoint_skipped_non_scalar = 0usize;
    push_kvs(
        &mut scratch.datapoint_labels,
        &mut scratch.datapoint_values,
        datapoint_attrs,
        2,
        &mut || {
            datapoint_skipped_non_scalar = datapoint_skipped_non_scalar.saturating_add(1);
        },
    );
    sort_and_dedup_labels(&mut scratch.datapoint_labels);
    scratch.merged_labels.reserve(
        metric
            .resource
            .labels
            .len()
            .saturating_add(scratch.datapoint_labels.len())
            .saturating_add(1),
    );

    merge_prepared_labels(
        metric,
        scratch.datapoint_labels.as_slice(),
        &mut scratch.merged_labels,
    );

    let skipped_non_scalar = metric
        .resource
        .skipped_non_scalar
        .saturating_add(datapoint_skipped_non_scalar);
    for _ in 0..skipped_non_scalar {
        interner.on_skipped_non_scalar();
    }

    CanonicalLabelSet {
        labels: CanonicalLabels::Prepared {
            labels: scratch.merged_labels.as_slice(),
            resource_values: metric.resource.scratch_values.as_slice(),
            datapoint_values: scratch.datapoint_values.as_slice(),
            resource_symbols: metric.resource.symbol_cache.as_slice(),
            metric_symbols: &metric.symbol_cache,
        },
    }
}

fn merge_prepared_labels<'input>(
    metric: &PreparedOtlpMetricLabels<'_, 'input>,
    datapoint_labels: &[TmpLabel<'input>],
    merged: &mut Vec<PreparedCanonicalLabel<'input>>,
) {
    let resource_labels = metric.resource.labels.as_slice();
    let mut resource_index = 0usize;
    let mut datapoint_index = 0usize;
    let mut metric_pending = true;

    while resource_index < resource_labels.len()
        || datapoint_index < datapoint_labels.len()
        || metric_pending
    {
        let resource = resource_labels.get(resource_index);
        let datapoint = datapoint_labels.get(datapoint_index);
        let next_attribute_key = match (resource, datapoint) {
            (Some(resource), Some(datapoint)) => Some(resource.key.min(datapoint.key)),
            (Some(resource), None) => Some(resource.key),
            (None, Some(datapoint)) => Some(datapoint.key),
            (None, None) => None,
        };

        if metric_pending && next_attribute_key.is_none_or(|key| METRIC_NAME_LABEL < key) {
            merged.push(PreparedCanonicalLabel {
                key: METRIC_NAME_LABEL,
                value: PreparedCanonicalValue::Borrowed(metric.metric_name),
                cache: PreparedSymbolCache::Metric,
            });
            metric_pending = false;
            continue;
        }

        match (resource, datapoint) {
            (Some(resource), Some(datapoint)) => match resource.key.cmp(datapoint.key) {
                std::cmp::Ordering::Less => {
                    merged.push(prepared_resource_label(resource_index, *resource));
                    resource_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    merged.push(prepared_datapoint_label(*datapoint));
                    resource_index += 1;
                    datapoint_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    merged.push(prepared_datapoint_label(*datapoint));
                    datapoint_index += 1;
                }
            },
            (Some(resource), None) => {
                merged.push(prepared_resource_label(resource_index, *resource));
                resource_index += 1;
            }
            (None, Some(datapoint)) => {
                merged.push(prepared_datapoint_label(*datapoint));
                datapoint_index += 1;
            }
            (None, None) => {
                debug_assert!(metric_pending);
            }
        }
    }
}

fn prepared_resource_label(
    resource_index: usize,
    label: TmpLabel<'_>,
) -> PreparedCanonicalLabel<'_> {
    PreparedCanonicalLabel {
        key: label.key,
        value: match label.value {
            TmpValue::Borrowed(value) => PreparedCanonicalValue::Borrowed(value),
            TmpValue::Scratch(index) => PreparedCanonicalValue::ResourceScratch(index),
        },
        cache: PreparedSymbolCache::Resource(resource_index),
    }
}

fn prepared_datapoint_label(label: TmpLabel<'_>) -> PreparedCanonicalLabel<'_> {
    PreparedCanonicalLabel {
        key: label.key,
        value: match label.value {
            TmpValue::Borrowed(value) => PreparedCanonicalValue::Borrowed(value),
            TmpValue::Scratch(index) => PreparedCanonicalValue::DatapointScratch(index),
        },
        cache: PreparedSymbolCache::None,
    }
}

fn sort_and_dedup_labels(labels: &mut Vec<TmpLabel<'_>>) {
    labels.sort_unstable_by(|a, b| {
        a.key
            .cmp(b.key)
            .then_with(|| a.rank.cmp(&b.rank))
            .then_with(|| a.ordinal.cmp(&b.ordinal))
    });

    let mut read = 0;
    let mut write = 0;
    while read < labels.len() {
        let key = labels[read].key;
        let mut end = read + 1;
        while end < labels.len() && labels[end].key == key {
            end += 1;
        }
        labels[write] = labels[end - 1];
        write += 1;
        read = end;
    }
    labels.truncate(write);
}

fn push_kvs<'a, F>(
    out: &mut Vec<TmpLabel<'a>>,
    scratch_values: &mut Vec<Box<str>>,
    kvs: &'a [KeyValue],
    rank: u8,
    on_skipped: &mut F,
) where
    F: FnMut(),
{
    out.reserve(kvs.len());

    for kv in kvs {
        let key = kv.key.as_str();
        if key.is_empty() || key == METRIC_NAME_LABEL {
            continue;
        }

        let Some(any_value) = kv.value.as_ref() else {
            continue;
        };
        let Some(value) = any_value.value.as_ref() else {
            continue;
        };

        let value = match value {
            AnyValue::StringValue(value) => TmpValue::Borrowed(value.as_str()),
            AnyValue::BoolValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::IntValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::DoubleValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::BytesValue(_)
            | AnyValue::ArrayValue(_)
            | AnyValue::KvlistValue(_)
            | AnyValue::StringValueStrindex(_) => {
                on_skipped();
                continue;
            }
        };

        let ordinal = out.len();
        out.push(TmpLabel {
            key,
            value,
            rank,
            ordinal,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;

    #[derive(Default)]
    struct RecordingInterner {
        labelsets: Vec<Vec<(String, String)>>,
        skipped: usize,
    }

    impl OtlpLabelSetInterner for RecordingInterner {
        type Error = std::convert::Infallible;

        fn on_skipped_non_scalar(&mut self) {
            self.skipped += 1;
        }

        fn on_intern_error(&mut self, error: Self::Error) {
            match error {}
        }

        fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
            self.labelsets.push(
                labels
                    .iter()
                    .map(|label| (label.key.to_string(), label.value.to_string()))
                    .collect(),
            );
            Ok(SeriesRef::new((self.labelsets.len() - 1) as u32))
        }
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

    fn attribute(key: &str, value: Option<AnyValue>) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.map(|value| OtlpAnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    fn record_full_sort(
        resource_attributes: &[KeyValue],
        metric_name: &str,
        datapoint_attributes: &[KeyValue],
    ) -> RecordingInterner {
        let mut interner = RecordingInterner::default();
        let mut scratch_values = Vec::new();
        let mut tmp_labels = Vec::new();
        assert_eq!(
            intern_labelset(
                &mut interner,
                resource_attributes,
                metric_name,
                datapoint_attributes,
                &mut scratch_values,
                &mut tmp_labels,
            ),
            Some(SeriesRef::new(0))
        );
        interner
    }

    fn record_prepared(
        resource_attributes: &[KeyValue],
        metric_name: &str,
        datapoint_attributes: &[KeyValue],
    ) -> RecordingInterner {
        let resource = PreparedOtlpResourceLabels::new(resource_attributes);
        let metric = resource.metric(metric_name);
        let mut scratch = PreparedOtlpLabelSetScratch::default();
        let mut interner = RecordingInterner::default();
        assert_eq!(
            intern_prepared_labelset(&mut interner, &metric, datapoint_attributes, &mut scratch,),
            Some(SeriesRef::new(0))
        );
        interner
    }

    #[test]
    fn canonicalization_preserves_last_input_value_for_equal_key_and_rank() {
        let resource_attributes = [
            string_attribute("z", "z-value"),
            string_attribute("resource-duplicate", "first"),
            string_attribute("a", "a-value"),
            string_attribute("resource-duplicate", "last"),
            string_attribute("shared", "resource"),
        ];
        let datapoint_attributes = [
            string_attribute("shared", "datapoint-first"),
            string_attribute("shared", "datapoint-last"),
        ];
        let mut interner = RecordingInterner::default();
        let mut scratch_values = Vec::new();
        let mut tmp_labels = Vec::new();

        let series = intern_labelset(
            &mut interner,
            &resource_attributes,
            "metric.name",
            &datapoint_attributes,
            &mut scratch_values,
            &mut tmp_labels,
        );

        assert_eq!(series, Some(SeriesRef::new(0)));
        assert_eq!(
            interner.labelsets,
            [vec![
                ("__name__".to_string(), "metric.name".to_string()),
                ("a".to_string(), "a-value".to_string()),
                ("resource-duplicate".to_string(), "last".to_string()),
                ("shared".to_string(), "datapoint-last".to_string()),
                ("z".to_string(), "z-value".to_string()),
            ]]
        );
    }

    #[test]
    fn canonicalization_reuses_tmp_label_capacity_across_repeated_hits() {
        let attributes = [
            string_attribute("z", "z-value"),
            string_attribute("a", "a-value"),
        ];
        let mut interner = RecordingInterner::default();
        let mut scratch_values = Vec::new();
        let mut tmp_labels = Vec::new();

        assert_eq!(
            intern_labelset(
                &mut interner,
                &attributes,
                "metric",
                &[],
                &mut scratch_values,
                &mut tmp_labels,
            ),
            Some(SeriesRef::new(0))
        );
        let capacity = tmp_labels.capacity();
        let pointer = tmp_labels.as_ptr();

        for expected in 1..=32 {
            assert_eq!(
                intern_labelset(
                    &mut interner,
                    &attributes,
                    "metric",
                    &[],
                    &mut scratch_values,
                    &mut tmp_labels,
                ),
                Some(SeriesRef::new(expected))
            );
        }

        assert_eq!(tmp_labels.capacity(), capacity);
        assert_eq!(tmp_labels.as_ptr(), pointer);
        assert!(scratch_values.is_empty());
    }

    fn reference_canonical_labels(
        resource_attributes: &[KeyValue],
        metric_name: &str,
        datapoint_attributes: &[KeyValue],
    ) -> Vec<(String, String)> {
        let mut labels = vec![(METRIC_NAME_LABEL.to_string(), metric_name.to_string(), 3u8)];
        for (attributes, rank) in [(resource_attributes, 0u8), (datapoint_attributes, 2u8)] {
            for attribute in attributes {
                let value = match attribute
                    .value
                    .as_ref()
                    .and_then(|value| value.value.as_ref())
                {
                    Some(AnyValue::StringValue(value)) => value.clone(),
                    _ => continue,
                };
                labels.push((attribute.key.clone(), value, rank));
            }
        }

        labels.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2)));
        let mut canonical = Vec::new();
        let mut index = 0;
        while index < labels.len() {
            let key = labels[index].0.as_str();
            let mut end = index + 1;
            while end < labels.len() && labels[end].0 == key {
                end += 1;
            }
            canonical.push((labels[end - 1].0.clone(), labels[end - 1].1.clone()));
            index = end;
        }
        canonical
    }

    #[test]
    fn ordinal_unstable_sort_matches_previous_stable_canonicalization() {
        let mut state = 0x7f4a_7c15_9e37_79b9u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };

        for case in 0..128 {
            let resource_count = (next() % 65) as usize;
            let datapoint_count = (next() % 65) as usize;
            let resource_attributes = (0..resource_count)
                .map(|index| {
                    string_attribute(
                        format!("key_{:02}", next() % 12).as_str(),
                        format!("resource_{case}_{index}").as_str(),
                    )
                })
                .collect::<Vec<_>>();
            let datapoint_attributes = (0..datapoint_count)
                .map(|index| {
                    string_attribute(
                        format!("key_{:02}", next() % 12).as_str(),
                        format!("datapoint_{case}_{index}").as_str(),
                    )
                })
                .collect::<Vec<_>>();
            let expected =
                reference_canonical_labels(&resource_attributes, "metric", &datapoint_attributes);
            let mut interner = RecordingInterner::default();
            let mut scratch_values = Vec::new();
            let mut tmp_labels = Vec::new();

            assert_eq!(
                intern_labelset(
                    &mut interner,
                    &resource_attributes,
                    "metric",
                    &datapoint_attributes,
                    &mut scratch_values,
                    &mut tmp_labels,
                ),
                Some(SeriesRef::new(0))
            );
            assert_eq!(interner.labelsets, [expected], "case {case}");
        }
    }

    #[test]
    fn prepared_plan_matches_full_sort_for_generated_inputs() {
        let mut state = 0x8d58_ac26_afe1_2e47u64;
        let mut next = || {
            state = state
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            state
        };

        for case in 0..256 {
            let resource_count = (next() % 65) as usize;
            let datapoint_count = (next() % 65) as usize;
            let resource_attributes = (0..resource_count)
                .map(|index| {
                    let key = match next() % 16 {
                        0 => "".to_string(),
                        1 => METRIC_NAME_LABEL.to_string(),
                        value => format!("key_{value:02}"),
                    };
                    string_attribute(key.as_str(), format!("resource_{case}_{index}").as_str())
                })
                .collect::<Vec<_>>();
            let datapoint_attributes = (0..datapoint_count)
                .map(|index| {
                    let key = match next() % 16 {
                        0 => "".to_string(),
                        1 => METRIC_NAME_LABEL.to_string(),
                        value => format!("key_{value:02}"),
                    };
                    string_attribute(key.as_str(), format!("datapoint_{case}_{index}").as_str())
                })
                .collect::<Vec<_>>();

            let full = record_full_sort(
                &resource_attributes,
                "prepared.metric",
                &datapoint_attributes,
            );
            let prepared = record_prepared(
                &resource_attributes,
                "prepared.metric",
                &datapoint_attributes,
            );
            assert_eq!(prepared.labelsets, full.labelsets, "case {case}");
            assert_eq!(prepared.skipped, full.skipped, "case {case}");
        }
    }

    #[test]
    fn prepared_plan_preserves_scalar_skip_and_duplicate_semantics() {
        let resource_attributes = vec![
            string_attribute("z", "resource-z"),
            attribute("bool", Some(AnyValue::BoolValue(true))),
            attribute("int", Some(AnyValue::IntValue(i64::MIN))),
            attribute("double", Some(AnyValue::DoubleValue(-0.0))),
            string_attribute("resource-duplicate", "first"),
            attribute(
                "resource-duplicate",
                Some(AnyValue::BytesValue(vec![1, 2, 3])),
            ),
            string_attribute("resource-duplicate", "last-supported"),
            attribute("resource-duplicate", None),
            attribute(
                "skip-resource",
                Some(AnyValue::ArrayValue(Default::default())),
            ),
            attribute("", Some(AnyValue::BytesValue(vec![4]))),
            attribute(
                METRIC_NAME_LABEL,
                Some(AnyValue::KvlistValue(Default::default())),
            ),
            string_attribute("!before-name", "first"),
        ];
        let datapoint_attributes = vec![
            attribute("double", Some(AnyValue::DoubleValue(f64::INFINITY))),
            string_attribute("z", "datapoint-first"),
            attribute("z", Some(AnyValue::StringValueStrindex(7))),
            string_attribute("z", "datapoint-last-supported"),
            attribute("skip-datapoint", Some(AnyValue::BytesValue(vec![9]))),
        ];

        let full = record_full_sort(
            &resource_attributes,
            "prepared.metric",
            &datapoint_attributes,
        );
        let prepared = record_prepared(
            &resource_attributes,
            "prepared.metric",
            &datapoint_attributes,
        );

        assert_eq!(prepared.labelsets, full.labelsets);
        assert_eq!(prepared.skipped, full.skipped);
        assert_eq!(prepared.skipped, 4);
        assert_eq!(
            prepared.labelsets[0],
            vec![
                ("!before-name".to_string(), "first".to_string()),
                ("__name__".to_string(), "prepared.metric".to_string()),
                ("bool".to_string(), "true".to_string()),
                ("double".to_string(), "inf".to_string()),
                ("int".to_string(), i64::MIN.to_string()),
                (
                    "resource-duplicate".to_string(),
                    "last-supported".to_string(),
                ),
                ("z".to_string(), "datapoint-last-supported".to_string(),),
            ]
        );
    }

    #[test]
    fn profiling_string_table_value_is_skipped_for_metrics() {
        let attributes = [KeyValue {
            key: "profile-only".to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::StringValueStrindex(7)),
            }),
            key_strindex: 0,
        }];
        let mut labels = Vec::new();
        let mut scratch_values = Vec::new();
        let mut skipped = 0;

        push_kvs(
            &mut labels,
            &mut scratch_values,
            &attributes,
            0,
            &mut || skipped += 1,
        );

        assert!(labels.is_empty());
        assert!(scratch_values.is_empty());
        assert_eq!(skipped, 1);
    }
}
