use crate::labels::{KeyValueRef, METRIC_NAME_LABEL, SeriesRef, TmpLabel, TmpValue};
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

pub trait OtlpLabelSetInterner {
    type Error;

    fn on_skipped_non_scalar(&mut self);
    fn on_intern_error(&mut self, error: Self::Error);
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, Self::Error>;
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
    let labels = build_labelset_refs(
        interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    );

    match interner.intern(&labels) {
        Ok(series) => Some(series),
        Err(err) => {
            interner.on_intern_error(err);
            None
        }
    }
}

fn build_labelset_refs<'a, 's, I: OtlpLabelSetInterner>(
    interner: &mut I,
    resource_attrs: &'a [KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [KeyValue],
    scratch_values: &'s mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Vec<KeyValueRef<'s>>
where
    'a: 's,
{
    tmp_labels.clear();
    scratch_values.clear();

    tmp_labels.push(TmpLabel {
        key: METRIC_NAME_LABEL,
        value: TmpValue::Borrowed(metric_name),
        rank: 3,
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

    tmp_labels.sort_by(|a, b| a.key.cmp(b.key).then_with(|| a.rank.cmp(&b.rank)));

    let mut canonical: Vec<KeyValueRef<'_>> = Vec::with_capacity(tmp_labels.len());
    let scratch_slice = scratch_values.as_slice();

    let mut i = 0;
    while i < tmp_labels.len() {
        let key = tmp_labels[i].key;
        let mut j = i + 1;
        while j < tmp_labels.len() && tmp_labels[j].key == key {
            j += 1;
        }
        let chosen = tmp_labels[j - 1];
        let value = chosen.value.as_str(scratch_slice);
        canonical.push(KeyValueRef { key, value });
        i = j;
    }

    canonical
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
            AnyValue::BytesValue(_) | AnyValue::ArrayValue(_) | AnyValue::KvlistValue(_) => {
                on_skipped();
                continue;
            }
        };

        out.push(TmpLabel { key, value, rank });
    }
}
