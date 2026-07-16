//! Fully verified schema-6 series materialization for the layout A/B facade.

use super::*;

/// One schema-6 series whose stable identity is exposed only after the
/// complete v2 label row and every required same-generation symbol have been
/// materialized and authenticated against the stored fingerprint.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Schema6VerifiedSeries {
    series_ref: u32,
    series_id: u64,
    kind_mask: u8,
    chunk_index: crate::storage::chunk::ChunkIndexRange,
    labels: Vec<(String, String)>,
}

impl Schema6VerifiedSeries {
    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn series_id(&self) -> u64 {
        self.series_id
    }

    pub(crate) fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    pub(crate) fn chunk_index(&self) -> crate::storage::chunk::ChunkIndexRange {
        self.chunk_index
    }

    pub(crate) fn labels(&self) -> &[(String, String)] {
        &self.labels
    }
}

/// Ordered, query-local verified series. Duplicate requested refs remain
/// duplicate outputs, while required symbol IDs are resolved only once for
/// the complete batch. The owned allocation remains governed until drop.
#[derive(Debug)]
pub(crate) struct GovernedSchema6VerifiedSeriesBatch {
    provenance: SegmentGenerationProvenance,
    values: Vec<Schema6VerifiedSeries>,
    _charge: MetadataCharge,
}

impl GovernedSchema6VerifiedSeriesBatch {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

#[derive(Debug)]
struct PendingVerifiedSeries {
    series_ref: u32,
    table_entry: SeriesTableEntryV2,
    encoded_labels: Vec<(u32, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CanonicalComponent {
    Name,
    Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SymbolOccurrence {
    symbol_id: u32,
    series_index: usize,
    label_index: usize,
    component: CanonicalComponent,
}

impl GovernedSchema6SeriesSession {
    /// Materializes complete schema-6 v2 label rows and exposes their stored
    /// identities only after all required same-generation symbols reproduce
    /// the canonical label-byte fingerprints. Symbol IDs shared by labels or
    /// duplicate requested series are resolved once for the whole batch.
    pub(crate) fn materialize_verified(
        &self,
        root: &GovernedSchema6SeriesRoot,
        chunk_index: &GovernedSchema6ChunkIndexSession,
        chunk_index_root: &GovernedSchema6ChunkIndexRoot,
        symbols: &GovernedSymbolSession,
        series_refs: &[u32],
    ) -> Result<GovernedSchema6VerifiedSeriesBatch, Schema6SeriesReaderError> {
        self.ensure_provenance(&root.provenance)?;
        chunk_index.ensure_same_generation(&self.guard)?;
        chunk_index.bind_series_count(chunk_index_root, root.num_series())?;
        symbols.ensure_same_generation(&self.guard)?;
        self.validate_series_refs(root, series_refs)?;

        let reader = self.guard.reader(SegmentFile::Series)?;
        let declared = checked_table_work_upper::<PendingVerifiedSeries>(series_refs.len())?;
        let mut charge = reader
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let mut work = self.prepare_table_work(root, series_refs)?;
        let mut pending =
            try_vec_with_capacity(series_refs.len(), "schema-6 pending verified series")?;
        let pending_vec_bytes = checked_vec_bytes::<PendingVerifiedSeries>(
            pending.capacity(),
            "schema-6 pending-series charge overflows",
        )?;
        let mut encoded_label_bytes = 0u64;
        charge
            .reconcile(checked_add_bytes(
                work.temporary_bytes()?,
                pending_vec_bytes,
                "schema-6 verified-series working-set charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        self.load_table_spans(root, &mut work)?;

        for series_ref in series_refs.iter().copied() {
            let table_entry = self.record_series_result(work.entry(series_ref))?;
            chunk_index.validate_series_range(
                chunk_index_root,
                series_ref,
                table_entry.chunk_index,
            )?;
            let already_charged = checked_add_many_bytes(
                &[
                    work.temporary_bytes()?,
                    pending_vec_bytes,
                    encoded_label_bytes,
                ],
                "schema-6 verified-series working-set charge overflows",
            )?;
            let encoded_labels =
                self.decode_label_ids(root, table_entry, &mut charge, already_charged)?;
            encoded_label_bytes = checked_add_bytes(
                encoded_label_bytes,
                checked_vec_bytes::<(u32, u32)>(
                    encoded_labels.capacity(),
                    "schema-6 encoded-label charge overflows",
                )?,
                "schema-6 encoded-label charge overflows",
            )?;
            pending.push(PendingVerifiedSeries {
                series_ref,
                table_entry,
                encoded_labels,
            });
        }

        drop(work);
        let pending_bytes = checked_add_bytes(
            pending_vec_bytes,
            encoded_label_bytes,
            "schema-6 pending-series charge overflows",
        )?;
        charge
            .reconcile(pending_bytes)
            .map_err(MetadataCacheError::from)?;

        let component_count = pending.iter().try_fold(0usize, |total, value| {
            value
                .encoded_labels
                .len()
                .checked_mul(2)
                .and_then(|count| total.checked_add(count))
                .ok_or_else(|| invalid_input("schema-6 symbol occurrence count overflows"))
        })?;
        let declared_symbol_ids =
            checked_vec_bytes::<u32>(component_count, "schema-6 unique-symbol charge overflows")?;
        let declared_occurrences = checked_vec_bytes::<SymbolOccurrence>(
            component_count,
            "schema-6 symbol-occurrence charge overflows",
        )?;
        charge
            .reconcile(checked_add_many_bytes(
                &[pending_bytes, declared_symbol_ids, declared_occurrences],
                "schema-6 symbol-plan charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        // Sorting charged vectors avoids an auxiliary hash map whose buckets
        // would need a separate logical-capacity bound and reconciliation.
        let mut unique_symbol_ids =
            try_vec_with_capacity(component_count, "schema-6 unique symbol IDs")?;
        let mut occurrences =
            try_vec_with_capacity(component_count, "schema-6 symbol occurrences")?;
        let unique_symbol_bytes = checked_vec_bytes::<u32>(
            unique_symbol_ids.capacity(),
            "schema-6 unique-symbol charge overflows",
        )?;
        let occurrence_bytes = checked_vec_bytes::<SymbolOccurrence>(
            occurrences.capacity(),
            "schema-6 symbol-occurrence charge overflows",
        )?;
        charge
            .reconcile(checked_add_many_bytes(
                &[pending_bytes, unique_symbol_bytes, occurrence_bytes],
                "schema-6 symbol-plan charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        for (series_index, value) in pending.iter().enumerate() {
            for (label_index, &(name_sym, value_sym)) in value.encoded_labels.iter().enumerate() {
                for (symbol_id, component) in [
                    (name_sym, CanonicalComponent::Name),
                    (value_sym, CanonicalComponent::Value),
                ] {
                    if usize::try_from(symbol_id).map_or(true, |id| id >= symbols.len()) {
                        return Err(self.record_series_error(invalid_data(format!(
                            "schema-6 series ref {} requires out-of-range symbol {symbol_id}",
                            value.series_ref
                        ))));
                    }
                    unique_symbol_ids.push(symbol_id);
                    occurrences.push(SymbolOccurrence {
                        symbol_id,
                        series_index,
                        label_index,
                        component,
                    });
                }
            }
        }
        unique_symbol_ids.sort_unstable();
        unique_symbol_ids.dedup();
        occurrences.sort_unstable_by_key(|occurrence| {
            (
                occurrence.symbol_id,
                occurrence.series_index,
                occurrence.label_index,
                occurrence.component,
            )
        });

        let declared_output = checked_vec_bytes::<Schema6VerifiedSeries>(
            pending.len(),
            "schema-6 verified output charge overflows",
        )?;
        charge
            .reconcile(checked_add_many_bytes(
                &[
                    pending_bytes,
                    unique_symbol_bytes,
                    occurrence_bytes,
                    declared_output,
                ],
                "schema-6 verified output charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        let mut values = try_vec_with_capacity(pending.len(), "schema-6 verified output")?;
        let output_vec_bytes = checked_vec_bytes::<Schema6VerifiedSeries>(
            values.capacity(),
            "schema-6 verified output charge overflows",
        )?;
        let mut output_label_bytes = 0u64;
        charge
            .reconcile(checked_add_many_bytes(
                &[
                    pending_bytes,
                    unique_symbol_bytes,
                    occurrence_bytes,
                    output_vec_bytes,
                ],
                "schema-6 verified output charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        for value in &pending {
            let declared_labels = checked_vec_bytes::<(String, String)>(
                value.encoded_labels.len(),
                "schema-6 canonical-label vector charge overflows",
            )?;
            charge
                .reconcile(checked_add_many_bytes(
                    &[
                        pending_bytes,
                        unique_symbol_bytes,
                        occurrence_bytes,
                        output_vec_bytes,
                        output_label_bytes,
                        declared_labels,
                    ],
                    "schema-6 verified output charge overflows",
                )?)
                .map_err(MetadataCacheError::from)?;
            let mut labels =
                try_vec_with_capacity(value.encoded_labels.len(), "schema-6 canonical labels")?;
            let actual_labels = checked_vec_bytes::<(String, String)>(
                labels.capacity(),
                "schema-6 canonical-label vector charge overflows",
            )?;
            charge
                .reconcile(checked_add_many_bytes(
                    &[
                        pending_bytes,
                        unique_symbol_bytes,
                        occurrence_bytes,
                        output_vec_bytes,
                        output_label_bytes,
                        actual_labels,
                    ],
                    "schema-6 verified output charge overflows",
                )?)
                .map_err(MetadataCacheError::from)?;
            labels.resize_with(value.encoded_labels.len(), || {
                (String::new(), String::new())
            });
            output_label_bytes = checked_add_bytes(
                output_label_bytes,
                actual_labels,
                "schema-6 canonical-label vector charge overflows",
            )?;
            values.push(Schema6VerifiedSeries {
                series_ref: value.series_ref,
                series_id: value.table_entry.series_id,
                kind_mask: value.table_entry.kind_mask,
                chunk_index: value.table_entry.chunk_index,
                labels,
            });
        }

        drop(pending);
        let mut charged_bytes = checked_add_many_bytes(
            &[
                unique_symbol_bytes,
                occurrence_bytes,
                output_vec_bytes,
                output_label_bytes,
            ],
            "schema-6 canonical-label working-set charge overflows",
        )?;
        charge
            .reconcile(charged_bytes)
            .map_err(MetadataCacheError::from)?;

        let mut deferred_error = None;
        let visit_result =
            symbols.visit_resolved_many(&unique_symbol_ids, |request_index, value| {
                let symbol_id = *unique_symbol_ids.get(request_index).ok_or_else(|| {
                    invalid_input("schema-6 symbol resolver returned an invalid request index")
                })?;
                let start = occurrences.partition_point(|entry| entry.symbol_id < symbol_id);
                let end = occurrences.partition_point(|entry| entry.symbol_id <= symbol_id);
                if start == end {
                    return Err(invalid_input(
                        "schema-6 resolved symbol has no materialization occurrence",
                    ));
                }
                for occurrence in &occurrences[start..end] {
                    let requested = u64::try_from(value.len()).map_err(|_| {
                        invalid_input("schema-6 canonical string length exceeds u64")
                    })?;
                    let requested_total =
                        charged_bytes.checked_add(requested).ok_or_else(|| {
                            invalid_input("schema-6 canonical string charge overflows")
                        })?;
                    if let Err(error) = charge.reconcile(requested_total) {
                        deferred_error = Some(Schema6SeriesReaderError::Cache(
                            MetadataCacheError::from(error),
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "schema-6 canonical string reservation was refused",
                        ));
                    }

                    let target = match canonical_component_mut(&mut values, *occurrence) {
                        Ok(target) => target,
                        Err(error) => {
                            deferred_error = Some(Schema6SeriesReaderError::Planning(error));
                            return Err(invalid_input(
                                "schema-6 canonical string occurrence is invalid",
                            ));
                        }
                    };
                    if let Err(error) = target.try_reserve_exact(value.len()) {
                        deferred_error = Some(Schema6SeriesReaderError::Planning(io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            format!("schema-6 canonical string allocation failed: {error}"),
                        )));
                        return Err(io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "schema-6 canonical string allocation failed",
                        ));
                    }
                    let actual = u64::try_from(target.capacity()).map_err(|_| {
                        invalid_input("schema-6 canonical string capacity exceeds u64")
                    })?;
                    let actual_total = charged_bytes.checked_add(actual).ok_or_else(|| {
                        invalid_input("schema-6 canonical string charge overflows")
                    })?;
                    if let Err(error) = charge.reconcile(actual_total) {
                        deferred_error = Some(Schema6SeriesReaderError::Cache(
                            MetadataCacheError::from(error),
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "schema-6 canonical string capacity reconciliation was refused",
                        ));
                    }
                    charged_bytes = actual_total;
                    target.push_str(value);
                }
                Ok(())
            });
        if let Some(error) = deferred_error {
            return Err(error);
        }
        if !visit_result? {
            return Err(self.record_series_error(invalid_data(
                "schema-6 required symbol resolution was incomplete",
            )));
        }

        drop(unique_symbol_ids);
        drop(occurrences);
        let final_bytes = verified_output_bytes(&values, values.capacity())?;
        charge
            .reconcile(final_bytes)
            .map_err(MetadataCacheError::from)?;
        for value in &values {
            let actual_series_id = canonical_label_identity(&value.labels);
            if actual_series_id != value.series_id {
                return Err(self.record_series_error(invalid_data(format!(
                    "schema-6 series identity mismatch: expected={} actual={actual_series_id}",
                    value.series_id
                ))));
            }
        }

        Ok(GovernedSchema6VerifiedSeriesBatch {
            provenance: self.guard.provenance(),
            values,
            _charge: charge,
        })
    }

    pub(crate) fn verified_series<'a>(
        &'a self,
        values: &'a GovernedSchema6VerifiedSeriesBatch,
    ) -> Result<&'a [Schema6VerifiedSeries], Schema6SeriesReaderError> {
        self.ensure_provenance(&values.provenance)?;
        Ok(&values.values)
    }
}

fn canonical_component_mut(
    values: &mut [Schema6VerifiedSeries],
    occurrence: SymbolOccurrence,
) -> io::Result<&mut String> {
    let label = values
        .get_mut(occurrence.series_index)
        .and_then(|value| value.labels.get_mut(occurrence.label_index))
        .ok_or_else(|| invalid_input("schema-6 canonical string occurrence is out of bounds"))?;
    Ok(match occurrence.component {
        CanonicalComponent::Name => &mut label.0,
        CanonicalComponent::Value => &mut label.1,
    })
}

fn canonical_label_identity(labels: &[(String, String)]) -> u64 {
    let mut hash = XxHash64::default();
    for (name, value) in labels {
        hash.update(name.as_bytes());
        hash.update(&[0]);
        hash.update(value.as_bytes());
        hash.update(&[0xff]);
    }
    hash.finish()
}

fn verified_output_bytes(
    values: &[Schema6VerifiedSeries],
    values_capacity: usize,
) -> io::Result<u64> {
    let mut bytes = checked_vec_bytes::<Schema6VerifiedSeries>(
        values_capacity,
        "schema-6 verified output charge overflows",
    )?;
    for value in values {
        bytes = checked_add_bytes(
            bytes,
            checked_vec_bytes::<(String, String)>(
                value.labels.capacity(),
                "schema-6 canonical-label vector charge overflows",
            )?,
            "schema-6 verified output charge overflows",
        )?;
        for (name, label_value) in &value.labels {
            let name_bytes = u64::try_from(name.capacity())
                .map_err(|_| invalid_input("schema-6 canonical label capacity exceeds u64"))?;
            let value_bytes = u64::try_from(label_value.capacity())
                .map_err(|_| invalid_input("schema-6 canonical label capacity exceeds u64"))?;
            bytes = checked_add_many_bytes(
                &[bytes, name_bytes, value_bytes],
                "schema-6 verified output charge overflows",
            )?;
        }
    }
    Ok(bytes)
}

fn checked_add_bytes(left: u64, right: u64, message: &'static str) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_input(message))
}

fn checked_add_many_bytes(values: &[u64], message: &'static str) -> io::Result<u64> {
    values.iter().try_fold(0u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| invalid_input(message))
    })
}

#[cfg(test)]
mod tests;
