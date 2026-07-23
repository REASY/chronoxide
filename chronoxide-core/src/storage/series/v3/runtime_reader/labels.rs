use super::*;

impl Schema7MetadataSession {
    pub(super) fn materialize_and_verify_canonical_labels<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        encoded_labels: &[(u32, u32)],
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<MaterializedCanonicalLabels, Schema7MetadataReaderError> {
        let label_construction_started = detailed_stage_started::<DETAILED>();
        let output_capacity = selection.output_capacity(encoded_labels.len());
        let declared = checked_vec_bytes::<(String, String)>(
            output_capacity,
            "schema-7 canonical-label vector charge overflows",
        )?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let mut labels = try_vec_with_capacity(
            output_capacity,
            "schema-7 canonical-label vector allocation failed",
        )?;
        let mut charged_bytes = checked_vec_bytes::<(String, String)>(
            labels.capacity(),
            "schema-7 canonical-label vector charge overflows",
        )?;
        charge
            .reconcile(charged_bytes)
            .map_err(MetadataCacheError::from)?;
        if DETAILED {
            materialization_profile.label_construction =
                materialization_profile.label_construction.saturating_add(
                    label_construction_started
                        .expect("detailed label-construction timer exists")
                        .elapsed(),
                );
        }
        let mut hash = XxHash64::default();
        let mut metric_name_dropped_hash = selection
            .derives_metric_name_dropped_identity()
            .then(XxHash64::default);
        for &(key_sym, value_sym) in encoded_labels {
            let mut include_in_metric_name_dropped_identity = true;
            let key = self.resolve_canonical_component::<DETAILED>(
                symbols,
                key_sym,
                0,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
                materialization_profile,
                "schema-7 canonical label-name allocation failed",
                |resolved| {
                    include_in_metric_name_dropped_identity = resolved != METRIC_NAME_LABEL;
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0]);
                    }
                    selection.includes(resolved)
                },
            )?;
            let selected = key.is_some();
            let value = self.resolve_canonical_component::<DETAILED>(
                symbols,
                value_sym,
                0xff,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
                materialization_profile,
                "schema-7 canonical label-value allocation failed",
                |resolved| {
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0xff]);
                    }
                    selected
                },
            )?;
            let construction_started = detailed_stage_started::<DETAILED>();
            match (key, value) {
                (Some(key), Some(value)) => labels.push((key, value)),
                (None, None) => {}
                _ => unreachable!("label name and value ownership selection must stay aligned"),
            }
            if DETAILED {
                materialization_profile.label_construction =
                    materialization_profile.label_construction.saturating_add(
                        construction_started
                            .expect("detailed label-push timer exists")
                            .elapsed(),
                    );
            }
        }
        let identity_started = detailed_stage_started::<DETAILED>();
        let actual_series_id = hash.finish();
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    identity_started
                        .expect("detailed identity timer exists")
                        .elapsed(),
                );
        }
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "schema-7 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
                ),
            )));
        }
        debug_assert_eq!(charge.bytes(), charged_bytes);
        let secondary_identity_started = detailed_stage_started::<DETAILED>();
        let metric_name_dropped_series_id = metric_name_dropped_hash.map(|hash| hash.finish());
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    secondary_identity_started
                        .expect("detailed secondary-identity timer exists")
                        .elapsed(),
                );
        }
        Ok((labels, charge, metric_name_dropped_series_id))
    }

    /// Verifies the complete canonical row without allocating per-component
    /// strings, then compacts the source-ID vector to the requested labels.
    /// Compaction may overwrite already-verified entries, but the shortened
    /// row is not exposed unless all later symbols and the final identity also
    /// succeed.
    pub(super) fn verify_and_select_encoded_labels<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        encoded_labels: &mut Vec<(u32, u32)>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<Option<u64>, Schema7MetadataReaderError> {
        let mut hash = XxHash64::default();
        let mut metric_name_dropped_hash = selection
            .derives_metric_name_dropped_identity()
            .then(XxHash64::default);
        let mut output_len = 0usize;

        for read_index in 0..encoded_labels.len() {
            let (key_sym, value_sym) = encoded_labels[read_index];
            let mut selected = false;
            let mut include_in_metric_name_dropped_identity = true;
            self.visit_encoded_canonical_component::<DETAILED>(
                symbols,
                key_sym,
                0,
                &mut hash,
                materialization_profile,
                |resolved| {
                    include_in_metric_name_dropped_identity = resolved != METRIC_NAME_LABEL;
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0]);
                    }
                    selected = selection.includes(resolved);
                },
            )?;
            self.visit_encoded_canonical_component::<DETAILED>(
                symbols,
                value_sym,
                0xff,
                &mut hash,
                materialization_profile,
                |resolved| {
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0xff]);
                    }
                },
            )?;
            if selected {
                let construction_started = detailed_stage_started::<DETAILED>();
                encoded_labels[output_len] = (key_sym, value_sym);
                output_len += 1;
                if DETAILED {
                    materialization_profile.label_construction =
                        materialization_profile.label_construction.saturating_add(
                            construction_started
                                .expect("detailed encoded-label timer exists")
                                .elapsed(),
                        );
                }
            }
        }

        let identity_started = detailed_stage_started::<DETAILED>();
        let actual_series_id = hash.finish();
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    identity_started
                        .expect("detailed encoded-identity timer exists")
                        .elapsed(),
                );
        }
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "schema-7 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
                ),
            )));
        }

        let secondary_identity_started = detailed_stage_started::<DETAILED>();
        let metric_name_dropped_series_id = metric_name_dropped_hash.map(|hash| hash.finish());
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    secondary_identity_started
                        .expect("detailed encoded secondary-identity timer exists")
                        .elapsed(),
                );
        }
        encoded_labels.truncate(output_len);
        Ok(metric_name_dropped_series_id)
    }

    fn visit_encoded_canonical_component<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        symbol_id: u32,
        delimiter: u8,
        hash: &mut XxHash64,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
        visit_resolved: impl FnOnce(&str),
    ) -> Result<(), Schema7MetadataReaderError> {
        let resolution_started = detailed_stage_started::<DETAILED>();
        let identity_before = materialization_profile.canonical_identity;
        let visit = symbols.visit_required_resolved(symbol_id, |resolved| {
            let identity_started = detailed_stage_started::<DETAILED>();
            hash.update(resolved.as_bytes());
            hash.update(&[delimiter]);
            visit_resolved(resolved);
            if DETAILED {
                materialization_profile.canonical_identity =
                    materialization_profile.canonical_identity.saturating_add(
                        identity_started
                            .expect("detailed encoded component timer exists")
                            .elapsed(),
                    );
            }
            Ok(())
        });
        if DETAILED {
            let identity_elapsed = materialization_profile
                .canonical_identity
                .saturating_sub(identity_before);
            materialization_profile.symbol_resolution =
                materialization_profile.symbol_resolution.saturating_add(
                    resolution_started
                        .expect("detailed encoded resolution timer exists")
                        .elapsed()
                        .saturating_sub(identity_elapsed),
                );
        }
        visit.map_err(Schema7MetadataReaderError::from)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_canonical_component<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        symbol_id: u32,
        delimiter: u8,
        hash: &mut XxHash64,
        charge: &mut MetadataCharge,
        charged_bytes: &mut u64,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
        allocation_message: &'static str,
        should_own: impl FnOnce(&str) -> bool,
    ) -> Result<Option<String>, Schema7MetadataReaderError> {
        let resolution_started = detailed_stage_started::<DETAILED>();
        let identity_before = materialization_profile.canonical_identity;
        let construction_before = materialization_profile.label_construction;
        let mut owned = None;
        let mut deferred_error = None;
        let visit = symbols.visit_required_resolved(symbol_id, |resolved| {
            let identity_started = detailed_stage_started::<DETAILED>();
            hash.update(resolved.as_bytes());
            hash.update(&[delimiter]);
            let should_own = should_own(resolved);
            if DETAILED {
                materialization_profile.canonical_identity =
                    materialization_profile.canonical_identity.saturating_add(
                        identity_started
                            .expect("detailed identity timer exists")
                            .elapsed(),
                    );
            }
            let construction_started = detailed_stage_started::<DETAILED>();
            if !should_own {
                if DETAILED {
                    materialization_profile.label_construction =
                        materialization_profile.label_construction.saturating_add(
                            construction_started
                                .expect("detailed label-construction timer exists")
                                .elapsed(),
                        );
                }
                return Ok(());
            }
            let requested_bytes = u64::try_from(resolved.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical label length exceeds u64",
                )
            })?;
            let requested_total =
                (*charged_bytes)
                    .checked_add(requested_bytes)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "schema-7 canonical-label charge overflows",
                        )
                    })?;
            if let Err(error) = charge.reconcile(requested_total) {
                deferred_error = Some(Schema7MetadataReaderError::Cache(MetadataCacheError::from(
                    error,
                )));
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label reservation was refused",
                ));
            }
            let mut value = String::new();
            value
                .try_reserve_exact(resolved.len())
                .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, allocation_message))?;
            let actual_bytes = u64::try_from(value.capacity()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical label capacity exceeds u64",
                )
            })?;
            let actual_total = (*charged_bytes).checked_add(actual_bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label charge overflows",
                )
            })?;
            if let Err(error) = charge.reconcile(actual_total) {
                deferred_error = Some(Schema7MetadataReaderError::Cache(MetadataCacheError::from(
                    error,
                )));
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label capacity reconciliation was refused",
                ));
            }
            *charged_bytes = actual_total;
            value.push_str(resolved);
            owned = Some(value);
            if DETAILED {
                materialization_profile.label_construction =
                    materialization_profile.label_construction.saturating_add(
                        construction_started
                            .expect("detailed label-construction timer exists")
                            .elapsed(),
                    );
            }
            Ok(())
        });
        if DETAILED {
            let attributed_in_callback = materialization_profile
                .canonical_identity
                .saturating_sub(identity_before)
                .saturating_add(
                    materialization_profile
                        .label_construction
                        .saturating_sub(construction_before),
                );
            materialization_profile.symbol_resolution =
                materialization_profile.symbol_resolution.saturating_add(
                    resolution_started
                        .expect("detailed symbol-resolution timer exists")
                        .elapsed()
                        .saturating_sub(attributed_in_callback),
                );
        }
        if let Some(error) = deferred_error {
            return Err(error);
        }
        visit?;
        Ok(owned)
    }
}
