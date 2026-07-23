use super::*;

impl Schema7MetadataSession {
    /// Materializes one complete v2 cold-label row and exposes its stable
    /// identity only after the same-generation symbol bytes reproduce the
    /// fingerprint stored in the authenticated hot record.
    pub(crate) fn materialize_verified(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_with_selection::<false>(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    /// Integrity-checks the complete canonical label row and stable identity,
    /// but owns only labels whose names were requested by the caller.
    pub(crate) fn materialize_verified_selected(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_with_selection::<false>(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    fn materialize_verified_with_selection<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;

        let keyset = self.load_keyset(roots, planned.cold_labels.keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            keyset.values.len(),
            "schema-7 materialized-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            keyset.values.len(),
            "schema-7 materialized-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 materialized-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        let block = self.load_keyset_block(roots, planned.cold_labels.keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        let row = if block.value.row_len_bytes == 0 {
            if planned.cold_labels.row >= block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &block.value,
                planned.cold_labels.row,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in keyset.values.iter().copied().enumerate() {
            let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
            let width = *block.value.widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-7 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                dictionary.values.len() as u32,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = dictionary
                .values
                .get(usize::try_from(code).map_err(|_| {
                    self.record_series_error(invalid_data("schema-7 value code exceeds usize"))
                })?)
                .copied()
                .ok_or_else(|| {
                    self.record_series_error(invalid_data("schema-7 value code is out of bounds"))
                })?;
            encoded_labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(self
                .record_series_error(invalid_data("schema-7 series cold row has trailing bytes")));
        }
        let (labels, output_charge, metric_name_dropped_series_id) = self
            .materialize_and_verify_canonical_labels::<DETAILED>(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
                materialization_profile,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );

        Ok(GovernedVerifiedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels,
            _charge: output_charge,
        })
    }

    fn materialize_verified_encoded_with_selection<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;

        let keyset = self.load_keyset(roots, planned.cold_labels.keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            keyset.values.len(),
            "schema-7 encoded-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            keyset.values.len(),
            "schema-7 encoded-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 encoded-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        let block = self.load_keyset_block(roots, planned.cold_labels.keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        let row = if block.value.row_len_bytes == 0 {
            if planned.cold_labels.row >= block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &block.value,
                planned.cold_labels.row,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in keyset.values.iter().copied().enumerate() {
            let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
            let width = *block.value.widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-7 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                u32::try_from(dictionary.values.len()).map_err(|_| {
                    self.record_series_error(invalid_data(
                        "schema-7 value dictionary length exceeds u32",
                    ))
                })?,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = dictionary
                .values
                .get(usize::try_from(code).map_err(|_| {
                    self.record_series_error(invalid_data("schema-7 value code exceeds usize"))
                })?)
                .copied()
                .ok_or_else(|| {
                    self.record_series_error(invalid_data("schema-7 value code is out of bounds"))
                })?;
            encoded_labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(self
                .record_series_error(invalid_data("schema-7 series cold row has trailing bytes")));
        }
        let integrity_checked_label_count = encoded_labels.len();
        let metric_name_dropped_series_id = self.verify_and_select_encoded_labels::<DETAILED>(
            symbols,
            planned.expected_label_identity,
            &mut encoded_labels,
            selection,
            materialization_profile,
        )?;
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );

        Ok(GovernedVerifiedEncodedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels: encoded_labels,
            _charge: encoded_charge,
        })
    }

    /// Creates best-effort lazy reuse state for one planned hot-page batch.
    /// If its fixed bookkeeping reservation cannot fit, materialization remains
    /// correct and falls back to the scalar path instead of failing the query.
    pub(crate) fn materialization_context(
        &self,
        roots: &BoundSchema7Roots,
        planned_capacity: usize,
    ) -> Result<Schema7MaterializationContext, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let provenance = self.guard.provenance();
        let dictionary_capacity = usize::try_from(roots.series_root().header.num_value_dicts)
            .map_err(|_| planning_error("schema-7 value dictionary count exceeds usize"))?;
        let cache = match self.try_materialization_cache(planned_capacity, dictionary_capacity) {
            Ok(cache) => Some(cache),
            Err(error) if is_optional_materialization_cache_error(&error) => None,
            Err(error) => return Err(error),
        };
        Ok(Schema7MaterializationContext { provenance, cache })
    }

    fn try_materialization_cache(
        &self,
        planned_capacity: usize,
        dictionary_capacity: usize,
    ) -> Result<Schema7MaterializationCache, Schema7MetadataReaderError> {
        let declared = checked_add_bytes(
            checked_vec_bytes::<(u32, DecodedKeysetPlan)>(
                planned_capacity,
                "schema-7 decoded keyset-plan allocation charge overflows",
            )?,
            checked_vec_bytes::<(u32, GovernedDecodedVec<u32>)>(
                dictionary_capacity,
                "schema-7 decoded dictionary-cache allocation charge overflows",
            )?,
            "schema-7 materialization-cache allocation charge overflows",
        )?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let plans = try_vec_with_capacity(
            planned_capacity,
            "schema-7 decoded keyset-plan allocation failed",
        )?;
        let dictionaries = try_vec_with_capacity(
            dictionary_capacity,
            "schema-7 decoded dictionary-cache allocation failed",
        )?;
        charge
            .reconcile(checked_add_bytes(
                checked_vec_bytes::<(u32, DecodedKeysetPlan)>(
                    plans.capacity(),
                    "schema-7 decoded keyset-plan allocation charge overflows",
                )?,
                checked_vec_bytes::<(u32, GovernedDecodedVec<u32>)>(
                    dictionaries.capacity(),
                    "schema-7 decoded dictionary-cache allocation charge overflows",
                )?,
                "schema-7 materialization-cache allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(Schema7MaterializationCache {
            plans,
            dictionaries,
            _charge: charge,
        })
    }

    /// Materializes only the current series while retaining already decoded
    /// shared cold metadata for later series. A visitor that stops after this
    /// value therefore cannot observe corruption or I/O belonging exclusively
    /// to an unvisited later row.
    pub(crate) fn materialize_verified_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    /// Cached counterpart to [`Self::materialize_verified_selected`]. Shared
    /// cold metadata remains reusable while omitted labels are still decoded,
    /// integrity-checked, and included in stable-identity verification.
    pub(crate) fn materialize_verified_selected_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<ProfiledGovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    pub(crate) fn materialize_verified_selected_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<ProfiledGovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    /// Compact-label counterpart to [`Self::materialize_verified_cached`].
    /// The complete row and all symbol bytes are authenticated before source
    /// symbol IDs are exposed to the generation-bound facade.
    pub(crate) fn materialize_verified_encoded_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_encoded_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_encoded_selected_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_encoded_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_encoded_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<ProfiledGovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_encoded_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    pub(crate) fn materialize_verified_encoded_selected_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<ProfiledGovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_encoded_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    fn materialize_verified_selected_cached_impl<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(&context.provenance)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;
        self.guard.reader(SegmentFile::Series)?.check_artifact()?;
        let Some(cache) = context.cache.as_ref() else {
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        };
        let keyset_id = planned.cold_labels.keyset_id;
        if cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
            .is_err()
            && cache.plans.len() == cache.plans.capacity()
        {
            context.cache = None;
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }

        let result = match context.cache.as_mut() {
            Some(cache) => self.materialize_verified_with_cache::<DETAILED>(
                roots, symbols, cache, planned, selection, profile,
            ),
            None => {
                return self.materialize_verified_with_selection::<DETAILED>(
                    roots, symbols, planned, selection, profile,
                );
            }
        };
        if result.as_ref().is_err_and(is_budget_error) {
            // Cached decoded values are an optimization, not semantic state.
            // Release them before retrying the established scalar path so a
            // tight in-flight budget does not become a new query failure.
            context.cache = None;
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }
        result.inspect(|_| {
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
        })
    }

    fn materialize_verified_encoded_selected_cached_impl<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(&context.provenance)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;
        self.guard.reader(SegmentFile::Series)?.check_artifact()?;
        let Some(cache) = context.cache.as_ref() else {
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        };
        let keyset_id = planned.cold_labels.keyset_id;
        if cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
            .is_err()
            && cache.plans.len() == cache.plans.capacity()
        {
            context.cache = None;
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }

        let result = match context.cache.as_mut() {
            Some(cache) => self.materialize_verified_encoded_with_cache::<DETAILED>(
                roots, symbols, cache, planned, selection, profile,
            ),
            None => {
                return self.materialize_verified_encoded_with_selection::<DETAILED>(
                    roots, symbols, planned, selection, profile,
                );
            }
        };
        if result.as_ref().is_err_and(is_budget_error) {
            context.cache = None;
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }
        result.inspect(|_| {
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
        })
    }

    fn materialize_verified_with_cache<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        cache: &mut Schema7MaterializationCache,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        let keyset_id = planned.cold_labels.keyset_id;
        let plan_index = match cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
        {
            Ok(index) => index,
            Err(index) => {
                let plan = self.load_decoded_keyset_plan(roots, symbols, keyset_id)?;
                cache.plans.insert(index, (keyset_id, plan));
                index
            }
        };
        let plan = &cache.plans[plan_index].1;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            plan.keyset.values.len(),
            "schema-7 materialized-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            plan.keyset.values.len(),
            "schema-7 materialized-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 materialized-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        self.decode_encoded_labels(
            roots,
            symbols,
            plan,
            &mut cache.dictionaries,
            planned.cold_labels.row,
            &mut encoded_labels,
        )?;
        let (labels, output_charge, metric_name_dropped_series_id) = self
            .materialize_and_verify_canonical_labels::<DETAILED>(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
                materialization_profile,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );
        Ok(GovernedVerifiedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels,
            _charge: output_charge,
        })
    }

    fn materialize_verified_encoded_with_cache<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        cache: &mut Schema7MaterializationCache,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        let keyset_id = planned.cold_labels.keyset_id;
        let plan_index = match cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
        {
            Ok(index) => index,
            Err(index) => {
                let plan = self.load_decoded_keyset_plan(roots, symbols, keyset_id)?;
                cache.plans.insert(index, (keyset_id, plan));
                index
            }
        };
        let plan = &cache.plans[plan_index].1;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            plan.keyset.values.len(),
            "schema-7 encoded-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            plan.keyset.values.len(),
            "schema-7 encoded-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 encoded-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        self.decode_encoded_labels(
            roots,
            symbols,
            plan,
            &mut cache.dictionaries,
            planned.cold_labels.row,
            &mut encoded_labels,
        )?;
        let integrity_checked_label_count = encoded_labels.len();
        let metric_name_dropped_series_id = self.verify_and_select_encoded_labels::<DETAILED>(
            symbols,
            planned.expected_label_identity,
            &mut encoded_labels,
            selection,
            materialization_profile,
        )?;
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );
        Ok(GovernedVerifiedEncodedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels: encoded_labels,
            _charge: encoded_charge,
        })
    }
}
