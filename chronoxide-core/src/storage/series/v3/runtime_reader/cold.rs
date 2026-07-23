use super::*;

impl Schema7MetadataSession {
    pub(super) fn load_decoded_keyset_plan(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        keyset_id: u32,
    ) -> Result<DecodedKeysetPlan, Schema7MetadataReaderError> {
        let keyset = self.load_keyset(roots, keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let block = self.load_keyset_block(roots, keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        Ok(DecodedKeysetPlan { keyset, block })
    }

    pub(super) fn decode_encoded_labels(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        plan: &DecodedKeysetPlan,
        dictionaries: &mut Vec<(u32, GovernedDecodedVec<u32>)>,
        row_index: u32,
        encoded_labels: &mut Vec<(u32, u32)>,
    ) -> Result<(), Schema7MetadataReaderError> {
        let row = if plan.block.value.row_len_bytes == 0 {
            if row_index >= plan.block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &plan.block.value,
                row_index,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in plan.keyset.values.iter().copied().enumerate() {
            let dictionary_index =
                match dictionaries.binary_search_by_key(&key_sym, |(key_sym, _)| *key_sym) {
                    Ok(index) => index,
                    Err(insert_at) => {
                        let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
                        dictionaries.insert(insert_at, (key_sym, dictionary));
                        insert_at
                    }
                };
            let dictionary = &dictionaries[dictionary_index].1;
            let width = *plan.block.value.widths.get(index).ok_or_else(|| {
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
        Ok(())
    }

    /// Returns one logical cold range only after every intersecting physical
    /// page has been loaded, CRC-authenticated, and rebound to this root. A
    /// single-page range borrows its pinned page; only cross-page ranges need
    /// an owned assembly buffer.
    fn read_authenticated_cold_range(
        &self,
        roots: &BoundSchema7Roots,
        range: Range<u64>,
    ) -> Result<GovernedColdRange, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let header = roots.series_root().header;
        if range.start > range.end
            || range.start < header.keysets_offset
            || range.end > header.file_len
        {
            return Err(self.record_series_error(invalid_data(
                "schema-7 cold logical range is outside the cold stream",
            )));
        }
        let byte_len_u64 = range.end - range.start;
        let byte_len = usize::try_from(byte_len_u64)
            .map_err(|_| planning_error("schema-7 cold logical range length exceeds usize"))?;
        if range.is_empty() {
            return Ok(GovernedColdRange {
                bytes: GovernedColdRangeBytes::Empty,
                _charge: self.reserve_series_scratch(0)?,
            });
        }

        let first_page = (range.start - header.keysets_offset) / super::SERIES_COLD_PAGE_LEN_V1;
        let final_page = (range.end - 1 - header.keysets_offset) / super::SERIES_COLD_PAGE_LEN_V1;
        let page_count_u64 = final_page
            .checked_sub(first_page)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| planning_error("schema-7 cold page span overflows"))?;
        let page_count = usize::try_from(page_count_u64)
            .map_err(|_| planning_error("schema-7 cold page span exceeds usize"))?;
        if page_count == 1 {
            let page_index = u32::try_from(first_page)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            let descriptor = roots.cold_descriptor(page_index)?;
            let page = self.load_cold_page(roots, page_index)?;
            let page_bytes =
                self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
            let page_start = header
                .keysets_offset
                .checked_add(
                    first_page
                        .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                        .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
            let local_start = usize::try_from(range.start - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice start exceeds usize"))?;
            let local_end = usize::try_from(range.end - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice end exceeds usize"))?;
            if local_start > local_end
                || local_end > page_bytes.len()
                || local_end - local_start != byte_len
            {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 authenticated cold logical range is incomplete",
                )));
            }
            return Ok(GovernedColdRange {
                bytes: GovernedColdRangeBytes::Borrowed {
                    page,
                    header,
                    page_index,
                    descriptor,
                    range: local_start..local_end,
                },
                _charge: self.reserve_series_scratch(0)?,
            });
        }
        let declared = checked_vec_bytes::<MetadataCachePin<ValidatedSeriesColdPage>>(
            page_count,
            "schema-7 cold-page pin allocation charge overflows",
        )?
        .checked_add(byte_len_u64)
        .ok_or_else(|| planning_error("schema-7 cold-range allocation charge overflows"))?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let mut pages =
            try_vec_with_capacity(page_count, "schema-7 cold-page pin allocation failed")?;
        let mut bytes = try_vec_with_capacity(byte_len, "schema-7 cold-range allocation failed")?;
        charge
            .reconcile(
                checked_vec_bytes::<MetadataCachePin<ValidatedSeriesColdPage>>(
                    pages.capacity(),
                    "schema-7 cold-page pin allocation charge overflows",
                )?
                .checked_add(checked_vec_bytes::<u8>(
                    bytes.capacity(),
                    "schema-7 cold-range allocation charge overflows",
                )?)
                .ok_or_else(|| planning_error("schema-7 cold-range allocation charge overflows"))?,
            )
            .map_err(MetadataCacheError::from)?;

        for page_index in first_page..=final_page {
            let page_index = u32::try_from(page_index)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            pages.push(self.load_cold_page(roots, page_index)?);
        }

        // Rebind every pin before returning any byte from the logical range.
        // Only after all pages succeed do we copy their intersecting slices.
        for (ordinal, page) in pages.iter().enumerate() {
            let page_index_u64 = first_page
                .checked_add(
                    u64::try_from(ordinal)
                        .map_err(|_| planning_error("schema-7 cold page ordinal exceeds u64"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page index overflows"))?;
            let page_index = u32::try_from(page_index_u64)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            let descriptor = roots.cold_descriptor(page_index)?;
            let page_bytes =
                self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
            let page_start = header
                .keysets_offset
                .checked_add(
                    page_index_u64
                        .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                        .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
            let page_end = page_start
                .checked_add(
                    u64::try_from(page_bytes.len())
                        .map_err(|_| planning_error("schema-7 cold page length exceeds u64"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page end overflows"))?;
            let copy_start = range.start.max(page_start);
            let copy_end = range.end.min(page_end);
            if copy_start >= copy_end {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 authenticated cold page does not intersect its logical range",
                )));
            }
            let local_start = usize::try_from(copy_start - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice start exceeds usize"))?;
            let local_end = usize::try_from(copy_end - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice end exceeds usize"))?;
            bytes.extend_from_slice(&page_bytes[local_start..local_end]);
        }
        if bytes.len() != byte_len {
            return Err(self.record_series_error(invalid_data(
                "schema-7 authenticated cold logical range is incomplete",
            )));
        }
        drop(pages);
        charge
            .reconcile(checked_vec_bytes::<u8>(
                bytes.capacity(),
                "schema-7 cold-range allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedColdRange {
            bytes: GovernedColdRangeBytes::Owned(bytes),
            _charge: charge,
        })
    }

    /// Returns owned bytes before the caller starts nested metadata reads or
    /// reserves decoded-output scratch. Retaining a transient 16-KiB page pin
    /// across that work would raise the minimum viable in-flight budget versus
    /// the original copy-and-release path. Leaf parsers may borrow directly.
    pub(super) fn read_authenticated_cold_range_owned(
        &self,
        roots: &BoundSchema7Roots,
        range: Range<u64>,
    ) -> Result<GovernedColdRange, Schema7MetadataReaderError> {
        let governed = self.read_authenticated_cold_range(roots, range)?;
        let GovernedColdRange {
            bytes,
            _charge: existing_charge,
        } = governed;
        let GovernedColdRangeBytes::Borrowed {
            page,
            header,
            page_index,
            descriptor,
            range,
        } = bytes
        else {
            return Ok(GovernedColdRange {
                bytes,
                _charge: existing_charge,
            });
        };

        let byte_len = range.len();
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u8>(
            byte_len,
            "schema-7 owned cold-range allocation charge overflows",
        )?)?;
        let mut owned =
            try_vec_with_capacity(byte_len, "schema-7 owned cold-range allocation failed")?;
        charge
            .reconcile(checked_vec_bytes::<u8>(
                owned.capacity(),
                "schema-7 owned cold-range allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        let page_bytes =
            self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
        let source = page_bytes.get(range).ok_or_else(|| {
            self.record_series_error(invalid_data(
                "schema-7 authenticated cold logical range is incomplete",
            ))
        })?;
        owned.extend_from_slice(source);
        drop(existing_charge);
        drop(page);
        Ok(GovernedColdRange {
            bytes: GovernedColdRangeBytes::Owned(owned),
            _charge: charge,
        })
    }

    pub(super) fn load_keyset(
        &self,
        roots: &BoundSchema7Roots,
        keyset_id: u32,
    ) -> Result<GovernedDecodedVec<u32>, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let range = self.load_cold_entry_range(
            roots,
            ColdSection {
                offset: header.keysets_offset,
                end: header.value_dicts_offset,
                count: header.num_keysets,
            },
            keyset_id,
        )?;
        let bytes = self.read_authenticated_cold_range_owned(roots, range.clone())?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        let declared_count = bytes_slice.len() / std::mem::size_of::<u32>();
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u32>(
            declared_count,
            "schema-7 decoded keyset allocation charge overflows",
        )?)?;
        let values = self.record_series_result(cold_v2_reader::decode_keyset_entry(
            bytes_slice,
            range.start,
            range.end,
        ))?;
        charge
            .reconcile(checked_vec_bytes::<u32>(
                values.capacity(),
                "schema-7 decoded keyset allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedDecodedVec {
            values,
            _charge: charge,
        })
    }

    pub(super) fn load_keyset_block(
        &self,
        roots: &BoundSchema7Roots,
        keyset_id: u32,
    ) -> Result<GovernedBlockMeta, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let range = self.load_cold_entry_range(
            roots,
            ColdSection {
                offset: header.keyset_blocks_offset,
                end: header.file_len,
                count: header.num_keysets,
            },
            keyset_id,
        )?;
        let fixed_range = self.record_series_result(cold_v2_reader::keyset_block_header_range(
            range.start,
            range.end,
        ))?;
        let fixed = self.read_authenticated_cold_range_owned(roots, fixed_range)?;
        let fixed_slice = self.record_series_result(fixed.as_slice())?;
        let widths_range = self.record_series_result(cold_v2_reader::keyset_block_widths_range(
            fixed_slice,
            range.start,
            range.end,
        ))?;
        let widths = self.read_authenticated_cold_range_owned(roots, widths_range)?;
        let widths_slice = self.record_series_result(widths.as_slice())?;
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u8>(
            widths_slice.len(),
            "schema-7 decoded width allocation charge overflows",
        )?)?;
        let value = self.record_series_result(cold_v2_reader::decode_keyset_block_meta(
            fixed_slice,
            widths_slice,
            range.start,
            range.end,
        ))?;
        charge
            .reconcile(checked_vec_bytes::<u8>(
                value.widths.capacity(),
                "schema-7 decoded width allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedBlockMeta {
            value,
            _charge: charge,
        })
    }

    pub(super) fn find_value_dictionary(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        key_sym: u32,
    ) -> Result<GovernedDecodedVec<u32>, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let section = ColdSection {
            offset: header.value_dicts_offset,
            end: header.keyset_blocks_offset,
            count: header.num_value_dicts,
        };
        let mut low = 0u32;
        let mut high = section.count;
        while low < high {
            let mid = low + (high - low) / 2;
            let meta = self.load_value_dictionary_meta(roots, section, mid)?;
            match meta.value.key_sym.cmp(&key_sym) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    let values_range = meta.value.values_offset..meta.entry_end;
                    let bytes = self.read_authenticated_cold_range_owned(roots, values_range)?;
                    let declared_count = usize::try_from(meta.value.cardinality).map_err(|_| {
                        planning_error("schema-7 value dictionary count exceeds usize")
                    })?;
                    let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u32>(
                        declared_count,
                        "schema-7 value dictionary allocation charge overflows",
                    )?)?;
                    let bytes_slice = self.record_series_result(bytes.as_slice())?;
                    let values = self.record_series_result(
                        cold_v2_reader::decode_value_dict_values(bytes_slice, meta.value),
                    )?;
                    self.validate_value_dictionary(symbols, &values)?;
                    charge
                        .reconcile(checked_vec_bytes::<u32>(
                            values.capacity(),
                            "schema-7 value dictionary allocation charge overflows",
                        )?)
                        .map_err(MetadataCacheError::from)?;
                    return Ok(GovernedDecodedVec {
                        values,
                        _charge: charge,
                    });
                }
            }
        }
        Err(self.record_series_error(invalid_data("schema-7 value dictionary is missing")))
    }

    fn load_value_dictionary_meta(
        &self,
        roots: &BoundSchema7Roots,
        section: ColdSection,
        dict_id: u32,
    ) -> Result<ValueDictEntryMeta, Schema7MetadataReaderError> {
        let range = self.load_cold_entry_range(roots, section, dict_id)?;
        let header_range = self.record_series_result(cold_v2_reader::value_dict_header_range(
            range.start,
            range.end,
        ))?;
        let bytes = self.read_authenticated_cold_range(roots, header_range)?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        let value = self.record_series_result(cold_v2_reader::decode_value_dict_meta(
            bytes_slice,
            range.start,
            range.end,
        ))?;
        Ok(ValueDictEntryMeta {
            value,
            entry_end: range.end,
        })
    }

    fn load_cold_entry_range(
        &self,
        roots: &BoundSchema7Roots,
        section: ColdSection,
        entry_index: u32,
    ) -> Result<Range<u64>, Schema7MetadataReaderError> {
        let pair_range = self.record_series_result(cold_v2_reader::offset_pair_range(
            section.offset,
            section.end,
            section.count,
            entry_index,
        ))?;
        let bytes = self.read_authenticated_cold_range(roots, pair_range)?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        self.record_series_result(cold_v2_reader::decode_entry_range(
            bytes_slice,
            section.offset,
            section.end,
            section.count,
            entry_index,
        ))
    }

    fn validate_value_dictionary(
        &self,
        symbols: &GovernedSymbolSession,
        values: &[u32],
    ) -> Result<(), Schema7MetadataReaderError> {
        let mut previous = None;
        for &value in values {
            if previous.is_some_and(|previous| previous >= value) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 value dictionary symbols are not strictly increasing",
                )));
            }
            if usize::try_from(value).map_or(true, |value| value >= symbols.len()) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 value dictionary symbol exceeds the bound symbol count",
                )));
            }
            previous = Some(value);
        }
        Ok(())
    }

    pub(super) fn validate_key_symbols(
        &self,
        symbols: &GovernedSymbolSession,
        key_symbols: &[u32],
    ) -> Result<(), Schema7MetadataReaderError> {
        for &key_sym in key_symbols {
            if usize::try_from(key_sym).map_or(true, |key_sym| key_sym >= symbols.len()) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 key symbol exceeds the bound symbol count",
                )));
            }
        }
        Ok(())
    }
}
