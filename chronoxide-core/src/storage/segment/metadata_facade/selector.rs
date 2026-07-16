use super::super::{CompiledLabelMatcher, labels_match_compiled};
use super::*;

impl SegmentMetadataSession {
    /// Builds a governed, conservative candidate set from authenticated
    /// postings and label-value FSTs without decoding canonical series labels.
    ///
    /// Matchers that accept the missing-label value (`""`) deliberately keep
    /// all current candidates. [`Self::visit_matching_verified_series`] then
    /// applies every matcher to the authenticated canonical label row. This
    /// prevents an index absence from becoming an incorrect negative proof.
    pub(crate) fn select_matcher_candidates(
        &self,
        root: &SegmentMetadataRoot,
        matchers: &[CompiledLabelMatcher],
        time_range: Option<(u64, u64)>,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        if let Some((start_ms, end_ms)) = time_range {
            validate_time_range(start_ms, end_ms)?;
        }

        let mut candidates = None;
        for matcher in matchers {
            let positive = match matcher {
                CompiledLabelMatcher::Eq { name, value } if !value.is_empty() => {
                    Some(self.exact_value_ref_set(root, name, value, time_range)?)
                }
                CompiledLabelMatcher::Regex { name, pattern } if !pattern.is_match("") => {
                    Some(self.regex_value_ref_set(root, name, pattern, time_range)?)
                }
                CompiledLabelMatcher::Eq { .. }
                | CompiledLabelMatcher::NotEq { .. }
                | CompiledLabelMatcher::Regex { .. }
                | CompiledLabelMatcher::NotRegex { .. } => None,
            };
            if let Some(positive) = positive {
                candidates = Some(match candidates {
                    Some(current) => self.intersect_series_ref_sets(root, &current, &positive)?,
                    None => positive,
                });
                if candidates
                    .as_ref()
                    .is_some_and(GovernedSeriesRefSet::is_empty)
                {
                    return candidates.ok_or(SegmentMetadataFacadeError::RefSetSizeOverflow);
                }
            }
        }

        let mut candidates = match candidates {
            Some(candidates) => candidates,
            None => self.all_series_ref_set(root)?,
        };
        for matcher in matchers {
            let excluded = match matcher {
                CompiledLabelMatcher::NotEq { name, value } if !value.is_empty() => {
                    Some(self.exact_value_ref_set(root, name, value, time_range)?)
                }
                CompiledLabelMatcher::NotRegex { name, pattern } if !pattern.is_match("") => {
                    Some(self.regex_value_ref_set(root, name, pattern, time_range)?)
                }
                CompiledLabelMatcher::Eq { .. }
                | CompiledLabelMatcher::NotEq { .. }
                | CompiledLabelMatcher::Regex { .. }
                | CompiledLabelMatcher::NotRegex { .. } => None,
            };
            if let Some(excluded) = excluded {
                candidates = self.difference_series_ref_sets(root, &candidates, &excluded)?;
                if candidates.is_empty() {
                    break;
                }
            }
        }
        Ok(candidates)
    }

    /// Selects with governed index metadata, then materializes canonical
    /// labels only for the surviving refs and exposes exact authenticated
    /// chunk locators only after the complete matcher set succeeds.
    pub(crate) fn visit_matching_verified_series<E>(
        &self,
        root: &SegmentMetadataRoot,
        matchers: &[CompiledLabelMatcher],
        time_range: Option<(u64, u64)>,
        mut visitor: impl FnMut(SegmentVerifiedSeries<'_>) -> Result<SegmentMetadataVisitControl, E>,
    ) -> Result<SegmentMetadataVisitOutcome, SegmentMetadataVisitError<E>> {
        let candidates = self
            .select_matcher_candidates(root, matchers, time_range)
            .map_err(SegmentMetadataVisitError::Metadata)?;
        self.visit_verified_series(root, &candidates, |series| {
            if labels_match_compiled(series.labels(), matchers) {
                visitor(series)
            } else {
                Ok(SegmentMetadataVisitControl::Continue)
            }
        })
    }

    fn exact_value_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        label_name: &str,
        label_value: &str,
        time_range: Option<(u64, u64)>,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        let Some(label_name_sym) = self.lookup_symbol(root, label_name)? else {
            return self.series_ref_set(root, &[]);
        };
        let Some(label_value_sym) = self.lookup_symbol(root, label_value)? else {
            return self.series_ref_set(root, &[]);
        };
        self.exact_symbol_pair_ref_set(root, label_name_sym, label_value_sym, time_range)
    }

    fn exact_symbol_pair_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        label_name_sym: u32,
        label_value_sym: u32,
        time_range: Option<(u64, u64)>,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        let Some(selection) = self.select_exact_postings(root, label_name_sym, label_value_sym)?
        else {
            return self.series_ref_set(root, &[]);
        };
        if let Some((start_ms, end_ms)) = time_range
            && !self.exact_postings_overlaps(root, &selection, start_ms, end_ms)?
        {
            return self.series_ref_set(root, &[]);
        }
        let postings = self.read_exact_postings(root, &selection)?;
        self.exact_postings_ref_set(root, &postings)
    }

    fn regex_value_ref_set(
        &self,
        root: &SegmentMetadataRoot,
        label_name: &str,
        pattern: &regex::Regex,
        time_range: Option<(u64, u64)>,
    ) -> Result<GovernedSeriesRefSet, SegmentMetadataFacadeError> {
        let Some(label_name_sym) = self.lookup_symbol(root, label_name)? else {
            return self.series_ref_set(root, &[]);
        };
        let mut matched = self.series_ref_set(root, &[])?;
        let mut failure = None;
        self.visit_label_values(
            root,
            label_name_sym,
            None,
            time_range,
            |label_value_sym, label_value| {
                if !pattern.is_match(label_value) {
                    return true;
                }
                let next = self
                    .exact_symbol_pair_ref_set(root, label_name_sym, label_value_sym, time_range)
                    .and_then(|postings| self.union_series_ref_sets(root, &matched, &postings));
                match next {
                    Ok(union) => matched = union,
                    Err(error) => {
                        failure = Some(error);
                        return false;
                    }
                }
                true
            },
        )?;
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(matched)
    }
}
