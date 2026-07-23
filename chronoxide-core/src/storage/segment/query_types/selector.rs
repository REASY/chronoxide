use super::super::{
    Arc, BTreeMap, BTreeSet, ExactPostingsMetadata, METRIC_NAME_LABEL, normalize_matcher_name,
    normalize_matcher_name_value, normalize_metric_name,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct MetadataAccumulator {
    metric_names: BTreeSet<String>,
    label_names: BTreeSet<String>,
    label_values: BTreeMap<String, BTreeSet<String>>,
}

impl MetadataAccumulator {
    pub(crate) fn add_label_name(&mut self, name: String) {
        self.label_names.insert(name);
    }

    pub(crate) fn add_label_value(&mut self, name: String, value: String) {
        self.label_names.insert(name.clone());
        self.label_values
            .entry(name.clone())
            .or_default()
            .insert(value.clone());
        if name == METRIC_NAME_LABEL {
            self.metric_names.insert(value);
        }
    }

    pub(crate) fn add_labelset(&mut self, labels: &[(String, String)]) {
        for (name, value) in labels {
            self.add_label_value(name.clone(), value.clone());
        }
    }

    pub(crate) fn metric_names(&self) -> Vec<String> {
        self.metric_names.iter().cloned().collect()
    }

    pub(crate) fn label_names(&self) -> Vec<String> {
        self.label_names.iter().cloned().collect()
    }

    pub(crate) fn label_values(&self, label_name: &str) -> Vec<String> {
        self.label_values
            .get(label_name)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

impl LabelMatcher {
    pub fn eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Eq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn not_eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::NotEq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::Regex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }

    pub fn not_regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::NotRegex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentSelector {
    pub(in crate::storage::segment) metric_name: Option<String>,
    pub(in crate::storage::segment) matchers: Vec<LabelMatcher>,
    pub(in crate::storage::segment) projection: SegmentProjection,
    pub(in crate::storage::segment) label_demand: QueryLabelDemand,
}

/// Internal ownership demand for labels consumed by a terminal aggregation.
/// Raw selector APIs always use `Full`; an `Include` value must not escape the
/// aggregation execution that created it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::storage::segment) enum QueryLabelDemand {
    #[default]
    Full,
    Include {
        /// Superset required while verifying matchers and projection kind.
        names: Arc<[String]>,
        /// Exact names observable from the terminal aggregation input.
        output_names: Arc<[String]>,
        derive_metric_name_dropped_identity: bool,
    },
}

/// Controls whether query planning may reduce owned source labels to the set
/// proven observable by the expression. `Full` is retained for one-binary
/// semantic and performance comparisons; it is not an error-recovery path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryLabelMaterializationPolicy {
    #[default]
    DemandDriven,
    Full,
}

/// Controls observer-heavy, fine-grained query stage timing for one session.
///
/// Production sessions default to `Off`. `Detailed` is intended for isolated
/// diagnostic runs because it reads the monotonic clock inside hot row and
/// symbol-processing paths and can materially perturb broad scans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryInstrumentationMode {
    #[default]
    Off,
    Detailed,
}

/// Selects the top-level PromQL range-query executor for one sealed-store
/// session.
///
/// `OnePassAssumeScalar` is deliberately named as a diagnostic assumption:
/// PromQL syntax does not prove that an exact metric name has only physical
/// Float/Int64 chunks. The comparator performs one `AllPromql` union read and
/// uses the one-pass scalar evaluator only when that read observes no typed
/// chunks. Observed Histogram, ExponentialHistogram, or Summary input fails
/// explicitly instead of omitting typed output or retrying after contaminating
/// caches/profiles. This post-decode check is not a pre-allocation governor and
/// is not a production eligibility proof.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RangeExecutionMode {
    #[default]
    Repeated,
    OnePassAssumeScalar,
}

impl RangeExecutionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Repeated => "repeated",
            Self::OnePassAssumeScalar => "one_pass_assume_scalar",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeExecutionFallbackReason {
    FiniteLimits,
    UnsupportedRootExpression,
    UnsupportedAggregation,
    UnsupportedGrouping,
    UnsupportedRangeFunction,
    MissingDirectMetricName,
    ProjectionLikeMetricName,
    UnsupportedProjection,
    StepExceedsWindow,
    InvalidQuery,
}

impl RangeExecutionFallbackReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FiniteLimits => "finite_limits",
            Self::UnsupportedRootExpression => "unsupported_root_expression",
            Self::UnsupportedAggregation => "unsupported_aggregation",
            Self::UnsupportedGrouping => "unsupported_grouping",
            Self::UnsupportedRangeFunction => "unsupported_range_function",
            Self::MissingDirectMetricName => "missing_direct_metric_name",
            Self::ProjectionLikeMetricName => "projection_like_metric_name",
            Self::UnsupportedProjection => "unsupported_projection",
            Self::StepExceedsWindow => "step_exceeds_window",
            Self::InvalidQuery => "invalid_query",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeExecutionTerminalReason {
    TypedSourceObservedAfterDecode,
}

impl RangeExecutionTerminalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TypedSourceObservedAfterDecode => "typed_source_observed_after_decode",
        }
    }
}

/// Finalized telemetry for the most recent top-level PromQL range-query call.
///
/// The retained-byte value is a post-decode estimate over the union result and
/// current sliced-window owned vectors. Shared labels, allocator slack, and
/// final output ownership are outside that estimate.
/// `preallocation_governed == false` is a capability statement: the current
/// selector API allocates those vectors before the comparator can inspect or
/// account for them. It must not be presented as an admission budget or lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeExecutionSummary {
    pub requested_mode: RangeExecutionMode,
    pub effective_mode: RangeExecutionMode,
    pub fallback_reason: Option<RangeExecutionFallbackReason>,
    pub terminal_reason: Option<RangeExecutionTerminalReason>,
    pub evaluation_count: u64,
    pub union_start_ms: Option<u64>,
    pub union_end_ms: Option<u64>,
    pub source_series: u64,
    pub source_samples: u64,
    pub estimated_retained_bytes_peak: u64,
    pub retained_bytes_after_finalize: u64,
    pub preallocation_governed: bool,
    pub cache_bypassed: bool,
}

impl RangeExecutionSummary {
    pub(crate) const fn new(requested_mode: RangeExecutionMode) -> Self {
        Self {
            requested_mode,
            effective_mode: RangeExecutionMode::Repeated,
            fallback_reason: None,
            terminal_reason: None,
            evaluation_count: 0,
            union_start_ms: None,
            union_end_ms: None,
            source_series: 0,
            source_samples: 0,
            estimated_retained_bytes_peak: 0,
            retained_bytes_after_finalize: 0,
            preallocation_governed: false,
            cache_bypassed: false,
        }
    }
}

impl QueryLabelDemand {
    pub(in crate::storage::segment) fn included_names(&self) -> Option<&[String]> {
        match self {
            Self::Full => None,
            Self::Include { names, .. } => Some(names),
        }
    }

    pub(in crate::storage::segment) fn derives_metric_name_dropped_identity(&self) -> bool {
        matches!(
            self,
            Self::Include {
                derive_metric_name_dropped_identity: true,
                ..
            }
        )
    }

    pub(in crate::storage::segment) fn output_names_arc(&self) -> Option<Arc<[String]>> {
        match self {
            Self::Full => None,
            Self::Include { output_names, .. } => Some(Arc::clone(output_names)),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum SegmentProjection {
    #[default]
    None,
    AllPromql {
        exponential_histogram_boundaries: Vec<f64>,
    },
    Count,
    Sum,
    HistogramBucket {
        le: BucketLeFilter,
        exponential_histogram_boundaries: Vec<f64>,
    },
    NativeHistogram,
    NativeExponentialHistogram,
    SummaryQuantile {
        quantile: Option<String>,
    },
}

impl SegmentProjection {
    pub(crate) fn needs_delta_projection_seed(&self) -> bool {
        matches!(
            self,
            SegmentProjection::Count
                | SegmentProjection::Sum
                | SegmentProjection::HistogramBucket { .. }
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum BucketLeFilter {
    #[default]
    All,
    Exact(String),
    Matchers(Vec<BucketLeMatcher>),
}

impl BucketLeFilter {
    pub(crate) fn from_matchers(matchers: Vec<BucketLeMatcher>) -> Self {
        match matchers.as_slice() {
            [] => Self::All,
            [BucketLeMatcher::Eq(value)] => Self::Exact(value.clone()),
            _ => Self::Matchers(matchers),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketLeMatcher {
    Eq(String),
    NotEq(String),
    Regex(String),
    NotRegex(String),
}

impl SegmentSelector {
    pub fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub fn metric(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub fn with_metric(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub(in crate::storage::segment) fn with_projection(
        mut self,
        projection: SegmentProjection,
    ) -> Self {
        self.projection = projection;
        self
    }

    pub(in crate::storage::segment) fn with_terminal_aggregation_label_demand(
        mut self,
        grouping_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Self {
        let mut output_names = grouping_names.to_vec();
        if derive_metric_name_dropped_identity {
            output_names.retain(|name| name != METRIC_NAME_LABEL);
        }
        output_names.sort_unstable();
        output_names.dedup();
        let normalized_matchers = self.normalized_matchers();
        let mut names = Vec::with_capacity(
            grouping_names
                .len()
                .saturating_add(normalized_matchers.len())
                .saturating_add(1),
        );
        names.extend(grouping_names.iter().cloned());
        names.extend(
            normalized_matchers
                .into_iter()
                .map(|matcher| match matcher {
                    NormalizedMatcher::Eq { name, .. }
                    | NormalizedMatcher::NotEq { name, .. }
                    | NormalizedMatcher::Regex { name, .. }
                    | NormalizedMatcher::NotRegex { name, .. } => name,
                }),
        );
        // Matchers and typed/scalar branch selection inspect the physical
        // metric name before a range function is allowed to remove it.
        names.push(METRIC_NAME_LABEL.to_string());
        names.sort_unstable();
        names.dedup();
        self.label_demand = QueryLabelDemand::Include {
            names: Arc::from(names.into_boxed_slice()),
            output_names: Arc::from(output_names.into_boxed_slice()),
            derive_metric_name_dropped_identity,
        };
        self
    }

    pub(in crate::storage::segment) fn label_demand(&self) -> &QueryLabelDemand {
        &self.label_demand
    }

    pub(crate) fn projection(&self) -> &SegmentProjection {
        &self.projection
    }

    pub(crate) fn normalized_matchers(&self) -> Vec<NormalizedMatcher> {
        let mut normalized = Vec::with_capacity(self.matchers.len() + 1);
        if let Some(metric_name) = &self.metric_name {
            normalized.push(NormalizedMatcher::Eq {
                name: METRIC_NAME_LABEL.to_string(),
                value: normalize_metric_name(metric_name),
            });
        }

        for matcher in &self.matchers {
            match matcher {
                LabelMatcher::Eq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::Eq { name, value });
                }
                LabelMatcher::NotEq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::NotEq { name, value });
                }
                LabelMatcher::Regex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::Regex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
                LabelMatcher::NotRegex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::NotRegex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
            }
        }

        normalized
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NormalizedMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

pub(crate) enum CompiledLabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: regex::Regex },
    NotRegex { name: String, pattern: regex::Regex },
}

pub(in crate::storage::segment) const PROMQL_PROJECTION_SUFFIXES: [&str; 3] =
    ["_bucket", "_count", "_sum"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::segment) struct ResolvedEqualityMatcher {
    pub(in crate::storage::segment) postings: ExactPostingsMetadata,
    pub(in crate::storage::segment) selection: crate::storage::index::ExactPostingsSelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentPruneReason {
    MissingEquality,
    MatcherTimeRange,
}
