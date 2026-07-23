use super::*;

pub struct SegmentIndexReader<R>
where
    R: SegmentIndexReadAt,
{
    inner: v7::SegmentIndexV7Reader<R>,
}

impl<R> SegmentIndexReader<R>
where
    R: SegmentIndexReadAt,
{
    pub fn open(source: R) -> io::Result<Self> {
        Ok(Self {
            inner: v7::SegmentIndexV7Reader::open(source)?,
        })
    }

    pub fn try_clone_reader(&self) -> io::Result<Self> {
        Ok(Self {
            inner: self.inner.try_clone_reader()?,
        })
    }

    pub fn read_stats(&self) -> SegmentIndexReadStats {
        self.inner.stats()
    }

    pub fn label_name_symbols(&self) -> io::Result<Vec<u32>> {
        self.inner.label_name_symbols()
    }

    pub fn has_label_values(&self) -> io::Result<bool> {
        self.inner.has_label_values()
    }

    pub fn label_time_range(&self, label_name_sym: u32) -> io::Result<Option<LabelValueTimeRange>> {
        self.inner.label_time_range(label_name_sym)
    }

    pub fn label_values(&self, label_name_sym: u32) -> io::Result<Vec<String>> {
        self.inner.label_values(label_name_sym)
    }

    pub fn label_values_with_prefix(
        &self,
        label_name_sym: u32,
        prefix: Option<&str>,
    ) -> io::Result<Vec<String>> {
        self.inner.label_values_with_prefix(label_name_sym, prefix)
    }

    pub fn exact_postings(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<Vec<u32>>> {
        self.inner.exact_postings(label_name_sym, label_value_sym)
    }

    pub fn exact_postings_metadata(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsMetadata>> {
        self.inner
            .exact_postings_metadata(label_name_sym, label_value_sym)
    }

    pub(in crate::storage) fn select_exact_postings(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsSelection>> {
        self.inner
            .exact_postings_selection(label_name_sym, label_value_sym)
    }

    pub(in crate::storage) fn read_exact_postings(
        &self,
        selection: ExactPostingsSelection,
    ) -> io::Result<Vec<u32>> {
        self.inner.read_exact_postings_selection(selection)
    }

    pub fn metric_series_ranges(&self, metric_sym: u32) -> io::Result<Vec<MetricSeriesRange>> {
        self.inner.metric_series_ranges(metric_sym)
    }

    pub fn metric_series_range_index(&self) -> io::Result<MetricSeriesRangeIndex> {
        self.inner.metric_series_range_index()
    }

    pub fn metric_series_ranges_byte_len(&self) -> u64 {
        self.inner.metric_series_ranges_byte_len()
    }

    pub fn routing_exact_postings_metadata(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> io::Result<RoutingLookupResult> {
        self.inner
            .routing_exact_postings_metadata(label_name, label_value)
    }

    pub fn routing_index(&self) -> io::Result<Option<SegmentRoutingIndex>> {
        self.inner.routing_index()
    }

    pub fn routing_index_byte_len(&self) -> Option<u64> {
        self.inner.routing_index_byte_len()
    }

    pub fn label_value_time_range(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        self.inner
            .label_value_time_range(label_name_sym, label_value_sym)
    }

    pub fn label_value_time_ranges(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<Vec<(u32, LabelValueTimeRange)>>> {
        self.inner.label_value_time_ranges(label_name_sym)
    }

    pub(in crate::storage::index) fn materialize(&self) -> io::Result<SegmentIndexes> {
        self.inner.materialize()
    }
}
