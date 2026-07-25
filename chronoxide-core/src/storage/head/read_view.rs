use std::fmt;
use std::sync::Arc;

use crate::labels::VersionedFlatInternedLabelSetSnapshot;

use super::*;

/// One immutable, self-contained head source pinned by a query session.
///
/// The encoded fragments and their exact catalog revision are owned through
/// `Arc`s so a query never observes a later mutable catalog tail or fragment
/// publication.
#[derive(Clone)]
pub struct HeadReadView {
    samples: Arc<FrozenHeadReadView>,
    labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    live_catalog: Option<Arc<LiveSeriesCatalog>>,
}

impl fmt::Debug for HeadReadView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeadReadView")
            .field("fragment_count", &self.samples.fragment_count())
            .field("catalog_revision", &self.labels.revision())
            .field("live_catalog", &self.live_catalog.is_some())
            .finish()
    }
}

impl HeadReadView {
    pub fn new(
        samples: Arc<FrozenHeadReadView>,
        labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    ) -> io::Result<Self> {
        let required_revision = samples.required_catalog_revision();
        if labels.revision() < required_revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frozen head requires catalog revision {required_revision}, \
                     but the pinned catalog revision is {}",
                    labels.revision()
                ),
            ));
        }
        Ok(Self {
            samples,
            labels,
            live_catalog: None,
        })
    }

    /// Constructs the production immutable live-head view.
    ///
    /// Unlike [`Self::new`], this path never builds a String-owning selector
    /// index per query. The catalog's active set must exactly match the sample
    /// root before the view can be published.
    pub fn new_live(
        samples: Arc<FrozenHeadReadView>,
        catalog: Arc<LiveSeriesCatalog>,
        view_generation: u64,
    ) -> io::Result<Self> {
        if catalog.generation() != view_generation {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live series catalog generation {} does not match view generation \
                     {view_generation}",
                    catalog.generation()
                ),
            ));
        }
        catalog.validate_sample_store(samples.sample_store())?;
        let required_revision = samples.required_catalog_revision();
        if catalog.revision() < required_revision {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frozen head requires catalog revision {required_revision}, \
                     but the pinned live catalog revision is {}",
                    catalog.revision()
                ),
            ));
        }
        Ok(Self {
            samples,
            labels: Arc::clone(catalog.labels()),
            live_catalog: Some(catalog),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn samples(&self) -> &Arc<FrozenHeadReadView> {
        &self.samples
    }

    pub fn labels(&self) -> &Arc<VersionedFlatInternedLabelSetSnapshot> {
        &self.labels
    }

    pub fn catalog_revision(&self) -> u64 {
        self.labels.revision()
    }

    pub fn live_catalog(&self) -> Option<&Arc<LiveSeriesCatalog>> {
        self.live_catalog.as_ref()
    }

    pub(crate) fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        match &self.live_catalog {
            Some(catalog) => self.samples.query_selector_with_live_catalog_budget(
                catalog, selector, start_ms, end_ms, budget,
            ),
            None => self.samples.query_selector_with_budget(
                self.labels.as_ref(),
                selector,
                start_ms,
                end_ms,
                budget,
            ),
        }
    }

    pub(crate) fn query_native_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        match &self.live_catalog {
            Some(catalog) => self
                .samples
                .query_native_histogram_with_live_catalog_budget(
                    catalog, selector, start_ms, end_ms, budget,
                ),
            None => self.samples.query_native_histogram_with_budget(
                self.labels.as_ref(),
                selector,
                start_ms,
                end_ms,
                budget,
            ),
        }
    }

    pub(crate) fn query_native_exponential_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        match &self.live_catalog {
            Some(catalog) => self
                .samples
                .query_native_exponential_histogram_with_live_catalog_budget(
                    catalog, selector, start_ms, end_ms, budget,
                ),
            None => self.samples.query_native_exponential_histogram_with_budget(
                self.labels.as_ref(),
                selector,
                start_ms,
                end_ms,
                budget,
            ),
        }
    }

    pub(crate) fn collect_metadata(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        match &self.live_catalog {
            Some(catalog) => self
                .samples
                .collect_metadata_with_live_catalog(catalog, start_ms, end_ms, metadata),
            None => self
                .samples
                .collect_metadata(self.labels.as_ref(), start_ms, end_ms, metadata),
        }
    }
}
