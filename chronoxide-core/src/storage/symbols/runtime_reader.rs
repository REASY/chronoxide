//! Aggregate-governed positional reader for the shared `symbols.bin` v3 format.
//!
//! The long-lived reader owns only a segment-generation registration and fixed
//! root facts. Query sessions pin the decoded root and authenticated pages for
//! exactly as long as callers need borrowed symbol bytes. No `File`, read
//! guard, or registration is retained by a cache value.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::storage::metadata_cache::{
    LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError, MetadataCachePin,
};
use crate::storage::metadata_governor::{MetadataCacheClass, MetadataCharge, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard,
    StoreMetadataRuntimeError,
};
use crate::storage::segment::SegmentFile;

use super::format::{
    SYMBOLS_V3_HEADER_LEN, SymbolPageDescriptor, SymbolRoot, SymbolRootHeaderFacts,
    ValidatedSymbolPage, decode_symbol_root, decode_symbol_root_header, invalid_symbols_data,
    validate_page,
};

#[derive(Debug, Error)]
pub(crate) enum GovernedSymbolReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error("governed symbol planning failed: {0}")]
    Planning(#[source] io::Error),
    #[error("governed symbol session belongs to another segment generation")]
    ForeignSegmentGeneration,
}

/// Long-lived symbol reader without descriptor or metadata-cache pins.
pub(crate) struct GovernedSymbolReader {
    registered: RegisteredSegment,
    facts: SymbolRootHeaderFacts,
}

/// Query-local root pin and generation authorization.
pub(crate) struct GovernedSymbolSession {
    guard: SegmentReadGuard,
    root: MetadataCachePin<SymbolRoot>,
    logical: GovernedSymbolLogicalCounters,
}

/// Unforgeable proof of the symbol count decoded from one generation's
/// validated `symbols.bin` v3 root.
#[derive(Debug)]
pub(crate) struct GovernedSymbolCountBinding {
    provenance: SegmentGenerationProvenance,
    symbol_count: u32,
}

impl GovernedSymbolCountBinding {
    pub(crate) fn matches(&self, guard: &SegmentReadGuard) -> bool {
        self.provenance.matches(guard)
    }

    pub(crate) fn symbol_count(&self) -> u32 {
        self.symbol_count
    }
}

/// Query-local symbol-ID results whose allocation remains scratch-charged.
pub(crate) struct GovernedSymbolLookupBatch {
    values: Vec<Option<u32>>,
    _charge: MetadataCharge,
}

impl GovernedSymbolLookupBatch {
    pub(crate) fn values(&self) -> &[Option<u32>] {
        &self.values
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct GovernedSymbolLogicalStats {
    pub(crate) returned_values: u64,
    pub(crate) returned_utf8_bytes: u64,
}

#[derive(Debug, Default)]
struct GovernedSymbolLogicalCounters {
    returned_values: AtomicU64,
    returned_utf8_bytes: AtomicU64,
}

impl GovernedSymbolLogicalCounters {
    fn record(&self, utf8_bytes: usize) {
        self.returned_values.fetch_add(1, Ordering::Relaxed);
        self.returned_utf8_bytes.fetch_add(
            u64::try_from(utf8_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> GovernedSymbolLogicalStats {
        GovernedSymbolLogicalStats {
            returned_values: self.returned_values.load(Ordering::Relaxed),
            returned_utf8_bytes: self.returned_utf8_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LookupWork {
    page_index: usize,
    result_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct ResolveWork {
    page_index: usize,
    request_index: usize,
    local_id: usize,
}

#[derive(Debug, Clone, Copy)]
struct PageOrder {
    page_index: usize,
    first_request_index: usize,
}

impl GovernedSymbolReader {
    /// Validates and caches the exact v3 root once, then releases every query
    /// guard and pin before returning the long-lived reader.
    pub(crate) fn open(registered: &RegisteredSegment) -> Result<Self, GovernedSymbolReaderError> {
        let guard = registered.read_guard()?;
        let reader = guard.reader(SegmentFile::Symbols)?;
        if reader.len() < SYMBOLS_V3_HEADER_LEN as u64 {
            return Err(reader
                .record_validation_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "symbols file is shorter than the v3 header",
                ))
                .into());
        }
        let mut prefix = [0u8; SYMBOLS_V3_HEADER_LEN];
        reader.read_exact_at_for_class(0, &mut prefix, MetadataCacheClass::SymbolRoot)?;
        let facts = decode_symbol_root_header(&prefix, reader.len())
            .map_err(|error| reader.record_validation_error(error))?;
        let root = load_root(&guard, facts, Some(&prefix))?;
        validate_root_facts(&root, facts).map_err(|error| reader.record_validation_error(error))?;
        drop(root);
        drop(guard);
        Ok(Self {
            registered: registered.clone(),
            facts,
        })
    }

    pub(crate) fn query_session(&self) -> Result<GovernedSymbolSession, GovernedSymbolReaderError> {
        let guard = self.registered.read_guard()?;
        let root = load_root(&guard, self.facts, None)?;
        if let Err(error) = validate_root_facts(&root, self.facts) {
            drop(root);
            let reader = guard.reader(SegmentFile::Symbols)?;
            return Err(reader.record_validation_error(error).into());
        }
        Ok(GovernedSymbolSession {
            guard,
            root,
            logical: GovernedSymbolLogicalCounters::default(),
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }

    pub(crate) fn len(&self) -> usize {
        self.facts.symbol_count as usize
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.facts.symbol_count == 0
    }
}

impl GovernedSymbolSession {
    pub(crate) fn symbol_count_binding(&self) -> GovernedSymbolCountBinding {
        GovernedSymbolCountBinding {
            provenance: self.guard.provenance(),
            symbol_count: self.root.symbol_count,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.root.symbol_count as usize
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.root.symbol_count == 0
    }

    pub(crate) fn logical_stats(&self) -> GovernedSymbolLogicalStats {
        self.logical.snapshot()
    }

    /// Validates every page in descriptor order without reporting logical
    /// symbol returns. Root validation already proves canonical adjacent
    /// fences; each loaded page proves its complete contents and fence binding.
    pub(crate) fn validate_all_pages(&self) -> Result<(), GovernedSymbolReaderError> {
        for page_index in 0..self.root.descriptors.len() {
            drop(self.load_page(page_index)?);
        }
        Ok(())
    }

    pub(crate) fn ensure_same_generation(
        &self,
        guard: &SegmentReadGuard,
    ) -> Result<(), GovernedSymbolReaderError> {
        if self.guard.provenance().matches(guard) {
            Ok(())
        } else {
            Err(GovernedSymbolReaderError::ForeignSegmentGeneration)
        }
    }

    pub(crate) fn lookup(&self, value: &str) -> Result<Option<u32>, GovernedSymbolReaderError> {
        let target = value.as_bytes();
        let Some(page_index) = lookup_page_index(&self.root, target) else {
            return Ok(None);
        };
        let page = self.load_page(page_index)?;
        let result = lookup_loaded_page(&page, target)?;
        if result.is_some() {
            self.logical.record(target.len());
        }
        Ok(result)
    }

    /// Resolves lookup requests page-by-page in first-touch order. The result
    /// vector remains charged until its owner is dropped; temporary grouping
    /// vectors are included in the charge while the operation runs.
    pub(crate) fn lookup_many<S>(
        &self,
        values: &[S],
    ) -> Result<GovernedSymbolLookupBatch, GovernedSymbolReaderError>
    where
        S: AsRef<str>,
    {
        let declared = lookup_scratch_bytes(values.len())?;
        let mut charge = self.reserve_scratch(declared)?;
        let mut results = Vec::new();
        results
            .try_reserve_exact(values.len())
            .map_err(|_| planning_error("symbol lookup result allocation failed"))?;
        results.resize(values.len(), None);
        let mut page_order = Vec::new();
        page_order
            .try_reserve_exact(values.len())
            .map_err(|_| planning_error("symbol lookup page-order allocation failed"))?;
        let mut work = Vec::new();
        work.try_reserve_exact(values.len())
            .map_err(|_| planning_error("symbol lookup work allocation failed"))?;
        charge
            .reconcile(actual_lookup_scratch_bytes(&results, &page_order, &work)?)
            .map_err(MetadataCacheError::from)?;

        for (result_index, value) in values.iter().enumerate() {
            let Some(page_index) = lookup_page_index(&self.root, value.as_ref().as_bytes()) else {
                continue;
            };
            work.push(LookupWork {
                page_index,
                result_index,
            });
        }
        work.sort_unstable_by_key(|entry| (entry.page_index, entry.result_index));
        collect_lookup_page_order(&work, &mut page_order);
        page_order.sort_unstable_by_key(|entry| entry.first_request_index);

        for order in &page_order {
            let page = self.load_page(order.page_index)?;
            let requests = lookup_work_for_page(&work, order.page_index);
            for request in requests {
                let value = values[request.result_index].as_ref();
                results[request.result_index] = lookup_loaded_page(&page, value.as_bytes())?;
            }
        }
        for (value, result) in values.iter().zip(&results) {
            if result.is_some() {
                self.logical.record(value.as_ref().len());
            }
        }
        drop(work);
        drop(page_order);
        charge
            .reconcile(vec_charge::<Option<u32>>(results.capacity())?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedSymbolLookupBatch {
            values: results,
            _charge: charge,
        })
    }

    /// Visits resolved strings while each backing page remains pinned. An
    /// out-of-range ID stops planning before later pages are touched, matching
    /// the scalar reader's corruption and missing-value precedence.
    pub(crate) fn visit_resolved_many(
        &self,
        symbol_ids: &[u32],
        mut visit: impl FnMut(usize, &str) -> io::Result<()>,
    ) -> Result<bool, GovernedSymbolReaderError> {
        if symbol_ids.is_empty() {
            return Ok(true);
        }
        let declared = resolve_scratch_bytes(symbol_ids.len())?;
        let mut charge = self.reserve_scratch(declared)?;
        let mut page_order = Vec::new();
        page_order
            .try_reserve_exact(symbol_ids.len())
            .map_err(|_| planning_error("symbol resolve page-order allocation failed"))?;
        let mut work = Vec::new();
        work.try_reserve_exact(symbol_ids.len())
            .map_err(|_| planning_error("symbol resolve work allocation failed"))?;
        charge
            .reconcile(actual_resolve_scratch_bytes(&page_order, &work)?)
            .map_err(MetadataCacheError::from)?;

        let mut complete = true;
        for (request_index, &symbol_id) in symbol_ids.iter().enumerate() {
            let Some((page_index, local_id)) = resolve_page_and_local_id(&self.root, symbol_id)?
            else {
                complete = false;
                break;
            };
            work.push(ResolveWork {
                page_index,
                request_index,
                local_id,
            });
        }
        work.sort_unstable_by_key(|entry| (entry.page_index, entry.request_index));
        collect_resolve_page_order(&work, &mut page_order);
        page_order.sort_unstable_by_key(|entry| entry.first_request_index);

        for order in &page_order {
            let page = self.load_page(order.page_index)?;
            let requests = resolve_work_for_page(&work, order.page_index);
            for request in requests {
                let value = page
                    .symbol(request.local_id)
                    .ok_or_else(|| planning_error("validated symbols page has a missing symbol"))?;
                visit(request.request_index, value).map_err(GovernedSymbolReaderError::Planning)?;
                self.logical.record(value.len());
            }
        }
        drop(work);
        drop(page_order);
        charge.reconcile(0).map_err(MetadataCacheError::from)?;
        Ok(complete)
    }

    /// Resolves one structurally required symbol and visits it while its
    /// authenticated page remains pinned. Unlike optional dictionary lookup,
    /// an out-of-range ID is corruption in the referring artifact contract.
    pub(crate) fn visit_required_resolved(
        &self,
        symbol_id: u32,
        visit: impl FnOnce(&str) -> io::Result<()>,
    ) -> Result<(), GovernedSymbolReaderError> {
        if symbol_id >= self.root.symbol_count {
            let reader = self.guard.reader(SegmentFile::Symbols)?;
            return Err(reader
                .record_validation_error(invalid_symbols_data(
                    "required symbol id exceeds the declared symbol count",
                ))
                .into());
        }

        let (page_index, local_id) = resolve_page_and_local_id(&self.root, symbol_id)?
            .ok_or_else(|| planning_error("validated required symbol id is missing"))?;
        let page = self.load_page(page_index)?;
        let value = page
            .symbol(local_id)
            .ok_or_else(|| planning_error("validated symbols page has a missing symbol"))?;
        visit(value).map_err(GovernedSymbolReaderError::Planning)?;
        self.logical.record(value.len());
        Ok(())
    }

    fn load_page(
        &self,
        page_index: usize,
    ) -> Result<MetadataCachePin<ValidatedSymbolPage>, GovernedSymbolReaderError> {
        let descriptor = self
            .root
            .descriptors
            .get(page_index)
            .ok_or_else(|| planning_error("symbols page descriptor is missing"))?;
        let page_index_u32 = u32::try_from(page_index)
            .map_err(|_| planning_error("symbols page index exceeds u32"))?;
        let reader = self.guard.reader(SegmentFile::Symbols)?;
        let key = cache_key(
            &reader,
            descriptor.page_offset,
            u64::from(descriptor.page_len),
            MetadataCacheClass::SymbolPage,
        )?;
        let declared = page_charge_bytes(descriptor)?;
        let first_fence = self.root.first_fence(descriptor);
        let last_fence = self.root.last_fence(descriptor);
        let page = reader.get_or_load_owned(key, declared, |bytes| {
            let page = validate_page(page_index_u32, descriptor, first_fence, last_fence, bytes)
                .map_err(MetadataCacheError::from_io)?;
            let charged = u64::try_from(page.charge_bytes()).map_err(|_| {
                MetadataCacheError::transient(
                    io::ErrorKind::OutOfMemory,
                    "validated symbol page charge exceeds u64",
                )
            })?;
            Ok(LoadedMetadata::new(page, charged))
        })?;
        if let Err(error) = validate_page_binding(&page, descriptor, first_fence, last_fence) {
            drop(page);
            return Err(reader.record_validation_error(error).into());
        }
        Ok(page)
    }

    fn reserve_scratch(&self, bytes: u64) -> Result<MetadataCharge, GovernedSymbolReaderError> {
        Ok(self
            .guard
            .reader(SegmentFile::Symbols)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(bytes, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?)
    }
}

fn load_root(
    guard: &SegmentReadGuard,
    facts: SymbolRootHeaderFacts,
    prefix: Option<&[u8]>,
) -> Result<MetadataCachePin<SymbolRoot>, GovernedSymbolReaderError> {
    let reader = guard.reader(SegmentFile::Symbols)?;
    let key = cache_key(
        &reader,
        0,
        u64::try_from(facts.root_len)
            .map_err(|_| planning_error("symbols root length exceeds u64"))?,
        MetadataCacheClass::SymbolRoot,
    )?;
    let declared = root_charge_bytes(facts)?;
    let decode = |bytes: &[u8]| {
        let root = decode_symbol_root(bytes, facts).map_err(MetadataCacheError::from_io)?;
        let charged = u64::try_from(root.retained_charge_bytes()).map_err(|_| {
            MetadataCacheError::transient(
                io::ErrorKind::OutOfMemory,
                "decoded symbols root charge exceeds u64",
            )
        })?;
        Ok(LoadedMetadata::new(root, charged))
    };
    Ok(match prefix {
        Some(prefix) => reader.get_or_load_with_prefix(key, declared, prefix, decode)?,
        None => reader.get_or_load(key, declared, decode)?,
    })
}

fn validate_root_facts(root: &SymbolRoot, facts: SymbolRootHeaderFacts) -> io::Result<()> {
    if root.symbol_count != facts.symbol_count
        || root.source_file_bytes != facts.file_len
        || root.encoded_bytes != facts.root_len
        || root.descriptors.len()
            != usize::try_from(facts.page_count)
                .map_err(|_| invalid_symbols_data("symbols page count exceeds usize"))?
    {
        return Err(invalid_symbols_data(
            "decoded symbols root does not match its staged header",
        ));
    }
    Ok(())
}

fn validate_page_binding(
    page: &ValidatedSymbolPage,
    descriptor: &SymbolPageDescriptor,
    first_fence: &[u8],
    last_fence: &[u8],
) -> io::Result<()> {
    if page.first_symbol_id != descriptor.first_symbol_id
        || page.len()
            != usize::try_from(descriptor.symbol_count)
                .map_err(|_| invalid_symbols_data("symbols page count exceeds usize"))?
        || page.symbol(0).map(str::as_bytes) != Some(first_fence)
        || page.symbol(page.len().saturating_sub(1)).map(str::as_bytes) != Some(last_fence)
    {
        return Err(invalid_symbols_data(
            "cached symbols page does not match its root descriptor",
        ));
    }
    Ok(())
}

fn lookup_page_index(root: &SymbolRoot, target: &[u8]) -> Option<usize> {
    let mut low = 0usize;
    let mut high = root.descriptors.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if root.first_fence(&root.descriptors[mid]) <= target {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let page_index = low.checked_sub(1)?;
    (target <= root.last_fence(&root.descriptors[page_index])).then_some(page_index)
}

fn resolve_page_and_local_id(
    root: &SymbolRoot,
    symbol_id: u32,
) -> Result<Option<(usize, usize)>, GovernedSymbolReaderError> {
    if symbol_id >= root.symbol_count {
        return Ok(None);
    }
    let mut low = 0usize;
    let mut high = root.descriptors.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if root.descriptors[mid].first_symbol_id <= symbol_id {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let page_index = low
        .checked_sub(1)
        .ok_or_else(|| planning_error("symbol id has no page descriptor"))?;
    let descriptor = &root.descriptors[page_index];
    let local_id = symbol_id
        .checked_sub(descriptor.first_symbol_id)
        .ok_or_else(|| planning_error("symbol id precedes its page"))?;
    if local_id >= descriptor.symbol_count {
        return Err(planning_error("symbol id exceeds its page descriptor"));
    }
    Ok(Some((
        page_index,
        usize::try_from(local_id).map_err(|_| planning_error("symbol local id exceeds usize"))?,
    )))
}

fn lookup_loaded_page(
    page: &ValidatedSymbolPage,
    target: &[u8],
) -> Result<Option<u32>, GovernedSymbolReaderError> {
    let mut low = 0usize;
    let mut high = page.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let candidate = page
            .symbol(mid)
            .ok_or_else(|| planning_error("validated symbols page has a missing symbol"))?;
        match candidate.as_bytes().cmp(target) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Equal => {
                let local_id = u32::try_from(mid)
                    .map_err(|_| planning_error("symbols page local id exceeds u32"))?;
                return Ok(Some(
                    page.first_symbol_id
                        .checked_add(local_id)
                        .ok_or_else(|| planning_error("symbol id overflows"))?,
                ));
            }
            std::cmp::Ordering::Greater => high = mid,
        }
    }
    Ok(None)
}

fn collect_lookup_page_order(work: &[LookupWork], page_order: &mut Vec<PageOrder>) {
    for request in work {
        if page_order
            .last()
            .is_none_or(|last| last.page_index != request.page_index)
        {
            page_order.push(PageOrder {
                page_index: request.page_index,
                first_request_index: request.result_index,
            });
        }
    }
}

fn collect_resolve_page_order(work: &[ResolveWork], page_order: &mut Vec<PageOrder>) {
    for request in work {
        if page_order
            .last()
            .is_none_or(|last| last.page_index != request.page_index)
        {
            page_order.push(PageOrder {
                page_index: request.page_index,
                first_request_index: request.request_index,
            });
        }
    }
}

fn lookup_work_for_page(work: &[LookupWork], page_index: usize) -> &[LookupWork] {
    let start = work.partition_point(|entry| entry.page_index < page_index);
    let end = start + work[start..].partition_point(|entry| entry.page_index == page_index);
    &work[start..end]
}

fn resolve_work_for_page(work: &[ResolveWork], page_index: usize) -> &[ResolveWork] {
    let start = work.partition_point(|entry| entry.page_index < page_index);
    let end = start + work[start..].partition_point(|entry| entry.page_index == page_index);
    &work[start..end]
}

fn cache_key(
    reader: &GovernedArtifactReader,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
    reader.metadata_cache_key(offset, length, class)
}

fn root_charge_bytes(facts: SymbolRootHeaderFacts) -> Result<u64, GovernedSymbolReaderError> {
    let descriptors = usize::try_from(facts.page_count)
        .map_err(|_| planning_error("symbols page count exceeds usize"))?
        .checked_mul(std::mem::size_of::<SymbolPageDescriptor>())
        .ok_or_else(|| planning_error("decoded symbols descriptor charge overflows"))?;
    let fences = usize::try_from(
        facts
            .pages_offset
            .checked_sub(facts.fence_offset)
            .ok_or_else(|| planning_error("symbols fence range is reversed"))?,
    )
    .map_err(|_| planning_error("symbols fence charge exceeds usize"))?;
    usize_charge(
        std::mem::size_of::<SymbolRoot>()
            .checked_add(descriptors)
            .and_then(|bytes| bytes.checked_add(fences))
            .ok_or_else(|| planning_error("decoded symbols root charge overflows"))?,
    )
}

fn page_charge_bytes(descriptor: &SymbolPageDescriptor) -> Result<u64, GovernedSymbolReaderError> {
    let offsets = usize::try_from(descriptor.symbol_count)
        .map_err(|_| planning_error("symbols page count exceeds usize"))?
        .checked_add(1)
        .and_then(|count| count.checked_mul(std::mem::size_of::<u32>()))
        .ok_or_else(|| planning_error("validated symbol offsets charge overflows"))?;
    let strings = usize::try_from(descriptor.string_bytes_len)
        .map_err(|_| planning_error("validated symbol strings charge exceeds usize"))?;
    usize_charge(
        std::mem::size_of::<ValidatedSymbolPage>()
            .checked_add(offsets)
            .and_then(|bytes| bytes.checked_add(strings))
            .ok_or_else(|| planning_error("validated symbol page charge overflows"))?,
    )
}

fn lookup_scratch_bytes(len: usize) -> Result<u64, GovernedSymbolReaderError> {
    checked_add_charges(&[
        vec_charge::<Option<u32>>(len)?,
        vec_charge::<PageOrder>(len)?,
        vec_charge::<LookupWork>(len)?,
    ])
}

fn resolve_scratch_bytes(len: usize) -> Result<u64, GovernedSymbolReaderError> {
    checked_add_charges(&[
        vec_charge::<PageOrder>(len)?,
        vec_charge::<ResolveWork>(len)?,
    ])
}

fn actual_lookup_scratch_bytes(
    results: &Vec<Option<u32>>,
    page_order: &Vec<PageOrder>,
    work: &Vec<LookupWork>,
) -> Result<u64, GovernedSymbolReaderError> {
    checked_add_charges(&[
        vec_charge::<Option<u32>>(results.capacity())?,
        vec_charge::<PageOrder>(page_order.capacity())?,
        vec_charge::<LookupWork>(work.capacity())?,
    ])
}

fn actual_resolve_scratch_bytes(
    page_order: &Vec<PageOrder>,
    work: &Vec<ResolveWork>,
) -> Result<u64, GovernedSymbolReaderError> {
    checked_add_charges(&[
        vec_charge::<PageOrder>(page_order.capacity())?,
        vec_charge::<ResolveWork>(work.capacity())?,
    ])
}

fn vec_charge<T>(capacity: usize) -> Result<u64, GovernedSymbolReaderError> {
    usize_charge(
        capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| planning_error("symbol scratch vector charge overflows"))?,
    )
}

fn checked_add_charges(values: &[u64]) -> Result<u64, GovernedSymbolReaderError> {
    values.iter().try_fold(0u64, |total, &value| {
        total
            .checked_add(value)
            .ok_or_else(|| planning_error("symbol scratch aggregate charge overflows"))
    })
}

fn usize_charge(bytes: usize) -> Result<u64, GovernedSymbolReaderError> {
    u64::try_from(bytes).map_err(|_| planning_error("symbol memory charge exceeds u64"))
}

fn planning_error(message: &'static str) -> GovernedSymbolReaderError {
    GovernedSymbolReaderError::Planning(io::Error::new(io::ErrorKind::InvalidInput, message))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::storage::metadata_governor::MetadataGovernorConfig;
    use crate::storage::metadata_runtime::{SegmentArtifactRegistration, StoreMetadataRuntime};
    use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;

    use super::super::write_symbols_bin_v3;
    use super::*;

    struct Fixture {
        _directory: TempDir,
        runtime: StoreMetadataRuntime,
        registered: RegisteredSegment,
        reader: GovernedSymbolReader,
    }

    fn fixture(
        identity: &str,
        values: &[&str],
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
        corrupt_page: bool,
    ) -> Result<Fixture, GovernedSymbolReaderError> {
        let directory = tempfile::tempdir().unwrap();
        let mut symbols = Vec::new();
        write_symbols_bin_v3(&mut symbols, values).unwrap();
        if corrupt_page {
            let descriptor_offset = SYMBOLS_V3_HEADER_LEN;
            let page_offset = u64::from_le_bytes(
                symbols[descriptor_offset + 8..descriptor_offset + 16]
                    .try_into()
                    .unwrap(),
            );
            symbols[usize::try_from(page_offset).unwrap()] ^= 0xff;
        }
        for file in SEGMENT_FOOTER_TRACKED_FILES {
            let bytes = if file == SegmentFile::Symbols {
                symbols.as_slice()
            } else {
                &[]
            };
            fs::write(directory.path().join(file.filename()), bytes).unwrap();
        }
        let runtime = StoreMetadataRuntime::new(MetadataGovernorConfig {
            retained_max_bytes,
            in_flight_max_bytes,
            max_open_files: 1,
            max_cached_open_files: 0,
        })
        .unwrap();
        let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
            let path = directory.path().join(file.filename());
            SegmentArtifactRegistration::new(file, path.clone(), fs::metadata(path).unwrap().len())
        });
        let registered = runtime.register_segment(identity, &artifacts)?;
        let reader = GovernedSymbolReader::open(&registered)?;
        Ok(Fixture {
            _directory: directory,
            runtime,
            registered,
            reader,
        })
    }

    fn symbols_issued(runtime: &StoreMetadataRuntime) -> (u64, u64) {
        let stats = runtime.snapshot().reads;
        let file = stats
            .files
            .iter()
            .find(|entry| entry.file == SegmentFile::Symbols)
            .unwrap();
        (file.issued.calls, file.issued.bytes)
    }

    #[test]
    fn governed_symbols_match_scalar_and_batched_v3_semantics() {
        let fixture = fixture(
            "symbols-roundtrip",
            &["", "__name__", "alpha", "omega"],
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        assert_eq!(fixture.reader.segment_identity(), "symbols-roundtrip");
        assert_eq!(fixture.reader.len(), 4);
        assert!(!fixture.reader.is_empty());

        let session = fixture.reader.query_session().unwrap();
        assert_eq!(session.len(), 4);
        assert!(!session.is_empty());
        assert_eq!(session.lookup("alpha").unwrap(), Some(2));
        assert_eq!(session.lookup("missing").unwrap(), None);
        let batch = session
            .lookup_many(&["omega", "", "missing", "alpha", "alpha"])
            .unwrap();
        assert_eq!(batch.values(), &[Some(3), Some(0), None, Some(2), Some(2)]);
        assert_eq!(
            batch.charged_bytes(),
            u64::try_from(batch.values.capacity() * std::mem::size_of::<Option<u32>>()).unwrap()
        );

        let mut resolved = vec![String::new(); 3];
        assert!(
            session
                .visit_resolved_many(&[3, 0, 2], |index, value| {
                    resolved[index] = value.to_string();
                    Ok(())
                })
                .unwrap()
        );
        assert_eq!(resolved, ["omega", "", "alpha"]);
        let stats = session.logical_stats();
        assert_eq!(stats.returned_values, 8);
        assert_eq!(stats.returned_utf8_bytes, 30);
    }

    #[test]
    fn required_scalar_visit_borrows_value_and_keeps_callback_errors_transient() {
        let fixture = fixture(
            "symbols-required-scalar",
            &["alpha", "beta"],
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        let session = fixture.reader.query_session().unwrap();

        let callback_error = session
            .visit_required_resolved(0, |value| {
                assert_eq!(value, "alpha");
                Err(io::Error::other("stop visiting"))
            })
            .unwrap_err();
        match callback_error {
            GovernedSymbolReaderError::Planning(error) => {
                assert_eq!(error.kind(), io::ErrorKind::Other);
                assert_eq!(error.to_string(), "stop visiting");
            }
            other => panic!("unexpected callback error: {other}"),
        }
        assert_eq!(
            session.logical_stats(),
            GovernedSymbolLogicalStats::default()
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

        let mut visited = false;
        session
            .visit_required_resolved(1, |value| {
                visited = true;
                assert_eq!(value, "beta");
                Ok(())
            })
            .unwrap();
        assert!(visited);
        assert_eq!(
            session.logical_stats(),
            GovernedSymbolLogicalStats {
                returned_values: 1,
                returned_utf8_bytes: 4,
            }
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    }

    #[test]
    fn required_out_of_range_id_is_sticky_without_page_io_or_callback() {
        let fixture = fixture(
            "symbols-required-invalid",
            &["alpha", "beta"],
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        let session = fixture.reader.query_session().unwrap();
        let before = symbols_issued(&fixture.runtime);
        let mut visited = false;

        let error = session
            .visit_required_resolved(2, |_| {
                visited = true;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(
            error,
            GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert!(!visited);
        assert_eq!(symbols_issued(&fixture.runtime), before);
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

        let repeated = session.visit_required_resolved(2, |_| Ok(())).unwrap_err();
        assert!(matches!(
            repeated,
            GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert_eq!(symbols_issued(&fixture.runtime), before);
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    }

    #[test]
    fn symbol_session_rejects_a_foreign_generation_without_io_or_poisoning() {
        let first = fixture(
            "symbols-generation-first",
            &["alpha"],
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        let second = fixture(
            "symbols-generation-second",
            &["alpha"],
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        let session = first.reader.query_session().unwrap();
        let own_guard = first.registered.read_guard().unwrap();
        let foreign_guard = second.registered.read_guard().unwrap();
        let before_first = symbols_issued(&first.runtime);
        let before_second = symbols_issued(&second.runtime);

        session.ensure_same_generation(&own_guard).unwrap();
        assert!(matches!(
            session.ensure_same_generation(&foreign_guard),
            Err(GovernedSymbolReaderError::ForeignSegmentGeneration)
        ));
        assert_eq!(symbols_issued(&first.runtime), before_first);
        assert_eq!(symbols_issued(&second.runtime), before_second);
        assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
        assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
    }

    #[test]
    fn empty_dictionary_resolves_without_page_io() {
        let fixture = fixture("symbols-empty", &[], 1024 * 1024, 1024 * 1024, false).unwrap();
        assert!(fixture.reader.is_empty());
        let before = symbols_issued(&fixture.runtime);
        let session = fixture.reader.query_session().unwrap();
        assert!(session.is_empty());
        assert_eq!(session.lookup("missing").unwrap(), None);
        assert_eq!(session.lookup_many(&["missing"]).unwrap().values(), &[None]);
        assert!(!session.visit_resolved_many(&[0], |_, _| Ok(())).unwrap());
        assert!(session.visit_resolved_many(&[], |_, _| Ok(())).unwrap());
        assert_eq!(symbols_issued(&fixture.runtime), before);
        assert_eq!(
            session.logical_stats(),
            GovernedSymbolLogicalStats::default()
        );
    }

    #[test]
    fn cross_page_batches_preserve_request_order_and_load_each_page_once() {
        let values = (0..600)
            .map(|index| format!("symbol-{index:04}-{}", "x".repeat(96)))
            .collect::<Vec<_>>();
        let value_refs = values.iter().map(String::as_str).collect::<Vec<_>>();
        let fixture = fixture(
            "symbols-cross-page",
            &value_refs,
            1024 * 1024,
            1024 * 1024,
            false,
        )
        .unwrap();
        let session = fixture.reader.query_session().unwrap();
        assert!(session.root.descriptors.len() > 1);
        let second_page_id = session.root.descriptors[1].first_symbol_id;
        let second_page_index = usize::try_from(second_page_id).unwrap();
        let before = symbols_issued(&fixture.runtime);

        let batch = session
            .lookup_many(&[
                values[second_page_index].as_str(),
                values[0].as_str(),
                values[second_page_index].as_str(),
                "!before-first",
                values[0].as_str(),
            ])
            .unwrap();
        assert_eq!(
            batch.values(),
            &[
                Some(second_page_id),
                Some(0),
                Some(second_page_id),
                None,
                Some(0),
            ]
        );
        assert_eq!(symbols_issued(&fixture.runtime).0 - before.0, 2);

        let before_resolve = symbols_issued(&fixture.runtime);
        let mut resolved = vec![String::new(); 4];
        assert!(
            session
                .visit_resolved_many(&[second_page_id, 0, second_page_id, 0], |slot, value| {
                    resolved[slot] = value.to_owned();
                    Ok(())
                })
                .unwrap()
        );
        assert_eq!(
            resolved,
            [
                values[second_page_index].clone(),
                values[0].clone(),
                values[second_page_index].clone(),
                values[0].clone(),
            ]
        );
        assert_eq!(symbols_issued(&fixture.runtime), before_resolve);
    }

    #[test]
    fn zero_retention_batches_one_page_into_one_physical_read() {
        let fixture = fixture(
            "symbols-zero-retention",
            &["alpha", "beta", "gamma"],
            0,
            1024 * 1024,
            false,
        )
        .unwrap();
        let before = symbols_issued(&fixture.runtime);
        let session = fixture.reader.query_session().unwrap();
        let after_root = symbols_issued(&fixture.runtime);
        assert_eq!(after_root.0 - before.0, 1);
        let batch = session
            .lookup_many(&["alpha", "beta", "gamma", "alpha"])
            .unwrap();
        assert_eq!(batch.values(), &[Some(0), Some(1), Some(2), Some(0)]);
        let after = symbols_issued(&fixture.runtime);
        assert_eq!(after.0 - after_root.0, 1);
        assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
    }

    #[test]
    fn touched_page_corruption_is_sticky_across_eviction() {
        let fixture = fixture(
            "symbols-corrupt-page",
            &["alpha", "beta"],
            1024 * 1024,
            1024 * 1024,
            true,
        )
        .unwrap();
        let session = fixture.reader.query_session().unwrap();
        let error = session.lookup("alpha").unwrap_err();
        assert!(matches!(
            error,
            GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_first = symbols_issued(&fixture.runtime);
        fixture.runtime.evict_all_resident_metadata();
        let repeated = session.lookup("alpha").unwrap_err();
        assert!(matches!(
            repeated,
            GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert_eq!(symbols_issued(&fixture.runtime), after_first);
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    }

    #[test]
    fn tiny_in_flight_budget_refuses_root_before_suffix_io_without_poisoning() {
        let directory = tempfile::tempdir().unwrap();
        let mut symbols = Vec::new();
        write_symbols_bin_v3(&mut symbols, ["alpha", "beta"]).unwrap();
        for file in SEGMENT_FOOTER_TRACKED_FILES {
            fs::write(
                directory.path().join(file.filename()),
                if file == SegmentFile::Symbols {
                    symbols.as_slice()
                } else {
                    &[]
                },
            )
            .unwrap();
        }
        const IN_FLIGHT_LIMIT: u64 = 4096;
        let runtime = StoreMetadataRuntime::new(MetadataGovernorConfig {
            retained_max_bytes: 1024 * 1024,
            in_flight_max_bytes: IN_FLIGHT_LIMIT,
            max_open_files: 1,
            max_cached_open_files: 0,
        })
        .unwrap();
        let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
            let path = directory.path().join(file.filename());
            SegmentArtifactRegistration::new(file, path.clone(), fs::metadata(path).unwrap().len())
        });
        let registered = runtime
            .register_segment("symbols-tiny", &artifacts)
            .unwrap();
        let _blocker = runtime
            .governor()
            .reserve_in_flight_for_usage(
                IN_FLIGHT_LIMIT - SYMBOLS_V3_HEADER_LEN as u64,
                MetadataUsageClass::Scratch,
            )
            .unwrap();
        let error = GovernedSymbolReader::open(&registered)
            .err()
            .expect("tiny budget must reject root loading");
        assert!(matches!(
            error,
            GovernedSymbolReaderError::Cache(MetadataCacheError::Budget(_))
        ));
        assert_eq!(symbols_issued(&runtime), (1, SYMBOLS_V3_HEADER_LEN as u64));
        assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);
    }

    #[test]
    fn reader_owner_keeps_registration_alive_until_drop() {
        let fixture =
            fixture("symbols-owner", &["alpha"], 1024 * 1024, 1024 * 1024, false).unwrap();
        let Fixture {
            _directory,
            runtime,
            registered,
            reader,
        } = fixture;
        drop(registered);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
        drop(reader);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }
}
