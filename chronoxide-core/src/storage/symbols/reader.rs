use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::format::{
    DEFAULT_SYMBOL_PAGE_CACHE_MAX_BYTES, SYMBOLS_V3_HEADER_LEN, SymbolRoot, ValidatedSymbolPage,
    decode_symbol_root, decode_symbol_root_header, invalid_symbols_data, validate_page,
};
use super::legacy::{LegacySymbolDictionary, read_legacy_v2_dictionary};

pub trait SegmentSymbolReadAt: Send + Sync {
    fn len(&self) -> io::Result<u64>;

    fn retained_open_files(&self) -> u64 {
        0
    }

    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()>;
}

#[cfg(any(unix, windows))]
impl SegmentSymbolReadAt for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        read_exact_at_loop(offset, destination, |read_offset, buffer| {
            file_read_at(self, read_offset, buffer)
        })
    }

    fn retained_open_files(&self) -> u64 {
        1
    }
}

impl<T> SegmentSymbolReadAt for Cursor<T>
where
    T: AsRef<[u8]> + Send + Sync,
{
    fn len(&self) -> io::Result<u64> {
        u64::try_from(self.get_ref().as_ref().len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "symbols source length exceeds u64",
            )
        })
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(|_| symbols_short_read())?;
        let end = start
            .checked_add(destination.len())
            .ok_or_else(symbols_offset_overflow)?;
        let source = self
            .get_ref()
            .as_ref()
            .get(start..end)
            .ok_or_else(symbols_short_read)?;
        destination.copy_from_slice(source);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentSymbolReadCount {
    pub calls: u64,
    pub bytes: u64,
}

impl SegmentSymbolReadCount {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            calls: self.calls.saturating_add(other.calls),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            calls: self.calls.saturating_sub(before.calls),
            bytes: self.bytes.saturating_sub(before.bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentSymbolReadStats {
    /// Complete eager v2 dictionary reads made only by the layout A/B backend.
    pub legacy_eager: SegmentSymbolReadCount,
    /// Successful caller-visible values and their UTF-8 bytes.
    pub logical_returned: SegmentSymbolReadCount,
    pub root: SegmentSymbolReadCount,
    pub page: SegmentSymbolReadCount,
    pub page_validation: SegmentSymbolReadCount,
    pub page_validation_ns: u64,
    pub touched_corrupt_pages: u64,
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
    pub page_cache_evictions: u64,
}

/// Current resources retained by one shared symbol-reader state.
///
/// Cloned readers have independent read counters but share this state, so a
/// caller aggregating these values must deduplicate by `state_identity()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentSymbolResourceSnapshot {
    /// File descriptors retained by this reader state.
    pub retained_open_files: u64,
    /// Complete encoded `symbols.bin` length represented by this reader.
    pub source_file_bytes: u64,
    /// Encoded v3 root length (`[0, pages_offset)`).
    pub root_encoded_bytes: u64,
    /// Decoded root allocations retained for routing lookups.
    pub root_retained_charge_bytes: u64,
    /// Retained whole-dictionary allocations used by an eager backend.
    pub eager_dictionary_retained_charge_bytes: u64,
    /// Current retained validated-page cache charge.
    pub page_cache_charge_bytes: u64,
    /// Configured validated-page cache capacity.
    pub page_cache_max_bytes: u64,
}

impl SegmentSymbolResourceSnapshot {
    pub fn total_retained_charge_bytes(self) -> u64 {
        self.root_retained_charge_bytes
            .saturating_add(self.eager_dictionary_retained_charge_bytes)
            .saturating_add(self.page_cache_charge_bytes)
    }
}

impl SegmentSymbolReadStats {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            legacy_eager: self.legacy_eager.saturating_add(other.legacy_eager),
            logical_returned: self.logical_returned.saturating_add(other.logical_returned),
            root: self.root.saturating_add(other.root),
            page: self.page.saturating_add(other.page),
            page_validation: self.page_validation.saturating_add(other.page_validation),
            page_validation_ns: self
                .page_validation_ns
                .saturating_add(other.page_validation_ns),
            touched_corrupt_pages: self
                .touched_corrupt_pages
                .saturating_add(other.touched_corrupt_pages),
            page_cache_hits: self.page_cache_hits.saturating_add(other.page_cache_hits),
            page_cache_misses: self
                .page_cache_misses
                .saturating_add(other.page_cache_misses),
            page_cache_evictions: self
                .page_cache_evictions
                .saturating_add(other.page_cache_evictions),
        }
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            legacy_eager: self.legacy_eager.delta_since(before.legacy_eager),
            logical_returned: self.logical_returned.delta_since(before.logical_returned),
            root: self.root.delta_since(before.root),
            page: self.page.delta_since(before.page),
            page_validation: self.page_validation.delta_since(before.page_validation),
            page_validation_ns: self
                .page_validation_ns
                .saturating_sub(before.page_validation_ns),
            touched_corrupt_pages: self
                .touched_corrupt_pages
                .saturating_sub(before.touched_corrupt_pages),
            page_cache_hits: self.page_cache_hits.saturating_sub(before.page_cache_hits),
            page_cache_misses: self
                .page_cache_misses
                .saturating_sub(before.page_cache_misses),
            page_cache_evictions: self
                .page_cache_evictions
                .saturating_sub(before.page_cache_evictions),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AtomicReadCount {
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl AtomicReadCount {
    fn record(&self, bytes: usize) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn snapshot(&self) -> SegmentSymbolReadCount {
        SegmentSymbolReadCount {
            calls: self.calls.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct SegmentSymbolReadCounters {
    pub(super) legacy_eager: AtomicReadCount,
    logical_returned: AtomicReadCount,
    root: AtomicReadCount,
    page: AtomicReadCount,
    page_validation: AtomicReadCount,
    page_validation_ns: AtomicU64,
    touched_corrupt_pages: AtomicU64,
    page_cache_hits: AtomicU64,
    page_cache_misses: AtomicU64,
    page_cache_evictions: AtomicU64,
}

impl SegmentSymbolReadCounters {
    fn snapshot(&self) -> SegmentSymbolReadStats {
        SegmentSymbolReadStats {
            legacy_eager: self.legacy_eager.snapshot(),
            logical_returned: self.logical_returned.snapshot(),
            root: self.root.snapshot(),
            page: self.page.snapshot(),
            page_validation: self.page_validation.snapshot(),
            page_validation_ns: self.page_validation_ns.load(Ordering::Relaxed),
            touched_corrupt_pages: self.touched_corrupt_pages.load(Ordering::Relaxed),
            page_cache_hits: self.page_cache_hits.load(Ordering::Relaxed),
            page_cache_misses: self.page_cache_misses.load(Ordering::Relaxed),
            page_cache_evictions: self.page_cache_evictions.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct SymbolRef {
    backing: SymbolRefBacking,
    local_id: usize,
}

#[derive(Clone)]
enum SymbolRefBacking {
    Paged(Arc<ValidatedSymbolPage>),
    LegacyV2(Arc<LegacySymbolDictionary>),
}

impl SymbolRef {
    pub fn symbol_id(&self) -> u32 {
        let first_symbol_id = match &self.backing {
            SymbolRefBacking::Paged(page) => page.first_symbol_id,
            SymbolRefBacking::LegacyV2(_) => 0,
        };
        first_symbol_id.saturating_add(u32::try_from(self.local_id).unwrap_or(u32::MAX))
    }

    pub fn as_str(&self) -> &str {
        match &self.backing {
            SymbolRefBacking::Paged(page) => page.symbol(self.local_id),
            SymbolRefBacking::LegacyV2(dictionary) => dictionary.symbol(self.local_id),
        }
        .expect("SymbolRef is constructed only from a validated page and local id")
    }
}

impl Deref for SymbolRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SymbolRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SymbolRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SymbolRef")
            .field("symbol_id", &self.symbol_id())
            .field("value", &self.as_str())
            .finish()
    }
}

impl fmt::Display for SymbolRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct CachedPage {
    page: Arc<ValidatedSymbolPage>,
    charge_bytes: usize,
    last_access: u64,
}

#[derive(Debug)]
struct SymbolPageCache {
    max_bytes: usize,
    charge_bytes: usize,
    access_clock: u64,
    pages: HashMap<u32, CachedPage>,
}

impl SymbolPageCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            charge_bytes: 0,
            access_clock: 0,
            pages: HashMap::new(),
        }
    }

    fn get(&mut self, page_index: u32) -> Option<Arc<ValidatedSymbolPage>> {
        self.access_clock = self.access_clock.saturating_add(1);
        let page = self.pages.get_mut(&page_index)?;
        page.last_access = self.access_clock;
        Some(Arc::clone(&page.page))
    }

    fn insert(
        &mut self,
        page_index: u32,
        page: Arc<ValidatedSymbolPage>,
    ) -> (Arc<ValidatedSymbolPage>, u64) {
        if let Some(existing) = self.get(page_index) {
            return (existing, 0);
        }

        // Charge the owned validated page allocation plus the fixed key/value
        // bookkeeping retained by the cache. Hash-table allocator slack is
        // bounded indirectly by this charged entry count.
        let charge_bytes = page
            .charge_bytes()
            .saturating_add(std::mem::size_of::<u32>())
            .saturating_add(std::mem::size_of::<CachedPage>());
        if self.max_bytes == 0 || charge_bytes > self.max_bytes {
            return (page, 0);
        }

        let mut evictions = 0u64;
        while self.charge_bytes.saturating_add(charge_bytes) > self.max_bytes {
            let Some((&evicted_index, _)) = self
                .pages
                .iter()
                .min_by_key(|(_, cached)| cached.last_access)
            else {
                break;
            };
            if let Some(evicted) = self.pages.remove(&evicted_index) {
                self.charge_bytes = self.charge_bytes.saturating_sub(evicted.charge_bytes);
                evictions = evictions.saturating_add(1);
            }
        }

        self.access_clock = self.access_clock.saturating_add(1);
        self.charge_bytes = self.charge_bytes.saturating_add(charge_bytes);
        self.pages.insert(
            page_index,
            CachedPage {
                page: Arc::clone(&page),
                charge_bytes,
                last_access: self.access_clock,
            },
        );
        (page, evictions)
    }
}

#[derive(Debug, Clone)]
struct CachedIoError {
    kind: io::ErrorKind,
    message: String,
}

impl CachedIoError {
    fn from_error(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn to_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

pub(super) struct SegmentSymbolReaderState<R>
where
    R: SegmentSymbolReadAt,
{
    source: Option<Arc<R>>,
    pub(super) root: SymbolRoot,
    legacy_v2: Option<Arc<LegacySymbolDictionary>>,
    cache: Mutex<SymbolPageCache>,
    sticky_corruption: Mutex<Option<CachedIoError>>,
}

pub struct SegmentSymbolReader<R>
where
    R: SegmentSymbolReadAt,
{
    pub(super) state: Arc<SegmentSymbolReaderState<R>>,
    counters: SegmentSymbolReadCounters,
}

impl<R> fmt::Debug for SegmentSymbolReader<R>
where
    R: SegmentSymbolReadAt,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentSymbolReader")
            .field("symbol_count", &self.state.root.symbol_count)
            .field("page_count", &self.state.root.descriptors.len())
            .field("read_stats", &self.read_stats())
            .finish_non_exhaustive()
    }
}

impl<R> SegmentSymbolReader<R>
where
    R: SegmentSymbolReadAt,
{
    pub fn open(source: R) -> io::Result<Self> {
        Self::open_with_cache_max_bytes(source, DEFAULT_SYMBOL_PAGE_CACHE_MAX_BYTES)
    }

    pub fn open_with_cache_max_bytes(source: R, cache_max_bytes: usize) -> io::Result<Self> {
        let source = Arc::new(source);
        let counters = SegmentSymbolReadCounters::default();
        let root = read_root(source.as_ref(), &counters)?;
        Ok(Self {
            state: Arc::new(SegmentSymbolReaderState {
                source: Some(source),
                root,
                legacy_v2: None,
                cache: Mutex::new(SymbolPageCache::new(cache_max_bytes)),
                sticky_corruption: Mutex::new(None),
            }),
            counters,
        })
    }

    /// Opens the schema-5 `symbols.bin` v2 layout for read-only A/B benchmarks.
    ///
    /// This is deliberately not an automatic fallback: production schema-6
    /// open continues to accept only v3. The legacy file is read and validated
    /// eagerly, matching the baseline layout's query-time ownership model.
    pub fn open_legacy_v2_for_layout_ab(source: R) -> io::Result<Self> {
        let counters = SegmentSymbolReadCounters::default();
        let dictionary = Arc::new(read_legacy_v2_dictionary(&source, &counters)?);
        let symbol_count = u32::try_from(dictionary.len())
            .map_err(|_| invalid_symbols_data("legacy v2 symbol count exceeds u32"))?;
        let source_file_bytes = dictionary.source_file_bytes;
        Ok(Self {
            state: Arc::new(SegmentSymbolReaderState {
                // The eager baseline drops its file after materialization.
                source: None,
                root: SymbolRoot {
                    symbol_count,
                    source_file_bytes,
                    encoded_bytes: 0,
                    descriptors: Box::new([]),
                    fences: Box::new([]),
                },
                legacy_v2: Some(dictionary),
                cache: Mutex::new(SymbolPageCache::new(0)),
                sticky_corruption: Mutex::new(None),
            }),
            counters,
        })
    }

    pub fn try_clone_reader(&self) -> io::Result<Self> {
        Ok(Self {
            state: Arc::clone(&self.state),
            counters: SegmentSymbolReadCounters::default(),
        })
    }

    pub fn len(&self) -> usize {
        self.state.root.symbol_count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.state.root.symbol_count == 0
    }

    pub(crate) fn is_legacy_v2_for_layout_ab(&self) -> bool {
        self.state.legacy_v2.is_some()
    }

    pub fn lookup(&self, value: &str) -> io::Result<Option<u32>> {
        self.check_sticky_corruption()?;
        if let Some(dictionary) = &self.state.legacy_v2 {
            let result = dictionary.lookup(value.as_bytes());
            if result.is_some() {
                self.counters.logical_returned.record(value.len());
            }
            return Ok(result);
        }
        let target = value.as_bytes();
        let Some(page_index) = self.lookup_page_index(target) else {
            return Ok(None);
        };
        let page = self.load_page(page_index)?;
        let result = Self::lookup_loaded_page(&page, target)?;
        if result.is_some() {
            self.counters.logical_returned.record(target.len());
        }
        Ok(result)
    }

    pub fn lookup_many<S>(&self, values: &[S]) -> io::Result<Vec<Option<u32>>>
    where
        S: AsRef<str>,
    {
        let mut results = vec![None; values.len()];
        if values.is_empty() {
            return Ok(results);
        }
        self.check_sticky_corruption()?;
        if let Some(dictionary) = &self.state.legacy_v2 {
            let results = values
                .iter()
                .map(|value| dictionary.lookup(value.as_ref().as_bytes()))
                .collect::<Vec<_>>();
            for (value, result) in values.iter().zip(&results) {
                if result.is_some() {
                    self.counters.logical_returned.record(value.as_ref().len());
                }
            }
            return Ok(results);
        }

        // Keep groups in first-request order. Besides being deterministic, this
        // preserves which touched corrupt page a scalar caller would observe
        // first while ensuring every required page is loaded at most once.
        let mut group_indexes = HashMap::new();
        let mut groups: Vec<(usize, Vec<(usize, &str)>)> = Vec::new();
        for (result_index, value) in values.iter().enumerate() {
            let value = value.as_ref();
            let Some(page_index) = self.lookup_page_index(value.as_bytes()) else {
                continue;
            };
            let group_index = match group_indexes.get(&page_index).copied() {
                Some(group_index) => group_index,
                None => {
                    let group_index = groups.len();
                    group_indexes.insert(page_index, group_index);
                    groups.push((page_index, Vec::new()));
                    group_index
                }
            };
            groups[group_index].1.push((result_index, value));
        }

        for (page_index, requests) in groups {
            let page = self.load_page(page_index)?;
            for (result_index, value) in requests {
                results[result_index] = Self::lookup_loaded_page(&page, value.as_bytes())?;
            }
        }
        for (value, result) in values.iter().zip(&results) {
            if result.is_some() {
                self.counters.logical_returned.record(value.as_ref().len());
            }
        }
        Ok(results)
    }

    fn lookup_loaded_page(page: &ValidatedSymbolPage, target: &[u8]) -> io::Result<Option<u32>> {
        let mut page_low = 0usize;
        let mut page_high = page.len();
        while page_low < page_high {
            let mid = page_low + (page_high - page_low) / 2;
            let candidate = page.symbol(mid).ok_or_else(|| {
                invalid_symbols_data("validated symbols page has a missing symbol")
            })?;
            match candidate.as_bytes().cmp(target) {
                std::cmp::Ordering::Less => page_low = mid + 1,
                std::cmp::Ordering::Equal => {
                    let local_id = u32::try_from(mid)
                        .map_err(|_| invalid_symbols_data("symbols page local id exceeds u32"))?;
                    return Ok(Some(
                        page.first_symbol_id
                            .checked_add(local_id)
                            .ok_or_else(|| invalid_symbols_data("symbol id overflow"))?,
                    ));
                }
                std::cmp::Ordering::Greater => page_high = mid,
            }
        }
        Ok(None)
    }

    pub fn resolve(&self, symbol_id: u32) -> io::Result<Option<SymbolRef>> {
        self.check_sticky_corruption()?;
        if let Some(dictionary) = &self.state.legacy_v2 {
            let local_id = usize::try_from(symbol_id)
                .map_err(|_| invalid_symbols_data("legacy v2 symbol id exceeds usize"))?;
            let Some(value) = dictionary.symbol(local_id) else {
                return Ok(None);
            };
            self.counters.logical_returned.record(value.len());
            return Ok(Some(SymbolRef {
                backing: SymbolRefBacking::LegacyV2(Arc::clone(dictionary)),
                local_id,
            }));
        }
        let Some((page_index, local_id)) = self.resolve_page_and_local_id(symbol_id)? else {
            return Ok(None);
        };
        let page = self.load_page(page_index)?;
        let value_len = page
            .symbol(local_id)
            .ok_or_else(|| invalid_symbols_data("validated symbols page has a missing symbol"))?
            .len();
        self.counters.logical_returned.record(value_len);
        Ok(Some(SymbolRef {
            backing: SymbolRefBacking::Paged(page),
            local_id,
        }))
    }

    pub fn resolve_many(&self, symbol_ids: &[u32]) -> io::Result<Vec<Option<SymbolRef>>> {
        let mut results = Vec::new();
        results.resize_with(symbol_ids.len(), || None);
        if symbol_ids.is_empty() {
            return Ok(results);
        }
        self.check_sticky_corruption()?;
        if let Some(dictionary) = &self.state.legacy_v2 {
            for (result_index, &symbol_id) in symbol_ids.iter().enumerate() {
                let local_id = usize::try_from(symbol_id)
                    .map_err(|_| invalid_symbols_data("legacy v2 symbol id exceeds usize"))?;
                if dictionary.symbol(local_id).is_some() {
                    results[result_index] = Some(SymbolRef {
                        backing: SymbolRefBacking::LegacyV2(Arc::clone(dictionary)),
                        local_id,
                    });
                }
            }
            for result in results.iter().flatten() {
                self.counters.logical_returned.record(result.as_str().len());
            }
            return Ok(results);
        }

        let mut group_indexes = HashMap::new();
        let mut groups: Vec<(usize, Vec<(usize, usize)>)> = Vec::new();
        for (result_index, &symbol_id) in symbol_ids.iter().enumerate() {
            let Some((page_index, local_id)) = self.resolve_page_and_local_id(symbol_id)? else {
                continue;
            };
            let group_index = match group_indexes.get(&page_index).copied() {
                Some(group_index) => group_index,
                None => {
                    let group_index = groups.len();
                    group_indexes.insert(page_index, group_index);
                    groups.push((page_index, Vec::new()));
                    group_index
                }
            };
            groups[group_index].1.push((result_index, local_id));
        }

        for (page_index, requests) in groups {
            let page = self.load_page(page_index)?;
            for (result_index, local_id) in requests {
                if page.symbol(local_id).is_none() {
                    return Err(invalid_symbols_data(
                        "validated symbols page has a missing symbol",
                    ));
                }
                results[result_index] = Some(SymbolRef {
                    backing: SymbolRefBacking::Paged(Arc::clone(&page)),
                    local_id,
                });
            }
        }
        for result in results.iter().flatten() {
            self.counters.logical_returned.record(result.as_str().len());
        }
        Ok(results)
    }

    /// Visits a fully resolved prefix page by page without retaining
    /// `SymbolRef` page ownership across the whole request. The callback
    /// receives the original request index. A missing/out-of-range ID stops
    /// the request: preceding pages are still validated and visited, later
    /// IDs are not touched, and the returned boolean is false.
    pub(crate) fn visit_resolved_many(
        &self,
        symbol_ids: &[u32],
        mut visit: impl FnMut(usize, &str) -> io::Result<()>,
    ) -> io::Result<bool> {
        if symbol_ids.is_empty() {
            return Ok(true);
        }
        self.check_sticky_corruption()?;
        if let Some(dictionary) = &self.state.legacy_v2 {
            for (result_index, &symbol_id) in symbol_ids.iter().enumerate() {
                let local_id = usize::try_from(symbol_id)
                    .map_err(|_| invalid_symbols_data("legacy v2 symbol id exceeds usize"))?;
                let Some(value) = dictionary.symbol(local_id) else {
                    return Ok(false);
                };
                self.counters.logical_returned.record(value.len());
                visit(result_index, value)?;
            }
            return Ok(true);
        }

        let mut group_indexes = HashMap::new();
        let mut groups: Vec<(usize, Vec<(usize, usize)>)> = Vec::new();
        let mut all_resolved = true;
        for (result_index, &symbol_id) in symbol_ids.iter().enumerate() {
            let Some((page_index, local_id)) = self.resolve_page_and_local_id(symbol_id)? else {
                all_resolved = false;
                break;
            };
            let group_index = match group_indexes.get(&page_index).copied() {
                Some(group_index) => group_index,
                None => {
                    let group_index = groups.len();
                    group_indexes.insert(page_index, group_index);
                    groups.push((page_index, Vec::new()));
                    group_index
                }
            };
            groups[group_index].1.push((result_index, local_id));
        }

        for (page_index, requests) in groups {
            let page = self.load_page(page_index)?;
            for (result_index, local_id) in requests {
                let value = page.symbol(local_id).ok_or_else(|| {
                    invalid_symbols_data("validated symbols page has a missing symbol")
                })?;
                self.counters.logical_returned.record(value.len());
                visit(result_index, value)?;
            }
        }
        Ok(all_resolved)
    }

    pub fn validate_all(&self) -> io::Result<()> {
        if self.state.legacy_v2.is_some() {
            return Ok(());
        }
        for page_index in 0..self.state.root.descriptors.len() {
            self.load_page(page_index)?;
        }
        Ok(())
    }

    pub fn read_stats(&self) -> SegmentSymbolReadStats {
        self.counters.snapshot()
    }

    pub(crate) fn state_identity(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }

    pub fn resource_snapshot(&self) -> io::Result<SegmentSymbolResourceSnapshot> {
        if let Some(dictionary) = &self.state.legacy_v2 {
            return Ok(SegmentSymbolResourceSnapshot {
                retained_open_files: 0,
                source_file_bytes: dictionary.source_file_bytes,
                root_encoded_bytes: 0,
                root_retained_charge_bytes: 0,
                eager_dictionary_retained_charge_bytes: u64::try_from(
                    dictionary.retained_charge_bytes(),
                )
                .unwrap_or(u64::MAX),
                page_cache_charge_bytes: 0,
                page_cache_max_bytes: 0,
            });
        }
        let cache = self
            .state
            .cache
            .lock()
            .map_err(|_| io::Error::other("symbol page cache lock poisoned"))?;
        Ok(SegmentSymbolResourceSnapshot {
            retained_open_files: self
                .state
                .source
                .as_deref()
                .map(|source| source.retained_open_files())
                .unwrap_or(0),
            source_file_bytes: self.state.root.source_file_bytes,
            root_encoded_bytes: u64::try_from(self.state.root.encoded_bytes).unwrap_or(u64::MAX),
            root_retained_charge_bytes: u64::try_from(self.state.root.retained_charge_bytes())
                .unwrap_or(u64::MAX),
            eager_dictionary_retained_charge_bytes: 0,
            page_cache_charge_bytes: u64::try_from(cache.charge_bytes).unwrap_or(u64::MAX),
            page_cache_max_bytes: u64::try_from(cache.max_bytes).unwrap_or(u64::MAX),
        })
    }

    pub fn cache_charge_bytes(&self) -> io::Result<usize> {
        if self.state.legacy_v2.is_some() {
            return Ok(0);
        }
        let cache = self
            .state
            .cache
            .lock()
            .map_err(|_| io::Error::other("symbol page cache lock poisoned"))?;
        Ok(cache.charge_bytes)
    }

    pub fn cache_max_bytes(&self) -> usize {
        if self.state.legacy_v2.is_some() {
            return 0;
        }
        self.state
            .cache
            .lock()
            .map(|cache| cache.max_bytes)
            .unwrap_or(0)
    }

    fn materialize_values(&self) -> io::Result<Vec<String>> {
        if let Some(dictionary) = &self.state.legacy_v2 {
            return (0..dictionary.len())
                .map(|symbol_id| {
                    dictionary
                        .symbol(symbol_id)
                        .map(str::to_string)
                        .ok_or_else(|| invalid_symbols_data("legacy v2 symbol is missing"))
                })
                .collect();
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.len())
            .map_err(|_| io::Error::other("symbols materialization allocation is too large"))?;
        for page_index in 0..self.state.root.descriptors.len() {
            let page = self.load_page(page_index)?;
            for local_id in 0..page.len() {
                values.push(
                    page.symbol(local_id)
                        .ok_or_else(|| {
                            invalid_symbols_data("validated symbols page has a missing symbol")
                        })?
                        .to_string(),
                );
            }
        }
        Ok(values)
    }

    fn lookup_page_index(&self, target: &[u8]) -> Option<usize> {
        let descriptors = &self.state.root.descriptors;
        let mut low = 0usize;
        let mut high = descriptors.len();
        while low < high {
            let mid = low + (high - low) / 2;
            if self.state.root.first_fence(&descriptors[mid]) <= target {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let page_index = low.checked_sub(1)?;
        (target <= self.state.root.last_fence(&descriptors[page_index])).then_some(page_index)
    }

    fn resolve_page_and_local_id(&self, symbol_id: u32) -> io::Result<Option<(usize, usize)>> {
        if symbol_id >= self.state.root.symbol_count {
            return Ok(None);
        }
        let descriptors = &self.state.root.descriptors;
        let mut low = 0usize;
        let mut high = descriptors.len();
        while low < high {
            let mid = low + (high - low) / 2;
            if descriptors[mid].first_symbol_id <= symbol_id {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let page_index = low
            .checked_sub(1)
            .ok_or_else(|| invalid_symbols_data("symbol id has no page descriptor"))?;
        let descriptor = &descriptors[page_index];
        let local_id = symbol_id
            .checked_sub(descriptor.first_symbol_id)
            .ok_or_else(|| invalid_symbols_data("symbol id precedes its page"))?;
        if local_id >= descriptor.symbol_count {
            return Err(invalid_symbols_data(
                "symbol id exceeds its page descriptor",
            ));
        }
        let local_id = usize::try_from(local_id)
            .map_err(|_| invalid_symbols_data("symbol local id exceeds platform usize"))?;
        Ok(Some((page_index, local_id)))
    }

    fn load_page(&self, page_index: usize) -> io::Result<Arc<ValidatedSymbolPage>> {
        self.check_sticky_corruption()?;
        let page_index_u32 = u32::try_from(page_index)
            .map_err(|_| invalid_symbols_data("symbols page index exceeds u32"))?;
        {
            let mut cache = self
                .state
                .cache
                .lock()
                .map_err(|_| io::Error::other("symbol page cache lock poisoned"))?;
            if let Some(page) = cache.get(page_index_u32) {
                self.counters
                    .page_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(page);
            }
        }
        self.counters
            .page_cache_misses
            .fetch_add(1, Ordering::Relaxed);

        let descriptor = self
            .state
            .root
            .descriptors
            .get(page_index)
            .ok_or_else(|| invalid_symbols_data("symbols page descriptor is missing"))?;
        let page_len = usize::try_from(descriptor.page_len)
            .map_err(|_| invalid_symbols_data("symbols page length exceeds platform usize"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(page_len)
            .map_err(|_| io::Error::other("symbols page allocation is too large"))?;
        bytes.resize(page_len, 0);
        let source = self
            .state
            .source
            .as_deref()
            .ok_or_else(|| invalid_symbols_data("paged symbols source is missing"))?;
        if let Err(error) = read_exact_at_counted(
            source,
            &self.counters.page,
            descriptor.page_offset,
            &mut bytes,
        ) {
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ) {
                self.counters
                    .touched_corrupt_pages
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(self.remember_corruption(error));
        }
        let validation_start = Instant::now();
        let page = match validate_page(
            page_index_u32,
            descriptor,
            self.state.root.first_fence(descriptor),
            self.state.root.last_fence(descriptor),
            bytes,
        ) {
            Ok(page) => {
                self.counters.page_validation.record(page_len);
                self.counters.page_validation_ns.fetch_add(
                    u64::try_from(validation_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                Arc::new(page)
            }
            Err(error) => {
                if matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ) {
                    self.counters
                        .touched_corrupt_pages
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(self.remember_corruption(error));
            }
        };
        self.check_sticky_corruption()?;
        let (page, evictions) = self
            .state
            .cache
            .lock()
            .map_err(|_| io::Error::other("symbol page cache lock poisoned"))?
            .insert(page_index_u32, page);
        if evictions != 0 {
            self.counters
                .page_cache_evictions
                .fetch_add(evictions, Ordering::Relaxed);
        }
        Ok(page)
    }

    fn check_sticky_corruption(&self) -> io::Result<()> {
        let sticky = self
            .state
            .sticky_corruption
            .lock()
            .map_err(|_| io::Error::other("symbol corruption cache lock poisoned"))?;
        match sticky.as_ref() {
            Some(error) => Err(error.to_error()),
            None => Ok(()),
        }
    }

    fn remember_corruption(&self, error: io::Error) -> io::Error {
        if !matches!(
            error.kind(),
            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
        ) {
            return error;
        }
        let cached = CachedIoError::from_error(&error);
        match self.state.sticky_corruption.lock() {
            Ok(mut sticky) => sticky.get_or_insert(cached).to_error(),
            Err(_) => error,
        }
    }
}

pub fn read_symbols_bin_v3(mut reader: impl Read) -> io::Result<Vec<String>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0)?.materialize_values()
}

fn read_root(
    source: &impl SegmentSymbolReadAt,
    counters: &SegmentSymbolReadCounters,
) -> io::Result<SymbolRoot> {
    let file_len = source.len()?;
    if file_len < SYMBOLS_V3_HEADER_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "symbols file is shorter than the v3 header",
        ));
    }
    let mut header = [0u8; SYMBOLS_V3_HEADER_LEN];
    read_exact_at_counted(source, &counters.root, 0, &mut header)?;
    let facts = decode_symbol_root_header(&header, file_len)?;
    let mut root = Vec::new();
    root.try_reserve_exact(facts.root_len)
        .map_err(|_| io::Error::other("symbols root allocation is too large"))?;
    root.resize(facts.root_len, 0);
    root[..SYMBOLS_V3_HEADER_LEN].copy_from_slice(&header);
    if facts.root_len > SYMBOLS_V3_HEADER_LEN {
        read_exact_at_counted(
            source,
            &counters.root,
            SYMBOLS_V3_HEADER_LEN as u64,
            &mut root[SYMBOLS_V3_HEADER_LEN..],
        )?;
    }
    decode_symbol_root(&root, facts)
}

pub(super) fn read_exact_at_counted(
    source: &impl SegmentSymbolReadAt,
    counter: &AtomicReadCount,
    offset: u64,
    destination: &mut [u8],
) -> io::Result<()> {
    source.read_exact_at(offset, destination)?;
    counter.record(destination.len());
    Ok(())
}

fn read_exact_at_loop(
    mut offset: u64,
    mut destination: &mut [u8],
    mut read_once: impl FnMut(u64, &mut [u8]) -> io::Result<usize>,
) -> io::Result<()> {
    while !destination.is_empty() {
        let read = match read_once(offset, destination) {
            Ok(0) => return Err(symbols_short_read()),
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if read > destination.len() {
            return Err(invalid_symbols_data(
                "symbols positional read exceeded its destination",
            ));
        }
        offset = offset
            .checked_add(u64::try_from(read).map_err(|_| symbols_offset_overflow())?)
            .ok_or_else(symbols_offset_overflow)?;
        destination = &mut destination[read..];
    }
    Ok(())
}

#[cfg(unix)]
fn file_read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    <File as std::os::unix::fs::FileExt>::read_at(file, destination, offset)
}

#[cfg(windows)]
fn file_read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    <File as std::os::windows::fs::FileExt>::seek_read(file, destination, offset)
}

fn symbols_offset_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "symbols positional read offset overflow",
    )
}

pub(super) fn symbols_short_read() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "symbols positional read reached EOF",
    )
}
