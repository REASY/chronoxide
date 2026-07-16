use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read, Write};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crc32c::{crc32c, crc32c_append};

#[allow(dead_code)] // Wired into the schema-neutral segment metadata backend next.
mod runtime_reader;

#[allow(unused_imports)]
pub(crate) use runtime_reader::{
    GovernedSymbolCountBinding, GovernedSymbolLogicalStats, GovernedSymbolLookupBatch,
    GovernedSymbolReader, GovernedSymbolReaderError, GovernedSymbolSession,
};

pub const SYMBOLS_V3_MAGIC: u32 = u32::from_le_bytes(*b"SYMB");
pub const SYMBOLS_V2_VERSION_FOR_LAYOUT_AB: u16 = 2;
pub const SYMBOLS_V3_VERSION: u16 = 3;
pub const SYMBOLS_V3_HEADER_LEN: usize = 80;
pub const SYMBOLS_V3_PAGE_DESCRIPTOR_LEN: usize = 48;
pub const SYMBOLS_V3_PAGE_TARGET_BYTES: usize = 32 * 1024;
pub const SYMBOLS_V3_PAGE_MAGIC: u32 = u32::from_le_bytes(*b"SYPG");
pub const SYMBOLS_V3_PAGE_VERSION: u16 = 1;
pub const SYMBOLS_V3_PAGE_HEADER_LEN: usize = 32;
pub const SYMBOLS_V3_MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_SYMBOL_PAGE_CACHE_MAX_BYTES: usize = 256 * 1024;
pub const SYMBOLS_V3_MAX_ROOT_BYTES: usize = 64 * 1024 * 1024;

const ROOT_CRC_OFFSET: usize = 72;
const ROOT_CRC_LEN: usize = 4;
const SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB: usize = 12;

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
struct AtomicReadCount {
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
struct SegmentSymbolReadCounters {
    legacy_eager: AtomicReadCount,
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

#[derive(Debug, Clone)]
struct SymbolPageDescriptor {
    first_symbol_id: u32,
    symbol_count: u32,
    page_offset: u64,
    page_len: u32,
    page_crc32c: u32,
    first_fence_offset: usize,
    first_fence_len: usize,
    last_fence_offset: usize,
    last_fence_len: usize,
    string_bytes_len: u32,
}

#[derive(Debug)]
struct SymbolRoot {
    symbol_count: u32,
    source_file_bytes: u64,
    encoded_bytes: usize,
    descriptors: Box<[SymbolPageDescriptor]>,
    fences: Box<[u8]>,
}

impl SymbolRoot {
    fn first_fence(&self, descriptor: &SymbolPageDescriptor) -> &[u8] {
        &self.fences[descriptor.first_fence_offset
            ..descriptor.first_fence_offset + descriptor.first_fence_len]
    }

    fn last_fence(&self, descriptor: &SymbolPageDescriptor) -> &[u8] {
        &self.fences
            [descriptor.last_fence_offset..descriptor.last_fence_offset + descriptor.last_fence_len]
    }

    fn retained_charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.descriptors
                    .len()
                    .saturating_mul(std::mem::size_of::<SymbolPageDescriptor>()),
            )
            .saturating_add(self.fences.len())
    }
}

#[derive(Debug)]
struct LegacySymbolDictionary {
    source_file_bytes: u64,
    offsets: Box<[usize]>,
    strings: Box<str>,
}

impl LegacySymbolDictionary {
    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    fn symbol(&self, symbol_id: usize) -> Option<&str> {
        let start = *self.offsets.get(symbol_id)?;
        let end = *self.offsets.get(symbol_id.checked_add(1)?)?;
        self.strings.get(start..end)
    }

    fn lookup(&self, target: &[u8]) -> Option<u32> {
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let mid = low + (high - low) / 2;
            match self.symbol(mid)?.as_bytes().cmp(target) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return u32::try_from(mid).ok(),
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        None
    }

    fn retained_charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(self.strings.len())
    }
}

#[derive(Debug)]
struct ValidatedSymbolPage {
    first_symbol_id: u32,
    offsets: Box<[u32]>,
    strings: Box<str>,
}

impl ValidatedSymbolPage {
    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    fn symbol(&self, local_id: usize) -> Option<&str> {
        let start = usize::try_from(*self.offsets.get(local_id)?).ok()?;
        let end = usize::try_from(*self.offsets.get(local_id + 1)?).ok()?;
        self.strings.get(start..end)
    }

    fn charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(self.strings.len())
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

struct SegmentSymbolReaderState<R>
where
    R: SegmentSymbolReadAt,
{
    source: Option<Arc<R>>,
    root: SymbolRoot,
    legacy_v2: Option<Arc<LegacySymbolDictionary>>,
    cache: Mutex<SymbolPageCache>,
    sticky_corruption: Mutex<Option<CachedIoError>>,
}

pub struct SegmentSymbolReader<R>
where
    R: SegmentSymbolReadAt,
{
    state: Arc<SegmentSymbolReaderState<R>>,
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

struct EncodedSymbolPage {
    first_symbol_id: u32,
    symbol_count: u32,
    string_bytes_len: u32,
    first_fence: Vec<u8>,
    last_fence: Vec<u8>,
    bytes: Vec<u8>,
    crc32c: u32,
}

#[derive(Clone, Copy)]
struct SymbolWriterOperationalLimits {
    max_page_bytes: usize,
    max_root_bytes: usize,
}

impl SymbolWriterOperationalLimits {
    const PRODUCTION: Self = Self {
        max_page_bytes: SYMBOLS_V3_MAX_PAGE_BYTES,
        max_root_bytes: SYMBOLS_V3_MAX_ROOT_BYTES,
    };
}

pub fn write_symbols_bin_v3<W, I, S>(writer: W, symbols: I) -> io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    write_symbols_bin_v3_with_operational_limits(
        writer,
        symbols,
        SymbolWriterOperationalLimits::PRODUCTION,
    )
}

fn write_symbols_bin_v3_with_operational_limits<W, I, S>(
    mut writer: W,
    symbols: I,
    limits: SymbolWriterOperationalLimits,
) -> io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = symbols
        .into_iter()
        .map(|value| value.as_ref().as_bytes().to_vec())
        .collect::<Vec<_>>();
    let symbol_count = u32::try_from(values.len())
        .map_err(|_| invalid_symbols_input("symbol count exceeds u32"))?;
    validate_sorted_values(&values)?;

    let pages = encode_pages(&values, limits.max_page_bytes)?;
    let page_count = u32::try_from(pages.len())
        .map_err(|_| invalid_symbols_input("symbols page count exceeds u32"))?;
    let directory_len = pages
        .len()
        .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
        .ok_or_else(|| invalid_symbols_input("symbols directory length overflow"))?;
    let fence_offset = SYMBOLS_V3_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_symbols_input("symbols fence offset overflow"))?;

    let mut fences = Vec::new();
    let mut fence_ranges = Vec::with_capacity(pages.len());
    for page in &pages {
        let first_offset = u32::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence region exceeds u32"))?;
        let first_len = u32::try_from(page.first_fence.len())
            .map_err(|_| invalid_symbols_input("symbols first fence exceeds u32"))?;
        fences.extend_from_slice(&page.first_fence);
        let last_offset = u32::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence region exceeds u32"))?;
        let last_len = u32::try_from(page.last_fence.len())
            .map_err(|_| invalid_symbols_input("symbols last fence exceeds u32"))?;
        fences.extend_from_slice(&page.last_fence);
        fence_ranges.push((first_offset, first_len, last_offset, last_len));
    }
    let pages_offset = fence_offset
        .checked_add(fences.len())
        .ok_or_else(|| invalid_symbols_input("symbols pages offset overflow"))?;
    if pages_offset > limits.max_root_bytes {
        return Err(invalid_symbols_input(
            "symbols root exceeds the operational size limit",
        ));
    }
    let mut file_len = u64::try_from(pages_offset)
        .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?;
    for page in &pages {
        file_len = file_len
            .checked_add(
                u64::try_from(page.bytes.len())
                    .map_err(|_| invalid_symbols_input("symbols page length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_symbols_input("symbols file length overflow"))?;
    }

    let mut root = vec![0u8; pages_offset];
    put_u32(&mut root, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut root, 4, SYMBOLS_V3_VERSION);
    put_u16(&mut root, 6, 0);
    put_u32(&mut root, 8, SYMBOLS_V3_HEADER_LEN as u32);
    put_u32(&mut root, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut root, 16, symbol_count);
    put_u32(&mut root, 20, page_count);
    put_u64(&mut root, 24, SYMBOLS_V3_HEADER_LEN as u64);
    put_u64(
        &mut root,
        32,
        u64::try_from(directory_len)
            .map_err(|_| invalid_symbols_input("symbols directory length exceeds u64"))?,
    );
    put_u64(
        &mut root,
        40,
        u64::try_from(fence_offset)
            .map_err(|_| invalid_symbols_input("symbols fence offset exceeds u64"))?,
    );
    put_u64(
        &mut root,
        48,
        u64::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence length exceeds u64"))?,
    );
    put_u64(
        &mut root,
        56,
        u64::try_from(pages_offset)
            .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?,
    );
    put_u64(&mut root, 64, file_len);
    put_u32(&mut root, ROOT_CRC_OFFSET, 0);
    put_u32(&mut root, 76, 0);

    let mut page_offset = u64::try_from(pages_offset)
        .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?;
    for (page_index, page) in pages.iter().enumerate() {
        let descriptor_offset = SYMBOLS_V3_HEADER_LEN
            .checked_add(
                page_index
                    .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
                    .ok_or_else(|| invalid_symbols_input("symbols descriptor offset overflow"))?,
            )
            .ok_or_else(|| invalid_symbols_input("symbols descriptor offset overflow"))?;
        let (first_offset, first_len, last_offset, last_len) = fence_ranges[page_index];
        put_u32(&mut root, descriptor_offset, page.first_symbol_id);
        put_u32(&mut root, descriptor_offset + 4, page.symbol_count);
        put_u64(&mut root, descriptor_offset + 8, page_offset);
        put_u32(
            &mut root,
            descriptor_offset + 16,
            u32::try_from(page.bytes.len())
                .map_err(|_| invalid_symbols_input("symbols page length exceeds u32"))?,
        );
        put_u32(&mut root, descriptor_offset + 20, page.crc32c);
        put_u32(&mut root, descriptor_offset + 24, first_offset);
        put_u32(&mut root, descriptor_offset + 28, first_len);
        put_u32(&mut root, descriptor_offset + 32, last_offset);
        put_u32(&mut root, descriptor_offset + 36, last_len);
        put_u32(&mut root, descriptor_offset + 40, page.string_bytes_len);
        put_u32(&mut root, descriptor_offset + 44, 0);
        page_offset = page_offset
            .checked_add(page.bytes.len() as u64)
            .ok_or_else(|| invalid_symbols_input("symbols page offset overflow"))?;
    }
    root[fence_offset..pages_offset].copy_from_slice(&fences);
    let root_crc = symbols_root_crc(&root);
    put_u32(&mut root, ROOT_CRC_OFFSET, root_crc);
    writer.write_all(&root)?;
    for page in pages {
        writer.write_all(&page.bytes)?;
    }
    Ok(())
}

pub fn read_symbols_bin_v3(mut reader: impl Read) -> io::Result<Vec<String>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0)?.materialize_values()
}

fn encode_pages(values: &[Vec<u8>], max_page_bytes: usize) -> io::Result<Vec<EncodedSymbolPage>> {
    let mut pages = Vec::new();
    let mut start = 0usize;
    while start < values.len() {
        let mut end = start;
        let mut string_bytes_len = 0usize;
        while end < values.len() {
            let candidate_strings_len = string_bytes_len
                .checked_add(values[end].len())
                .ok_or_else(|| invalid_symbols_input("symbols page string length overflow"))?;
            let candidate_count = end - start + 1;
            let candidate_len = encoded_page_len(candidate_count, candidate_strings_len)?;
            if candidate_count > 1 && candidate_len > SYMBOLS_V3_PAGE_TARGET_BYTES {
                break;
            }
            string_bytes_len = candidate_strings_len;
            end += 1;
            if candidate_len > SYMBOLS_V3_PAGE_TARGET_BYTES {
                break;
            }
        }
        if end == start {
            return Err(invalid_symbols_input("symbols page made no progress"));
        }
        pages.push(encode_page(
            u32::try_from(pages.len())
                .map_err(|_| invalid_symbols_input("symbols page index exceeds u32"))?,
            start,
            &values[start..end],
            max_page_bytes,
        )?);
        start = end;
    }
    Ok(pages)
}

fn encode_page(
    page_index: u32,
    first_symbol_id: usize,
    values: &[Vec<u8>],
    max_page_bytes: usize,
) -> io::Result<EncodedSymbolPage> {
    let symbol_count = u32::try_from(values.len())
        .map_err(|_| invalid_symbols_input("symbols page count exceeds u32"))?;
    let first_symbol_id = u32::try_from(first_symbol_id)
        .map_err(|_| invalid_symbols_input("first symbol id exceeds u32"))?;
    let string_bytes_len = values.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value.len())
            .ok_or_else(|| invalid_symbols_input("symbols page string length overflow"))
    })?;
    let page_len = encoded_page_len(values.len(), string_bytes_len)?;
    if page_len > max_page_bytes {
        return Err(invalid_symbols_input(
            "symbols page exceeds the operational size limit",
        ));
    }
    let offsets_len = values
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| invalid_symbols_input("symbols page offsets length overflow"))?;
    let mut bytes = vec![0u8; page_len];
    put_u32(&mut bytes, 0, SYMBOLS_V3_PAGE_MAGIC);
    put_u16(&mut bytes, 4, SYMBOLS_V3_PAGE_VERSION);
    put_u16(&mut bytes, 6, 0);
    put_u32(&mut bytes, 8, page_index);
    put_u32(&mut bytes, 12, first_symbol_id);
    put_u32(&mut bytes, 16, symbol_count);
    put_u32(
        &mut bytes,
        20,
        u32::try_from(offsets_len)
            .map_err(|_| invalid_symbols_input("symbols page offsets exceed u32"))?,
    );
    put_u32(
        &mut bytes,
        24,
        u32::try_from(string_bytes_len)
            .map_err(|_| invalid_symbols_input("symbols page strings exceed u32"))?,
    );
    put_u32(&mut bytes, 28, 0);

    let strings_offset = SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len;
    let mut string_cursor = 0usize;
    put_u32(&mut bytes, SYMBOLS_V3_PAGE_HEADER_LEN, 0);
    for (index, value) in values.iter().enumerate() {
        let destination_start = strings_offset + string_cursor;
        let destination_end = destination_start + value.len();
        bytes[destination_start..destination_end].copy_from_slice(value);
        string_cursor += value.len();
        put_u32(
            &mut bytes,
            SYMBOLS_V3_PAGE_HEADER_LEN + (index + 1) * 4,
            u32::try_from(string_cursor)
                .map_err(|_| invalid_symbols_input("symbols page offset exceeds u32"))?,
        );
    }
    let crc32c = crc32c(&bytes);
    Ok(EncodedSymbolPage {
        first_symbol_id,
        symbol_count,
        string_bytes_len: u32::try_from(string_bytes_len)
            .map_err(|_| invalid_symbols_input("symbols page strings exceed u32"))?,
        first_fence: values.first().cloned().unwrap_or_default(),
        last_fence: values.last().cloned().unwrap_or_default(),
        bytes,
        crc32c,
    })
}

fn encoded_page_len(symbol_count: usize, string_bytes_len: usize) -> io::Result<usize> {
    SYMBOLS_V3_PAGE_HEADER_LEN
        .checked_add(
            symbol_count
                .checked_add(1)
                .and_then(|count| count.checked_mul(4))
                .ok_or_else(|| invalid_symbols_input("symbols page offsets length overflow"))?,
        )
        .and_then(|length| length.checked_add(string_bytes_len))
        .ok_or_else(|| invalid_symbols_input("symbols page length overflow"))
}

fn validate_sorted_values(values: &[Vec<u8>]) -> io::Result<()> {
    for pair in values.windows(2) {
        if pair[0] >= pair[1] {
            return Err(invalid_symbols_input(
                "symbols must be sorted by unique UTF-8 bytes",
            ));
        }
    }
    Ok(())
}

fn read_legacy_v2_dictionary(
    source: &impl SegmentSymbolReadAt,
    counters: &SegmentSymbolReadCounters,
) -> io::Result<LegacySymbolDictionary> {
    let source_file_bytes = source.len()?;
    let file_len = usize::try_from(source_file_bytes)
        .map_err(|_| invalid_symbols_data("legacy v2 symbols length exceeds platform usize"))?;
    if file_len < SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "legacy v2 symbols file is shorter than its header",
        ));
    }

    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(file_len)
        .map_err(|_| io::Error::other("legacy v2 symbols allocation is too large"))?;
    bytes.resize(file_len, 0);
    read_exact_at_counted(source, &counters.legacy_eager, 0, &mut bytes)?;

    if read_u32_at(&bytes, 0) != SYMBOLS_V3_MAGIC {
        return Err(invalid_symbols_data("symbols magic mismatch"));
    }
    if read_u16_at(&bytes, 4) != SYMBOLS_V2_VERSION_FOR_LAYOUT_AB {
        return Err(invalid_symbols_data("unsupported symbols version"));
    }
    if read_u16_at(&bytes, 6) != 0 {
        return Err(invalid_symbols_data("legacy v2 symbols flags are non-zero"));
    }
    let symbol_count = usize::try_from(read_u32_at(&bytes, 8))
        .map_err(|_| invalid_symbols_data("legacy v2 symbol count exceeds platform usize"))?;
    let offset_count = symbol_count
        .checked_add(1)
        .ok_or_else(|| invalid_symbols_data("legacy v2 offset count overflow"))?;
    let offsets_bytes = offset_count
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| invalid_symbols_data("legacy v2 offset table length overflow"))?;
    let strings_start = SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB
        .checked_add(offsets_bytes)
        .ok_or_else(|| invalid_symbols_data("legacy v2 string section offset overflow"))?;
    if strings_start > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "legacy v2 symbols offset table is truncated",
        ));
    }

    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(|_| io::Error::other("legacy v2 offset allocation is too large"))?;
    for offset_index in 0..offset_count {
        let byte_offset = SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB
            .checked_add(offset_index.saturating_mul(std::mem::size_of::<u64>()))
            .ok_or_else(|| invalid_symbols_data("legacy v2 offset position overflow"))?;
        offsets.push(
            usize::try_from(read_u64_at(&bytes, byte_offset)).map_err(|_| {
                invalid_symbols_data("legacy v2 symbol offset exceeds platform usize")
            })?,
        );
    }
    if offsets.first().copied() != Some(0) {
        return Err(invalid_symbols_data(
            "legacy v2 symbols first offset must be zero",
        ));
    }
    let strings_len = bytes.len() - strings_start;
    if offsets.last().copied() != Some(strings_len) {
        return Err(invalid_symbols_data(
            "legacy v2 symbols final offset must match file length",
        ));
    }

    bytes.drain(..strings_start);
    let strings = String::from_utf8(bytes)
        .map_err(|_| invalid_symbols_data("legacy v2 symbols are not valid UTF-8"))?;
    let mut previous: Option<&[u8]> = None;
    for pair in offsets.windows(2) {
        let value = strings.get(pair[0]..pair[1]).ok_or_else(|| {
            invalid_symbols_data("legacy v2 symbol offsets are out of order or out of bounds")
        })?;
        if previous.is_some_and(|previous| previous >= value.as_bytes()) {
            return Err(invalid_symbols_data(
                "legacy v2 symbols are not strictly sorted and unique",
            ));
        }
        previous = Some(value.as_bytes());
    }

    Ok(LegacySymbolDictionary {
        source_file_bytes,
        offsets: offsets.into_boxed_slice(),
        strings: strings.into_boxed_str(),
    })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SymbolRootHeaderFacts {
    file_len: u64,
    symbol_count: u32,
    page_count: u32,
    fence_offset: u64,
    pages_offset: u64,
    root_len: usize,
}

fn decode_symbol_root_header(header: &[u8], file_len: u64) -> io::Result<SymbolRootHeaderFacts> {
    if header.len() != SYMBOLS_V3_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "symbols v3 header length is not exact",
        ));
    }
    if read_u32_at(&header, 0) != SYMBOLS_V3_MAGIC {
        return Err(invalid_symbols_data("symbols magic mismatch"));
    }
    if read_u16_at(&header, 4) != SYMBOLS_V3_VERSION {
        return Err(invalid_symbols_data("unsupported symbols version"));
    }
    if read_u16_at(&header, 6) != 0 {
        return Err(invalid_symbols_data("symbols flags are non-zero"));
    }
    if read_u32_at(&header, 8) != SYMBOLS_V3_HEADER_LEN as u32 {
        return Err(invalid_symbols_data("symbols header length is invalid"));
    }
    if read_u32_at(&header, 12) != SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32 {
        return Err(invalid_symbols_data(
            "symbols page descriptor length is invalid",
        ));
    }
    if read_u32_at(&header, 76) != 0 {
        return Err(invalid_symbols_data("symbols reserved field is non-zero"));
    }
    let symbol_count = read_u32_at(&header, 16);
    let page_count = read_u32_at(&header, 20);
    if (symbol_count == 0) != (page_count == 0) {
        return Err(invalid_symbols_data(
            "symbols and page counts disagree about emptiness",
        ));
    }
    if page_count > symbol_count {
        return Err(invalid_symbols_data(
            "symbols page count exceeds symbol count",
        ));
    }
    if read_u64_at(&header, 24) != SYMBOLS_V3_HEADER_LEN as u64 {
        return Err(invalid_symbols_data("symbols directory offset is invalid"));
    }
    let expected_directory_len = u64::from(page_count)
        .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64)
        .ok_or_else(|| invalid_symbols_data("symbols directory length overflow"))?;
    if read_u64_at(&header, 32) != expected_directory_len {
        return Err(invalid_symbols_data("symbols directory length is invalid"));
    }
    let expected_fence_offset = (SYMBOLS_V3_HEADER_LEN as u64)
        .checked_add(expected_directory_len)
        .ok_or_else(|| invalid_symbols_data("symbols fence offset overflow"))?;
    if read_u64_at(&header, 40) != expected_fence_offset {
        return Err(invalid_symbols_data("symbols fence offset is invalid"));
    }
    let fence_len = read_u64_at(&header, 48);
    let expected_pages_offset = expected_fence_offset
        .checked_add(fence_len)
        .ok_or_else(|| invalid_symbols_data("symbols pages offset overflow"))?;
    if read_u64_at(&header, 56) != expected_pages_offset {
        return Err(invalid_symbols_data("symbols pages offset is invalid"));
    }
    if read_u64_at(&header, 64) != file_len {
        return Err(invalid_symbols_data("symbols file length is invalid"));
    }
    if expected_pages_offset > file_len {
        return Err(invalid_symbols_data("symbols root exceeds the file"));
    }
    let root_len = usize::try_from(expected_pages_offset)
        .map_err(|_| invalid_symbols_data("symbols root length exceeds platform usize"))?;
    if root_len > SYMBOLS_V3_MAX_ROOT_BYTES {
        return Err(invalid_symbols_data(
            "symbols root exceeds the operational size limit",
        ));
    }
    Ok(SymbolRootHeaderFacts {
        file_len,
        symbol_count,
        page_count,
        fence_offset: expected_fence_offset,
        pages_offset: expected_pages_offset,
        root_len,
    })
}

fn decode_symbol_root(root: &[u8], facts: SymbolRootHeaderFacts) -> io::Result<SymbolRoot> {
    if root.len() != facts.root_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "symbols root length is not exact",
        ));
    }
    if symbols_root_crc(&root) != read_u32_at(&root, ROOT_CRC_OFFSET) {
        return Err(invalid_symbols_data("symbols root CRC mismatch"));
    }

    let fence_start = usize::try_from(facts.fence_offset)
        .map_err(|_| invalid_symbols_data("symbols fence offset exceeds platform usize"))?;
    let fence_end = usize::try_from(facts.pages_offset)
        .map_err(|_| invalid_symbols_data("symbols pages offset exceeds platform usize"))?;
    let fences = root
        .get(fence_start..fence_end)
        .ok_or_else(|| invalid_symbols_data("symbols fence region is out of bounds"))?
        .to_vec()
        .into_boxed_slice();
    let page_count_usize = usize::try_from(facts.page_count)
        .map_err(|_| invalid_symbols_data("symbols page count exceeds platform usize"))?;
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(page_count_usize)
        .map_err(|_| io::Error::other("symbols page descriptor allocation is too large"))?;
    let mut expected_symbol_id = 0u32;
    let mut expected_page_offset = facts.pages_offset;
    let mut expected_fence_offset = 0usize;
    let mut previous_last_fence: Option<Vec<u8>> = None;
    for page_index in 0..page_count_usize {
        let offset = SYMBOLS_V3_HEADER_LEN + page_index * SYMBOLS_V3_PAGE_DESCRIPTOR_LEN;
        let descriptor_bytes = root
            .get(offset..offset + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
            .ok_or_else(|| invalid_symbols_data("symbols page descriptor is truncated"))?;
        let first_symbol_id = read_u32_at(descriptor_bytes, 0);
        let descriptor_symbol_count = read_u32_at(descriptor_bytes, 4);
        if descriptor_symbol_count == 0 {
            return Err(invalid_symbols_data(
                "symbols page descriptor has no symbols",
            ));
        }
        if first_symbol_id != expected_symbol_id {
            return Err(invalid_symbols_data(
                "symbols page symbol ids are not contiguous",
            ));
        }
        expected_symbol_id = expected_symbol_id
            .checked_add(descriptor_symbol_count)
            .ok_or_else(|| invalid_symbols_data("symbols page symbol count overflow"))?;
        let page_offset = read_u64_at(descriptor_bytes, 8);
        if page_offset != expected_page_offset {
            return Err(invalid_symbols_data(
                "symbols page byte ranges are not contiguous",
            ));
        }
        let page_len = read_u32_at(descriptor_bytes, 16);
        if u64::from(page_len) > SYMBOLS_V3_MAX_PAGE_BYTES as u64 {
            return Err(invalid_symbols_data(
                "symbols page exceeds the operational size limit",
            ));
        }
        let string_bytes_len = read_u32_at(descriptor_bytes, 40);
        let expected_page_len = u64::from(descriptor_symbol_count)
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .and_then(|offsets_len| offsets_len.checked_add(SYMBOLS_V3_PAGE_HEADER_LEN as u64))
            .and_then(|length| length.checked_add(u64::from(string_bytes_len)))
            .ok_or_else(|| invalid_symbols_data("symbols page length overflow"))?;
        if u64::from(page_len) != expected_page_len {
            return Err(invalid_symbols_data("symbols page length is inconsistent"));
        }
        if descriptor_symbol_count > 1
            && usize::try_from(page_len)
                .ok()
                .is_some_and(|length| length > SYMBOLS_V3_PAGE_TARGET_BYTES)
        {
            return Err(invalid_symbols_data(
                "multi-symbol page exceeds the v3 target",
            ));
        }
        expected_page_offset = expected_page_offset
            .checked_add(u64::from(page_len))
            .ok_or_else(|| invalid_symbols_data("symbols page end overflow"))?;
        if expected_page_offset > facts.file_len {
            return Err(invalid_symbols_data("symbols page exceeds the file"));
        }
        if read_u32_at(descriptor_bytes, 44) != 0 {
            return Err(invalid_symbols_data(
                "symbols page descriptor reserved field is non-zero",
            ));
        }
        let first_fence_offset = usize::try_from(read_u32_at(descriptor_bytes, 24))
            .map_err(|_| invalid_symbols_data("symbols first fence offset exceeds usize"))?;
        let first_fence_len = usize::try_from(read_u32_at(descriptor_bytes, 28))
            .map_err(|_| invalid_symbols_data("symbols first fence length exceeds usize"))?;
        let last_fence_offset = usize::try_from(read_u32_at(descriptor_bytes, 32))
            .map_err(|_| invalid_symbols_data("symbols last fence offset exceeds usize"))?;
        let last_fence_len = usize::try_from(read_u32_at(descriptor_bytes, 36))
            .map_err(|_| invalid_symbols_data("symbols last fence length exceeds usize"))?;
        if first_fence_offset != expected_fence_offset {
            return Err(invalid_symbols_data(
                "symbols first fence is not canonically positioned",
            ));
        }
        expected_fence_offset = expected_fence_offset
            .checked_add(first_fence_len)
            .ok_or_else(|| invalid_symbols_data("symbols first fence end overflow"))?;
        if last_fence_offset != expected_fence_offset {
            return Err(invalid_symbols_data(
                "symbols last fence is not canonically positioned",
            ));
        }
        expected_fence_offset = expected_fence_offset
            .checked_add(last_fence_len)
            .ok_or_else(|| invalid_symbols_data("symbols last fence end overflow"))?;
        let first_fence = checked_fence(&fences, first_fence_offset, first_fence_len)?;
        let last_fence = checked_fence(&fences, last_fence_offset, last_fence_len)?;
        if descriptor_symbol_count == 1 {
            if first_fence != last_fence {
                return Err(invalid_symbols_data("singleton symbols page fences differ"));
            }
            if u64::try_from(first_fence.len())
                .map_err(|_| invalid_symbols_data("symbols fence length exceeds u64"))?
                != u64::from(string_bytes_len)
            {
                return Err(invalid_symbols_data(
                    "singleton symbols page string length disagrees with its fence",
                ));
            }
        } else {
            if first_fence >= last_fence {
                return Err(invalid_symbols_data(
                    "multi-symbol page fences are not strictly ordered",
                ));
            }
            let fence_string_bytes = first_fence
                .len()
                .checked_add(last_fence.len())
                .ok_or_else(|| invalid_symbols_data("symbols fence byte length overflow"))?;
            let minimum_string_bytes = u64::try_from(fence_string_bytes)
                .map_err(|_| invalid_symbols_data("symbols fence byte length exceeds u64"))?
                .checked_add(u64::from(descriptor_symbol_count - 2))
                .ok_or_else(|| invalid_symbols_data("symbols minimum byte length overflow"))?;
            if minimum_string_bytes > u64::from(string_bytes_len) {
                return Err(invalid_symbols_data(
                    "symbols page count and fences exceed its string byte length",
                ));
            }
            if descriptor_symbol_count == 2 && minimum_string_bytes != u64::from(string_bytes_len) {
                return Err(invalid_symbols_data(
                    "two-symbol page string length disagrees with its fences",
                ));
            }
        }
        if previous_last_fence
            .as_deref()
            .is_some_and(|previous| previous >= first_fence)
        {
            return Err(invalid_symbols_data(
                "symbols page fences are not strictly ordered",
            ));
        }
        previous_last_fence = Some(last_fence.to_vec());
        descriptors.push(SymbolPageDescriptor {
            first_symbol_id,
            symbol_count: descriptor_symbol_count,
            page_offset,
            page_len,
            page_crc32c: read_u32_at(descriptor_bytes, 20),
            first_fence_offset,
            first_fence_len,
            last_fence_offset,
            last_fence_len,
            string_bytes_len,
        });
    }
    if expected_symbol_id != facts.symbol_count {
        return Err(invalid_symbols_data(
            "symbols descriptor counts do not match the header",
        ));
    }
    if expected_fence_offset != fences.len() {
        return Err(invalid_symbols_data(
            "symbols fence region has trailing bytes",
        ));
    }
    if expected_page_offset != facts.file_len {
        return Err(invalid_symbols_data("symbols file has trailing bytes"));
    }
    for pair in descriptors.windows(2) {
        let current = &pair[0];
        let next = &pair[1];
        if usize::try_from(current.page_len)
            .ok()
            .is_some_and(|length| length > SYMBOLS_V3_PAGE_TARGET_BYTES)
        {
            // The earlier size check proves an oversized page is a singleton.
            continue;
        }
        let candidate_count = u64::from(current.symbol_count)
            .checked_add(1)
            .ok_or_else(|| invalid_symbols_data("symbols greedy page count overflow"))?;
        let candidate_offsets_len = candidate_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .ok_or_else(|| invalid_symbols_data("symbols greedy offsets length overflow"))?;
        let candidate_strings_len = u64::from(current.string_bytes_len)
            .checked_add(
                u64::try_from(next.first_fence_len)
                    .map_err(|_| invalid_symbols_data("symbols next fence length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_symbols_data("symbols greedy strings length overflow"))?;
        let candidate_page_len = (SYMBOLS_V3_PAGE_HEADER_LEN as u64)
            .checked_add(candidate_offsets_len)
            .and_then(|length| length.checked_add(candidate_strings_len))
            .ok_or_else(|| invalid_symbols_data("symbols greedy page length overflow"))?;
        if candidate_page_len <= SYMBOLS_V3_PAGE_TARGET_BYTES as u64 {
            return Err(invalid_symbols_data("symbols page is not greedily maximal"));
        }
    }
    Ok(SymbolRoot {
        symbol_count: facts.symbol_count,
        source_file_bytes: facts.file_len,
        encoded_bytes: facts.root_len,
        descriptors: descriptors.into_boxed_slice(),
        fences,
    })
}

fn checked_fence(fences: &[u8], offset: usize, len: usize) -> io::Result<&[u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_symbols_data("symbols fence range overflow"))?;
    let fence = fences
        .get(offset..end)
        .ok_or_else(|| invalid_symbols_data("symbols fence is out of bounds"))?;
    std::str::from_utf8(fence)
        .map_err(|_| invalid_symbols_data("symbols fence is not valid UTF-8"))?;
    Ok(fence)
}

fn validate_page(
    page_index: u32,
    descriptor: &SymbolPageDescriptor,
    first_fence: &[u8],
    last_fence: &[u8],
    bytes: Vec<u8>,
) -> io::Result<ValidatedSymbolPage> {
    if crc32c(&bytes) != descriptor.page_crc32c {
        return Err(invalid_symbols_data("symbols page CRC mismatch"));
    }
    if bytes.len() < SYMBOLS_V3_PAGE_HEADER_LEN {
        return Err(invalid_symbols_data("symbols page is truncated"));
    }
    if read_u32_at(&bytes, 0) != SYMBOLS_V3_PAGE_MAGIC {
        return Err(invalid_symbols_data("symbols page magic mismatch"));
    }
    if read_u16_at(&bytes, 4) != SYMBOLS_V3_PAGE_VERSION {
        return Err(invalid_symbols_data("symbols page version mismatch"));
    }
    if read_u16_at(&bytes, 6) != 0 {
        return Err(invalid_symbols_data("symbols page flags are non-zero"));
    }
    if read_u32_at(&bytes, 8) != page_index {
        return Err(invalid_symbols_data("symbols page index mismatch"));
    }
    if read_u32_at(&bytes, 12) != descriptor.first_symbol_id {
        return Err(invalid_symbols_data("symbols page first id mismatch"));
    }
    if read_u32_at(&bytes, 16) != descriptor.symbol_count {
        return Err(invalid_symbols_data("symbols page count mismatch"));
    }
    let expected_offsets_len = descriptor
        .symbol_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| invalid_symbols_data("symbols page offsets length overflow"))?;
    if read_u32_at(&bytes, 20) != expected_offsets_len {
        return Err(invalid_symbols_data("symbols page offsets length mismatch"));
    }
    if read_u32_at(&bytes, 24) != descriptor.string_bytes_len {
        return Err(invalid_symbols_data("symbols page strings length mismatch"));
    }
    if read_u32_at(&bytes, 28) != 0 {
        return Err(invalid_symbols_data(
            "symbols page reserved field is non-zero",
        ));
    }
    let offsets_start = SYMBOLS_V3_PAGE_HEADER_LEN;
    let offsets_end = offsets_start
        .checked_add(
            usize::try_from(expected_offsets_len)
                .map_err(|_| invalid_symbols_data("symbols page offsets exceed usize"))?,
        )
        .ok_or_else(|| invalid_symbols_data("symbols page offsets end overflow"))?;
    let strings_len = usize::try_from(descriptor.string_bytes_len)
        .map_err(|_| invalid_symbols_data("symbols page strings exceed usize"))?;
    let expected_page_len = offsets_end
        .checked_add(strings_len)
        .ok_or_else(|| invalid_symbols_data("symbols page length overflow"))?;
    if expected_page_len != bytes.len() {
        return Err(invalid_symbols_data("symbols page length mismatch"));
    }
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(descriptor.symbol_count as usize + 1)
        .map_err(|_| io::Error::other("symbols page offsets allocation is too large"))?;
    for offset in (offsets_start..offsets_end).step_by(4) {
        offsets.push(read_u32_at(&bytes, offset));
    }
    if offsets.first().copied() != Some(0) {
        return Err(invalid_symbols_data(
            "symbols page first offset must be zero",
        ));
    }
    if offsets.last().copied() != Some(descriptor.string_bytes_len) {
        return Err(invalid_symbols_data(
            "symbols page final offset does not match strings",
        ));
    }
    let string_bytes = &bytes[offsets_end..];
    let mut previous: Option<&[u8]> = None;
    for pair in offsets.windows(2) {
        let start = usize::try_from(pair[0])
            .map_err(|_| invalid_symbols_data("symbols page offset exceeds usize"))?;
        let end = usize::try_from(pair[1])
            .map_err(|_| invalid_symbols_data("symbols page offset exceeds usize"))?;
        if end < start {
            return Err(invalid_symbols_data(
                "symbols page offsets are out of order",
            ));
        }
        let value = string_bytes
            .get(start..end)
            .ok_or_else(|| invalid_symbols_data("symbols page offset is out of bounds"))?;
        std::str::from_utf8(value)
            .map_err(|_| invalid_symbols_data("symbols page value is not valid UTF-8"))?;
        if previous.is_some_and(|previous| previous >= value) {
            return Err(invalid_symbols_data(
                "symbols page values are not strictly sorted and unique",
            ));
        }
        previous = Some(value);
    }
    let first_value = offsets
        .get(0..2)
        .and_then(|pair| string_bytes.get(pair[0] as usize..pair[1] as usize))
        .ok_or_else(|| invalid_symbols_data("symbols page first value is missing"))?;
    let last_pair = offsets
        .get(offsets.len().saturating_sub(2)..)
        .ok_or_else(|| invalid_symbols_data("symbols page last value is missing"))?;
    let last_value = string_bytes
        .get(last_pair[0] as usize..last_pair[1] as usize)
        .ok_or_else(|| invalid_symbols_data("symbols page last value is missing"))?;
    if first_value != first_fence || last_value != last_fence {
        return Err(invalid_symbols_data(
            "symbols page values do not match its fences",
        ));
    }
    let strings = String::from_utf8(string_bytes.to_vec())
        .map_err(|_| invalid_symbols_data("symbols page strings are not valid UTF-8"))?;
    Ok(ValidatedSymbolPage {
        first_symbol_id: descriptor.first_symbol_id,
        offsets: offsets.into_boxed_slice(),
        strings: strings.into_boxed_str(),
    })
}

fn symbols_root_crc(root: &[u8]) -> u32 {
    let before = root.get(..ROOT_CRC_OFFSET).unwrap_or(root);
    let after = root
        .get(ROOT_CRC_OFFSET + ROOT_CRC_LEN..)
        .unwrap_or_default();
    crc32c_append(crc32c_append(crc32c(before), &[0; ROOT_CRC_LEN]), after)
}

fn read_exact_at_counted(
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

fn symbols_short_read() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "symbols positional read reached EOF",
    )
}

fn invalid_symbols_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_symbols_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, ErrorKind};

    use super::*;

    struct HeaderOnlySource {
        header: [u8; SYMBOLS_V3_HEADER_LEN],
        file_len: u64,
    }

    impl SegmentSymbolReadAt for HeaderOnlySource {
        fn len(&self) -> io::Result<u64> {
            Ok(self.file_len)
        }

        fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
            if offset == 0 && destination.len() == self.header.len() {
                destination.copy_from_slice(&self.header);
                return Ok(());
            }
            Err(symbols_short_read())
        }
    }

    struct SparsePrefixSource {
        prefix: Vec<u8>,
        file_len: u64,
    }

    impl SegmentSymbolReadAt for SparsePrefixSource {
        fn len(&self) -> io::Result<u64> {
            Ok(self.file_len)
        }

        fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
            let start = usize::try_from(offset).map_err(|_| symbols_short_read())?;
            let end = start
                .checked_add(destination.len())
                .ok_or_else(symbols_short_read)?;
            let source = self.prefix.get(start..end).ok_or_else(symbols_short_read)?;
            destination.copy_from_slice(source);
            Ok(())
        }
    }

    fn encoded(values: &[String]) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_symbols_bin_v3(&mut bytes, values).unwrap();
        bytes
    }

    fn encoded_v2_for_layout_ab(values: &[String]) -> Vec<u8> {
        let mut strings = Vec::new();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0u64);
        for value in values {
            strings.extend_from_slice(value.as_bytes());
            offsets.push(strings.len() as u64);
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SYMBOLS_V3_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&SYMBOLS_V2_VERSION_FOR_LAYOUT_AB.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for offset in offsets {
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        bytes.extend_from_slice(&strings);
        bytes
    }

    fn page_count(bytes: &[u8]) -> u32 {
        read_u32_at(bytes, 20)
    }

    fn descriptor_offset(page_index: usize) -> usize {
        SYMBOLS_V3_HEADER_LEN + page_index * SYMBOLS_V3_PAGE_DESCRIPTOR_LEN
    }

    fn page_offset(bytes: &[u8], page_index: usize) -> usize {
        read_u64_at(bytes, descriptor_offset(page_index) + 8) as usize
    }

    #[derive(Clone, Copy)]
    enum TestFieldValue {
        U16(u16),
        U32(u32),
        U64(u64),
    }

    impl TestFieldValue {
        fn write(self, bytes: &mut [u8], offset: usize) {
            match self {
                Self::U16(value) => put_u16(bytes, offset, value),
                Self::U32(value) => put_u32(bytes, offset, value),
                Self::U64(value) => put_u64(bytes, offset, value),
            }
        }
    }

    fn repair_root_crc_with_len(bytes: &mut [u8], root_len: usize) {
        put_u32(bytes, ROOT_CRC_OFFSET, 0);
        let root_crc = symbols_root_crc(&bytes[..root_len]);
        put_u32(bytes, ROOT_CRC_OFFSET, root_crc);
    }

    fn repair_root_crc(bytes: &mut [u8]) {
        let root_len = read_u64_at(bytes, 56) as usize;
        repair_root_crc_with_len(bytes, root_len);
    }

    fn repair_page_and_root_crcs(bytes: &mut [u8], page_index: usize) {
        let descriptor = descriptor_offset(page_index);
        let page_offset = read_u64_at(bytes, descriptor + 8) as usize;
        let page_len = read_u32_at(bytes, descriptor + 16) as usize;
        let page_crc = crc32c(&bytes[page_offset..page_offset + page_len]);
        put_u32(bytes, descriptor + 20, page_crc);
        repair_root_crc(bytes);
    }

    fn multi_page_values() -> Vec<String> {
        (0..5_000)
            .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
            .collect()
    }

    #[test]
    fn v3_roundtrips_sorted_values_and_lazy_lookups() {
        let values = vec![
            "".to_string(),
            "__name__".to_string(),
            "alpha".to_string(),
            "omega".to_string(),
        ];
        let bytes = encoded(&values);
        assert_eq!(read_u16_at(&bytes, 4), SYMBOLS_V3_VERSION);
        assert_eq!(
            read_symbols_bin_v3(Cursor::new(bytes.clone())).unwrap(),
            values
        );

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        assert_eq!(reader.len(), 4);
        assert_eq!(reader.lookup("alpha").unwrap(), Some(2));
        assert_eq!(reader.lookup("missing").unwrap(), None);
        assert_eq!(reader.resolve(3).unwrap().unwrap().as_str(), "omega");
        assert!(reader.resolve(4).unwrap().is_none());
        assert_eq!(
            reader.lookup_many(&["", "omega", "zeta"]).unwrap(),
            vec![Some(0), Some(3), None]
        );
    }

    #[test]
    fn lookup_many_groups_cross_page_duplicates_and_preserves_misses() {
        let values = multi_page_values();
        let bytes = encoded(&values);
        let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
        assert!(reader.state.root.descriptors.len() > 1);
        let second_page_id = reader.state.root.descriptors[1].first_symbol_id as usize;
        let in_page_miss = format!("{}!", values[0]);
        let queries = vec![
            values[second_page_id].clone(),
            values[0].clone(),
            values[second_page_id].clone(),
            in_page_miss,
            "!before-first".to_string(),
            "zzzz-after-last".to_string(),
            values[0].clone(),
        ];

        assert_eq!(
            reader.lookup_many(&queries).unwrap(),
            vec![
                Some(second_page_id as u32),
                Some(0),
                Some(second_page_id as u32),
                None,
                None,
                None,
                Some(0),
            ]
        );
        let stats = reader.read_stats();
        assert_eq!(stats.page.calls, 2);
        assert_eq!(stats.page_cache_misses, 2);
        assert_eq!(stats.page_cache_hits, 0);
        assert_eq!(stats.logical_returned.calls, 4);
        assert_eq!(
            stats.logical_returned.bytes,
            (2 * values[0].len() + 2 * values[second_page_id].len()) as u64
        );
    }

    #[test]
    fn resolve_many_groups_cross_page_duplicates_and_preserves_out_of_range_ids() {
        let values = multi_page_values();
        let bytes = encoded(&values);
        let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
        assert!(reader.state.root.descriptors.len() > 1);
        let second_page_id = reader.state.root.descriptors[1].first_symbol_id;
        let ids = [
            second_page_id,
            0,
            second_page_id,
            u32::MAX,
            0,
            reader.state.root.symbol_count,
        ];

        let resolved = reader.resolve_many(&ids).unwrap();
        assert_eq!(resolved.len(), ids.len());
        assert_eq!(
            resolved
                .iter()
                .map(|value| value.as_ref().map(|value| value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                Some(values[second_page_id as usize].as_str()),
                Some(values[0].as_str()),
                Some(values[second_page_id as usize].as_str()),
                None,
                Some(values[0].as_str()),
                None,
            ]
        );
        assert_eq!(resolved[0].as_ref().unwrap().symbol_id(), second_page_id);
        assert_eq!(resolved[1].as_ref().unwrap().symbol_id(), 0);
        assert_eq!(resolved[2].as_ref().unwrap().symbol_id(), second_page_id);
        assert_eq!(resolved[4].as_ref().unwrap().symbol_id(), 0);
        let stats = reader.read_stats();
        assert_eq!(stats.page.calls, 2);
        assert_eq!(stats.page_cache_misses, 2);
        assert_eq!(stats.page_cache_hits, 0);
        assert_eq!(stats.logical_returned.calls, 4);
        assert_eq!(
            stats.logical_returned.bytes,
            (2 * values[0].len() + 2 * values[second_page_id as usize].len()) as u64
        );
    }

    #[test]
    fn batched_page_load_order_and_sticky_corruption_match_scalar_requests() {
        let values = multi_page_values();
        let mut bytes = encoded(&values);
        assert!(page_count(&bytes) > 1);
        let second_page_id = read_u32_at(&bytes, descriptor_offset(1));
        let first_page_offset = page_offset(&bytes, 0);
        put_u32(&mut bytes, first_page_offset, 0);
        repair_page_and_root_crcs(&mut bytes, 0);
        let second_page_offset = page_offset(&bytes, 1);
        put_u32(&mut bytes, second_page_offset + 28, 1);
        repair_page_and_root_crcs(&mut bytes, 1);

        let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
        let error = reader
            .lookup_many(&[values[second_page_id as usize].as_str(), values[0].as_str()])
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page reserved field is non-zero");
        assert_eq!(reader.read_stats().page.calls, 1);
        assert_eq!(reader.read_stats().page_cache_misses, 1);

        let sticky = reader.resolve_many(&[u32::MAX]).unwrap_err();
        assert_eq!(sticky.to_string(), error.to_string());
        assert!(reader.lookup_many::<&str>(&[]).unwrap().is_empty());
        assert!(reader.resolve_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn visitor_propagates_touched_corruption_before_a_later_missing_id() {
        let values = multi_page_values();
        let mut bytes = encoded(&values);
        let first_page_offset = page_offset(&bytes, 0);
        put_u32(&mut bytes, first_page_offset, 0);
        repair_page_and_root_crcs(&mut bytes, 0);

        let reader =
            SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes.clone()), 0).unwrap();
        let error = reader
            .visit_resolved_many(&[0, u32::MAX], |_, _| Ok(()))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page magic mismatch");
        assert_eq!(reader.read_stats().page.calls, 1);
        let sticky = reader.resolve(u32::MAX).unwrap_err();
        assert_eq!(sticky.to_string(), error.to_string());

        let missing_first =
            SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
        let mut visits = 0;
        let all_resolved = missing_first
            .visit_resolved_many(&[u32::MAX, 0], |_, _| {
                visits += 1;
                Ok(())
            })
            .unwrap();
        assert!(!all_resolved);
        assert_eq!(visits, 0);
        assert_eq!(missing_first.read_stats().page.calls, 0);
    }

    #[test]
    fn empty_v3_root_is_deterministic_and_decodable() {
        let first = encoded(&[]);
        let second = encoded(&[]);
        assert_eq!(first, second);
        assert_eq!(first.len(), SYMBOLS_V3_HEADER_LEN);
        assert_eq!(read_u32_at(&first, 16), 0);
        assert_eq!(read_u32_at(&first, 20), 0);
        assert_eq!(read_u64_at(&first, 56), SYMBOLS_V3_HEADER_LEN as u64);
        assert_eq!(read_u64_at(&first, 64), SYMBOLS_V3_HEADER_LEN as u64);
        assert!(read_symbols_bin_v3(Cursor::new(first)).unwrap().is_empty());
    }

    #[test]
    fn singleton_v3_layout_and_checksums_match_the_golden_encoding() {
        let values = vec!["a".to_string()];
        let first = encoded(&values);
        let second = encoded(&values);
        assert_eq!(first, second);
        assert_eq!(first.len(), 171);

        assert_eq!(&first[0..4], b"SYMB");
        assert_eq!(read_u16_at(&first, 4), 3);
        assert_eq!(read_u16_at(&first, 6), 0);
        assert_eq!(read_u32_at(&first, 8), 80);
        assert_eq!(read_u32_at(&first, 12), 48);
        assert_eq!(read_u32_at(&first, 16), 1);
        assert_eq!(read_u32_at(&first, 20), 1);
        assert_eq!(read_u64_at(&first, 24), 80);
        assert_eq!(read_u64_at(&first, 32), 48);
        assert_eq!(read_u64_at(&first, 40), 128);
        assert_eq!(read_u64_at(&first, 48), 2);
        assert_eq!(read_u64_at(&first, 56), 130);
        assert_eq!(read_u64_at(&first, 64), 171);
        assert_eq!(read_u32_at(&first, 72), 0x04ca_4a2c);
        assert_eq!(read_u32_at(&first, 76), 0);

        let descriptor = descriptor_offset(0);
        assert_eq!(read_u32_at(&first, descriptor), 0);
        assert_eq!(read_u32_at(&first, descriptor + 4), 1);
        assert_eq!(read_u64_at(&first, descriptor + 8), 130);
        assert_eq!(read_u32_at(&first, descriptor + 16), 41);
        assert_eq!(read_u32_at(&first, descriptor + 20), 0xd58e_45db);
        assert_eq!(read_u32_at(&first, descriptor + 24), 0);
        assert_eq!(read_u32_at(&first, descriptor + 28), 1);
        assert_eq!(read_u32_at(&first, descriptor + 32), 1);
        assert_eq!(read_u32_at(&first, descriptor + 36), 1);
        assert_eq!(read_u32_at(&first, descriptor + 40), 1);
        assert_eq!(read_u32_at(&first, descriptor + 44), 0);
        assert_eq!(&first[128..130], b"aa");

        let page = 130;
        assert_eq!(&first[page..page + 4], b"SYPG");
        assert_eq!(read_u16_at(&first, page + 4), 1);
        assert_eq!(read_u16_at(&first, page + 6), 0);
        assert_eq!(read_u32_at(&first, page + 8), 0);
        assert_eq!(read_u32_at(&first, page + 12), 0);
        assert_eq!(read_u32_at(&first, page + 16), 1);
        assert_eq!(read_u32_at(&first, page + 20), 8);
        assert_eq!(read_u32_at(&first, page + 24), 1);
        assert_eq!(read_u32_at(&first, page + 28), 0);
        assert_eq!(read_u32_at(&first, page + 32), 0);
        assert_eq!(read_u32_at(&first, page + 36), 1);
        assert_eq!(&first[page + 40..], b"a");
        assert_eq!(read_symbols_bin_v3(Cursor::new(first)).unwrap(), values);
    }

    #[test]
    fn root_rejects_impossible_page_count_before_root_allocation() {
        let mut header = [0u8; SYMBOLS_V3_HEADER_LEN];
        put_u32(&mut header, 0, SYMBOLS_V3_MAGIC);
        put_u16(&mut header, 4, SYMBOLS_V3_VERSION);
        put_u32(&mut header, 8, SYMBOLS_V3_HEADER_LEN as u32);
        put_u32(&mut header, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
        put_u32(&mut header, 16, 1);
        put_u32(&mut header, 20, 2);

        let error = SegmentSymbolReader::open(HeaderOnlySource {
            header,
            file_len: SYMBOLS_V3_HEADER_LEN as u64,
        })
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page count exceeds symbol count");
    }

    #[test]
    fn root_rejects_operational_size_limit_before_reading_the_root() {
        let root_len = SYMBOLS_V3_MAX_ROOT_BYTES as u64 + 1;
        let mut header = [0u8; SYMBOLS_V3_HEADER_LEN];
        put_u32(&mut header, 0, SYMBOLS_V3_MAGIC);
        put_u16(&mut header, 4, SYMBOLS_V3_VERSION);
        put_u32(&mut header, 8, SYMBOLS_V3_HEADER_LEN as u32);
        put_u32(&mut header, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
        put_u32(&mut header, 16, 1);
        put_u32(&mut header, 20, 1);
        put_u64(&mut header, 24, SYMBOLS_V3_HEADER_LEN as u64);
        put_u64(&mut header, 32, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64);
        let fence_offset = (SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN) as u64;
        put_u64(&mut header, 40, fence_offset);
        put_u64(&mut header, 48, root_len - fence_offset);
        put_u64(&mut header, 56, root_len);
        put_u64(&mut header, 64, root_len);

        let error = SegmentSymbolReader::open(HeaderOnlySource {
            header,
            file_len: root_len,
        })
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "symbols root exceeds the operational size limit"
        );
    }

    #[test]
    fn root_rejects_page_size_limit_before_allocating_the_page() {
        let page_len = u32::try_from(SYMBOLS_V3_MAX_PAGE_BYTES + 1).unwrap();
        let pages_offset = (SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN) as u64;
        let file_len = pages_offset + u64::from(page_len);
        let mut root = vec![0u8; pages_offset as usize];
        put_u32(&mut root, 0, SYMBOLS_V3_MAGIC);
        put_u16(&mut root, 4, SYMBOLS_V3_VERSION);
        put_u32(&mut root, 8, SYMBOLS_V3_HEADER_LEN as u32);
        put_u32(&mut root, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
        put_u32(&mut root, 16, 1);
        put_u32(&mut root, 20, 1);
        put_u64(&mut root, 24, SYMBOLS_V3_HEADER_LEN as u64);
        put_u64(&mut root, 32, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64);
        put_u64(&mut root, 40, pages_offset);
        put_u64(&mut root, 48, 0);
        put_u64(&mut root, 56, pages_offset);
        put_u64(&mut root, 64, file_len);
        let descriptor = descriptor_offset(0);
        put_u32(&mut root, descriptor, 0);
        put_u32(&mut root, descriptor + 4, 1);
        put_u64(&mut root, descriptor + 8, pages_offset);
        put_u32(&mut root, descriptor + 16, page_len);
        put_u32(&mut root, descriptor + 20, 0);
        put_u32(&mut root, descriptor + 24, 0);
        put_u32(&mut root, descriptor + 28, 0);
        put_u32(&mut root, descriptor + 32, 0);
        put_u32(&mut root, descriptor + 36, 0);
        put_u32(&mut root, descriptor + 40, 0);
        put_u32(&mut root, descriptor + 44, 0);
        let root_crc = symbols_root_crc(&root);
        put_u32(&mut root, ROOT_CRC_OFFSET, root_crc);

        let error = SegmentSymbolReader::open(SparsePrefixSource {
            prefix: root,
            file_len,
        })
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "symbols page exceeds the operational size limit"
        );
    }

    #[test]
    fn touched_short_page_read_is_sticky_corruption_and_counted() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let bytes = encoded(&values);
        let pages_offset = read_u64_at(&bytes, 56) as usize;
        let reader = SegmentSymbolReader::open(SparsePrefixSource {
            prefix: bytes[..pages_offset].to_vec(),
            file_len: bytes.len() as u64,
        })
        .unwrap();

        let error = reader.resolve(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols positional read reached EOF");
        assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
        let sticky = reader.resolve(1).unwrap_err();
        assert_eq!(sticky.to_string(), error.to_string());
        assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
    }

    #[test]
    fn valid_crc_root_field_corruptions_are_rejected_by_field_validation() {
        struct Case {
            name: &'static str,
            offset: usize,
            value: TestFieldValue,
            expected: &'static str,
        }

        let pristine = encoded(&["a".to_string()]);
        let root_len = read_u64_at(&pristine, 56) as usize;
        let descriptor = descriptor_offset(0);
        let cases = [
            Case {
                name: "magic",
                offset: 0,
                value: TestFieldValue::U32(0),
                expected: "symbols magic mismatch",
            },
            Case {
                name: "flags",
                offset: 6,
                value: TestFieldValue::U16(1),
                expected: "symbols flags are non-zero",
            },
            Case {
                name: "header length",
                offset: 8,
                value: TestFieldValue::U32(79),
                expected: "symbols header length is invalid",
            },
            Case {
                name: "descriptor length",
                offset: 12,
                value: TestFieldValue::U32(47),
                expected: "symbols page descriptor length is invalid",
            },
            Case {
                name: "header symbol count",
                offset: 16,
                value: TestFieldValue::U32(2),
                expected: "symbols descriptor counts do not match the header",
            },
            Case {
                name: "directory offset",
                offset: 24,
                value: TestFieldValue::U64(81),
                expected: "symbols directory offset is invalid",
            },
            Case {
                name: "directory length",
                offset: 32,
                value: TestFieldValue::U64(49),
                expected: "symbols directory length is invalid",
            },
            Case {
                name: "fence offset",
                offset: 40,
                value: TestFieldValue::U64(129),
                expected: "symbols fence offset is invalid",
            },
            Case {
                name: "fence length",
                offset: 48,
                value: TestFieldValue::U64(3),
                expected: "symbols pages offset is invalid",
            },
            Case {
                name: "pages offset",
                offset: 56,
                value: TestFieldValue::U64(131),
                expected: "symbols pages offset is invalid",
            },
            Case {
                name: "file length",
                offset: 64,
                value: TestFieldValue::U64(170),
                expected: "symbols file length is invalid",
            },
            Case {
                name: "root reserved",
                offset: 76,
                value: TestFieldValue::U32(1),
                expected: "symbols reserved field is non-zero",
            },
            Case {
                name: "descriptor first id",
                offset: descriptor,
                value: TestFieldValue::U32(1),
                expected: "symbols page symbol ids are not contiguous",
            },
            Case {
                name: "descriptor count",
                offset: descriptor + 4,
                value: TestFieldValue::U32(0),
                expected: "symbols page descriptor has no symbols",
            },
            Case {
                name: "descriptor page offset",
                offset: descriptor + 8,
                value: TestFieldValue::U64(131),
                expected: "symbols page byte ranges are not contiguous",
            },
            Case {
                name: "descriptor page length",
                offset: descriptor + 16,
                value: TestFieldValue::U32(42),
                expected: "symbols page length is inconsistent",
            },
            Case {
                name: "descriptor reserved",
                offset: descriptor + 44,
                value: TestFieldValue::U32(1),
                expected: "symbols page descriptor reserved field is non-zero",
            },
        ];

        for case in cases {
            let mut bytes = pristine.clone();
            case.value.write(&mut bytes, case.offset);
            repair_root_crc_with_len(&mut bytes, root_len);
            assert_eq!(
                symbols_root_crc(&bytes[..root_len]),
                read_u32_at(&bytes, ROOT_CRC_OFFSET),
                "{} mutation did not retain a valid root CRC",
                case.name
            );

            let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidData, "{}", case.name);
            assert_eq!(error.to_string(), case.expected, "{}", case.name);
        }
    }

    #[test]
    fn writer_rejects_unsorted_or_duplicate_values() {
        let mut bytes = Vec::new();
        let error = write_symbols_bin_v3(&mut bytes, ["zeta", "alpha"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        let error = write_symbols_bin_v3(&mut bytes, ["alpha", "alpha"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn writer_rejects_page_and_root_operational_size_limits() {
        let mut page_output = Vec::new();
        let page_error = write_symbols_bin_v3_with_operational_limits(
            &mut page_output,
            ["a"],
            SymbolWriterOperationalLimits {
                max_page_bytes: SYMBOLS_V3_PAGE_HEADER_LEN + 2 * std::mem::size_of::<u32>(),
                max_root_bytes: SYMBOLS_V3_MAX_ROOT_BYTES,
            },
        )
        .unwrap_err();
        assert_eq!(page_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            page_error.to_string(),
            "symbols page exceeds the operational size limit"
        );
        assert!(page_output.is_empty());

        let mut root_output = Vec::new();
        let root_error = write_symbols_bin_v3_with_operational_limits(
            &mut root_output,
            ["a"],
            SymbolWriterOperationalLimits {
                max_page_bytes: SYMBOLS_V3_MAX_PAGE_BYTES,
                max_root_bytes: SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN + 1,
            },
        )
        .unwrap_err();
        assert_eq!(root_error.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            root_error.to_string(),
            "symbols root exceeds the operational size limit"
        );
        assert!(root_output.is_empty());
    }

    #[test]
    fn greedy_pages_are_deterministic_and_bounded() {
        let values = (0..5_000)
            .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
            .collect::<Vec<_>>();
        let bytes = encoded(&values);
        assert!(page_count(&bytes) > 1);
        for page_index in 0..page_count(&bytes) as usize {
            let descriptor = descriptor_offset(page_index);
            let count = read_u32_at(&bytes, descriptor + 4);
            let length = read_u32_at(&bytes, descriptor + 16) as usize;
            assert!(count == 1 || length <= SYMBOLS_V3_PAGE_TARGET_BYTES);
        }
        assert_eq!(read_symbols_bin_v3(Cursor::new(bytes)).unwrap(), values);
    }

    #[test]
    fn greedy_page_split_is_exact_at_the_target_and_one_byte_over() {
        let second_len_at_boundary = SYMBOLS_V3_PAGE_TARGET_BYTES
            - SYMBOLS_V3_PAGE_HEADER_LEN
            - 3 * std::mem::size_of::<u32>()
            - 1;
        let exact_values = vec!["a".to_string(), "b".repeat(second_len_at_boundary)];
        let exact_first = encoded(&exact_values);
        let exact_second = encoded(&exact_values);
        assert_eq!(exact_first, exact_second);
        assert_eq!(page_count(&exact_first), 1);
        assert_eq!(read_u32_at(&exact_first, descriptor_offset(0) + 4), 2);
        assert_eq!(
            read_u32_at(&exact_first, descriptor_offset(0) + 16) as usize,
            SYMBOLS_V3_PAGE_TARGET_BYTES
        );
        assert_eq!(
            read_symbols_bin_v3(Cursor::new(exact_first)).unwrap(),
            exact_values
        );

        let over_values = vec!["a".to_string(), "b".repeat(second_len_at_boundary + 1)];
        let over_first = encoded(&over_values);
        let over_second = encoded(&over_values);
        assert_eq!(over_first, over_second);
        assert_eq!(page_count(&over_first), 2);
        assert_eq!(read_u32_at(&over_first, descriptor_offset(0) + 4), 1);
        assert_eq!(read_u32_at(&over_first, descriptor_offset(1) + 4), 1);
        assert_eq!(read_u32_at(&over_first, descriptor_offset(0) + 16), 41);
        assert_eq!(
            read_u32_at(&over_first, descriptor_offset(1) + 16) as usize,
            SYMBOLS_V3_PAGE_TARGET_BYTES - 4
        );
        assert_eq!(
            read_symbols_bin_v3(Cursor::new(over_first)).unwrap(),
            over_values
        );
    }

    #[test]
    fn root_rejects_a_nonmaximal_page_with_a_valid_crc() {
        let values = multi_page_values();
        let mut bytes = encoded(&values);
        let page_count = page_count(&bytes) as usize;
        assert!(page_count > 1);
        let current = descriptor_offset(page_count - 2);
        let next = descriptor_offset(page_count - 1);
        let shifted_bytes = 1_024u32;
        let current_len = read_u32_at(&bytes, current + 16);
        let current_strings_len = read_u32_at(&bytes, current + 40);
        let next_offset = read_u64_at(&bytes, next + 8);
        let next_len = read_u32_at(&bytes, next + 16);
        let next_strings_len = read_u32_at(&bytes, next + 40);
        assert!(current_len > shifted_bytes);
        assert!(current_strings_len > shifted_bytes);
        assert!(next_len.saturating_add(shifted_bytes) <= SYMBOLS_V3_PAGE_TARGET_BYTES as u32);

        put_u32(&mut bytes, current + 16, current_len - shifted_bytes);
        put_u32(
            &mut bytes,
            current + 40,
            current_strings_len - shifted_bytes,
        );
        put_u64(&mut bytes, next + 8, next_offset - u64::from(shifted_bytes));
        put_u32(&mut bytes, next + 16, next_len + shifted_bytes);
        put_u32(&mut bytes, next + 40, next_strings_len + shifted_bytes);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
        let pages_offset = read_u64_at(&bytes, 56) as usize;
        let root_crc = symbols_root_crc(&bytes[..pages_offset]);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page is not greedily maximal");
    }

    #[test]
    fn oversized_symbol_uses_a_singleton_page() {
        let values = vec!["x".repeat(SYMBOLS_V3_PAGE_TARGET_BYTES + 100)];
        let bytes = encoded(&values);
        assert_eq!(page_count(&bytes), 1);
        let descriptor = descriptor_offset(0);
        assert_eq!(read_u32_at(&bytes, descriptor + 4), 1);
        assert!(read_u32_at(&bytes, descriptor + 16) as usize > SYMBOLS_V3_PAGE_TARGET_BYTES);
        assert_eq!(read_symbols_bin_v3(Cursor::new(bytes)).unwrap(), values);
    }

    #[test]
    fn v2_is_rejected_at_the_version_boundary() {
        let mut bytes = vec![0u8; SYMBOLS_V3_HEADER_LEN];
        put_u32(&mut bytes, 0, SYMBOLS_V3_MAGIC);
        put_u16(&mut bytes, 4, 2);
        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "unsupported symbols version");
    }

    #[test]
    fn explicit_layout_ab_v2_reader_is_eager_fallible_and_api_equivalent() {
        let values = vec![
            "".to_string(),
            "alpha".to_string(),
            "lambda".to_string(),
            "omega".to_string(),
            "ω".to_string(),
        ];
        let bytes = encoded_v2_for_layout_ab(&values);
        let encoded_len = bytes.len() as u64;
        let reader = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(bytes)).unwrap();

        assert_eq!(reader.len(), values.len());
        assert_eq!(reader.lookup("lambda").unwrap(), Some(2));
        assert_eq!(reader.lookup("missing").unwrap(), None);
        assert_eq!(
            reader
                .lookup_many(&["omega", "", "omega", "missing"])
                .unwrap(),
            vec![Some(3), Some(0), Some(3), None]
        );
        assert_eq!(reader.resolve(4).unwrap().unwrap().as_str(), "ω");
        assert!(reader.resolve(5).unwrap().is_none());
        let resolved = reader.resolve_many(&[3, 0, 3, 9]).unwrap();
        assert_eq!(
            resolved
                .iter()
                .map(|value| value.as_ref().map(|value| value.as_str()))
                .collect::<Vec<_>>(),
            vec![Some("omega"), Some(""), Some("omega"), None]
        );

        let stats = reader.read_stats();
        assert_eq!(
            stats.legacy_eager,
            SegmentSymbolReadCount {
                calls: 1,
                bytes: encoded_len,
            }
        );
        assert_eq!(stats.root, SegmentSymbolReadCount::default());
        assert_eq!(stats.page, SegmentSymbolReadCount::default());
        let resources = reader.resource_snapshot().unwrap();
        assert_eq!(resources.source_file_bytes, encoded_len);
        assert_eq!(resources.retained_open_files, 0);
        assert_eq!(resources.root_encoded_bytes, 0);
        assert!(resources.eager_dictionary_retained_charge_bytes > 0);
        assert_eq!(resources.page_cache_charge_bytes, 0);
        assert_eq!(resources.page_cache_max_bytes, 0);

        let clone = reader.try_clone_reader().unwrap();
        assert_eq!(clone.read_stats(), SegmentSymbolReadStats::default());
        assert_eq!(clone.lookup("alpha").unwrap(), Some(1));
        assert_eq!(
            clone.read_stats().logical_returned,
            SegmentSymbolReadCount { calls: 1, bytes: 5 }
        );
    }

    #[test]
    fn visitor_reports_equal_repeated_logical_work_for_v2_and_v3() {
        let values = vec![
            "".to_string(),
            "alpha".to_string(),
            "lambda".to_string(),
            "omega".to_string(),
        ];
        let ids = [3, 0, 3, 1, 0];

        let v3 = SegmentSymbolReader::open(Cursor::new(encoded(&values))).unwrap();
        let mut v3_values = vec![None; ids.len()];
        assert!(
            v3.visit_resolved_many(&ids, |slot, value| {
                v3_values[slot] = Some(value.to_string());
                Ok(())
            })
            .unwrap()
        );

        let v2 = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(
            encoded_v2_for_layout_ab(&values),
        ))
        .unwrap();
        let mut v2_values = vec![None; ids.len()];
        assert!(
            v2.visit_resolved_many(&ids, |slot, value| {
                v2_values[slot] = Some(value.to_string());
                Ok(())
            })
            .unwrap()
        );

        assert_eq!(v3_values, v2_values);
        assert_eq!(
            v3.read_stats().logical_returned,
            v2.read_stats().logical_returned
        );
        assert_eq!(
            v3.read_stats().logical_returned,
            SegmentSymbolReadCount {
                calls: 5,
                bytes: 15,
            }
        );
    }

    #[test]
    fn explicit_layout_ab_v2_reader_propagates_whole_dictionary_corruption() {
        let values = vec!["alpha".to_string(), "omega".to_string()];

        let mut nonzero_flags = encoded_v2_for_layout_ab(&values);
        put_u16(&mut nonzero_flags, 6, 1);
        let error = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(nonzero_flags))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);

        let mut bad_final_offset = encoded_v2_for_layout_ab(&values);
        put_u64(
            &mut bad_final_offset,
            SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB + 16,
            1,
        );
        let error =
            SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(bad_final_offset))
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);

        let duplicate_values = vec!["same".to_string(), "same".to_string()];
        let error = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(
            encoded_v2_for_layout_ab(&duplicate_values),
        ))
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);

        let mut invalid_utf8 = encoded_v2_for_layout_ab(&values);
        *invalid_utf8.last_mut().unwrap() = 0xff;
        let error = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(invalid_utf8))
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn root_crc_covers_descriptors_and_fences() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        bytes[descriptor_offset(0)] ^= 1;
        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.to_string(), "symbols root CRC mismatch");
    }

    #[test]
    fn root_rejects_invalid_utf8_fence_with_a_repaired_crc() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let descriptor = descriptor_offset(0);
        let fences_offset = read_u64_at(&bytes, 40) as usize;
        let first_fence_offset = read_u32_at(&bytes, descriptor + 24) as usize;
        bytes[fences_offset + first_fence_offset] = 0xff;
        repair_root_crc(&mut bytes);

        let root_len = read_u64_at(&bytes, 56) as usize;
        assert_eq!(
            symbols_root_crc(&bytes[..root_len]),
            read_u32_at(&bytes, ROOT_CRC_OFFSET)
        );
        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols fence is not valid UTF-8");
    }

    #[test]
    fn root_rejects_noncanonical_fence_aliasing_with_a_valid_crc() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let descriptor = descriptor_offset(0);
        put_u32(&mut bytes, descriptor + 32, 0);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
        let pages_offset = read_u64_at(&bytes, 56) as usize;
        let root_crc = symbols_root_crc(&bytes[..pages_offset]);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "symbols last fence is not canonically positioned"
        );
    }

    #[test]
    fn root_rejects_equal_fences_for_a_multi_symbol_page_with_a_valid_crc() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let descriptor = descriptor_offset(0);
        assert_eq!(read_u32_at(&bytes, descriptor + 4), 2);
        let fence_offset = read_u64_at(&bytes, 40) as usize;
        let first_offset = read_u32_at(&bytes, descriptor + 24) as usize;
        let first_len = read_u32_at(&bytes, descriptor + 28) as usize;
        let last_offset = read_u32_at(&bytes, descriptor + 32) as usize;
        let last_len = read_u32_at(&bytes, descriptor + 36) as usize;
        assert_eq!(first_len, last_len);
        let first =
            bytes[fence_offset + first_offset..fence_offset + first_offset + first_len].to_vec();
        bytes[fence_offset + last_offset..fence_offset + last_offset + last_len]
            .copy_from_slice(&first);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
        let pages_offset = read_u64_at(&bytes, 56) as usize;
        let root_crc = symbols_root_crc(&bytes[..pages_offset]);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "multi-symbol page fences are not strictly ordered"
        );
    }

    #[test]
    fn root_rejects_two_symbol_length_not_proven_by_fences() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let descriptor = descriptor_offset(0);
        assert_eq!(read_u32_at(&bytes, descriptor + 4), 2);
        let page_len = read_u32_at(&bytes, descriptor + 16);
        let string_bytes_len = read_u32_at(&bytes, descriptor + 40);
        let file_len = read_u64_at(&bytes, 64);
        put_u32(&mut bytes, descriptor + 16, page_len + 1);
        put_u32(&mut bytes, descriptor + 40, string_bytes_len + 1);
        put_u64(&mut bytes, 64, file_len + 1);
        bytes.push(0);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
        let pages_offset = read_u64_at(&bytes, 56) as usize;
        let root_crc = symbols_root_crc(&bytes[..pages_offset]);
        put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "two-symbol page string length disagrees with its fences"
        );
    }

    #[test]
    fn valid_crc_page_field_corruptions_are_rejected_when_touched() {
        struct Case {
            name: &'static str,
            relative_offset: usize,
            value: TestFieldValue,
            expected: &'static str,
        }

        let pristine = encoded(&["a".to_string(), "bb".to_string(), "ccc".to_string()]);
        let page = page_offset(&pristine, 0);
        let cases = [
            Case {
                name: "page magic",
                relative_offset: 0,
                value: TestFieldValue::U32(0),
                expected: "symbols page magic mismatch",
            },
            Case {
                name: "page version",
                relative_offset: 4,
                value: TestFieldValue::U16(2),
                expected: "symbols page version mismatch",
            },
            Case {
                name: "page flags",
                relative_offset: 6,
                value: TestFieldValue::U16(1),
                expected: "symbols page flags are non-zero",
            },
            Case {
                name: "page first id",
                relative_offset: 12,
                value: TestFieldValue::U32(1),
                expected: "symbols page first id mismatch",
            },
            Case {
                name: "page count",
                relative_offset: 16,
                value: TestFieldValue::U32(2),
                expected: "symbols page count mismatch",
            },
            Case {
                name: "page offsets length",
                relative_offset: 20,
                value: TestFieldValue::U32(12),
                expected: "symbols page offsets length mismatch",
            },
            Case {
                name: "page strings length",
                relative_offset: 24,
                value: TestFieldValue::U32(5),
                expected: "symbols page strings length mismatch",
            },
            Case {
                name: "first local offset",
                relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN,
                value: TestFieldValue::U32(1),
                expected: "symbols page first offset must be zero",
            },
            Case {
                name: "final local offset",
                relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 3 * 4,
                value: TestFieldValue::U32(5),
                expected: "symbols page final offset does not match strings",
            },
            Case {
                name: "out-of-order local offset",
                relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 4,
                value: TestFieldValue::U32(4),
                expected: "symbols page offsets are out of order",
            },
            Case {
                name: "out-of-bounds local offset",
                relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 4,
                value: TestFieldValue::U32(7),
                expected: "symbols page offset is out of bounds",
            },
        ];

        for case in cases {
            let mut bytes = pristine.clone();
            case.value.write(&mut bytes, page + case.relative_offset);
            repair_page_and_root_crcs(&mut bytes, 0);
            let descriptor = descriptor_offset(0);
            let page_len = read_u32_at(&bytes, descriptor + 16) as usize;
            assert_eq!(
                crc32c(&bytes[page..page + page_len]),
                read_u32_at(&bytes, descriptor + 20),
                "{} mutation did not retain a valid page CRC",
                case.name
            );
            let root_len = read_u64_at(&bytes, 56) as usize;
            assert_eq!(
                symbols_root_crc(&bytes[..root_len]),
                read_u32_at(&bytes, ROOT_CRC_OFFSET),
                "{} mutation did not retain a valid root CRC",
                case.name
            );

            let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
            let error = reader.resolve(0).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidData, "{}", case.name);
            assert_eq!(error.to_string(), case.expected, "{}", case.name);
        }
    }

    #[test]
    fn page_crc_is_checked_only_when_the_page_is_touched_and_is_sticky() {
        let values = (0..5_000)
            .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
            .collect::<Vec<_>>();
        let mut bytes = encoded(&values);
        assert!(page_count(&bytes) > 1);
        let corrupt_page = 1usize;
        let corrupt_offset = page_offset(&bytes, corrupt_page) + SYMBOLS_V3_PAGE_HEADER_LEN;
        bytes[corrupt_offset] ^= 1;

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        assert_eq!(reader.resolve(0).unwrap().unwrap().as_str(), values[0]);
        let corrupt_id = reader.state.root.descriptors[corrupt_page].first_symbol_id;
        let error = reader.resolve(corrupt_id).unwrap_err();
        assert_eq!(error.to_string(), "symbols page CRC mismatch");
        assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
        let sticky = reader.resolve(0).unwrap_err();
        assert_eq!(sticky.to_string(), "symbols page CRC mismatch");
        assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
    }

    #[test]
    fn touched_page_rejects_semantic_corruption_even_with_repaired_crcs() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let page = page_offset(&bytes, 0);
        let strings_len = read_u32_at(&bytes, descriptor_offset(0) + 40);
        put_u32(
            &mut bytes,
            page + SYMBOLS_V3_PAGE_HEADER_LEN + 4,
            strings_len,
        );
        repair_page_and_root_crcs(&mut bytes, 0);

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.resolve(0).unwrap_err();
        assert_eq!(
            error.to_string(),
            "symbols page values are not strictly sorted and unique"
        );
    }

    #[test]
    fn touched_page_rejects_reserved_bytes_even_with_repaired_crcs() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let page = page_offset(&bytes, 0);
        put_u32(&mut bytes, page + 28, 1);
        repair_page_and_root_crcs(&mut bytes, 0);

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.resolve(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page reserved field is non-zero");
    }

    #[test]
    fn touched_page_rejects_invalid_utf8_with_repaired_crcs() {
        let values = vec!["alpha".to_string(), "omega".to_string()];
        let mut bytes = encoded(&values);
        let descriptor = descriptor_offset(0);
        let page = page_offset(&bytes, 0);
        let offsets_len = read_u32_at(&bytes, page + 20) as usize;
        bytes[page + SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len] = 0xff;
        repair_page_and_root_crcs(&mut bytes, 0);

        let page_len = read_u32_at(&bytes, descriptor + 16) as usize;
        assert_eq!(
            crc32c(&bytes[page..page + page_len]),
            read_u32_at(&bytes, descriptor + 20)
        );
        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.resolve(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "symbols page value is not valid UTF-8");
    }

    #[test]
    fn page_identity_rejects_literal_swapped_pages_with_repaired_crcs() {
        let values = multi_page_values();
        let mut bytes = encoded(&values);
        assert!(page_count(&bytes) > 1);
        let first_offset = page_offset(&bytes, 0);
        let second_offset = page_offset(&bytes, 1);
        let first_len = read_u32_at(&bytes, descriptor_offset(0) + 16) as usize;
        let second_len = read_u32_at(&bytes, descriptor_offset(1) + 16) as usize;
        assert_eq!(first_len, second_len);
        let first_page = bytes[first_offset..first_offset + first_len].to_vec();
        let second_page = bytes[second_offset..second_offset + second_len].to_vec();
        bytes[first_offset..first_offset + first_len].copy_from_slice(&second_page);
        bytes[second_offset..second_offset + second_len].copy_from_slice(&first_page);
        repair_page_and_root_crcs(&mut bytes, 0);
        repair_page_and_root_crcs(&mut bytes, 1);

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.resolve(0).unwrap_err();
        assert_eq!(error.to_string(), "symbols page index mismatch");
    }

    #[test]
    fn cache_is_bounded_shared_by_clones_and_stats_are_per_reader() {
        let values = (0..5_000)
            .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
            .collect::<Vec<_>>();
        let bytes = encoded(&values);
        let reader = SegmentSymbolReader::open_with_cache_max_bytes(
            Cursor::new(bytes),
            SYMBOLS_V3_PAGE_TARGET_BYTES * 2,
        )
        .unwrap();
        let clone = reader.try_clone_reader().unwrap();
        let root_stats = reader.read_stats();
        assert_eq!(root_stats.root.calls, 2);
        assert_eq!(clone.read_stats(), SegmentSymbolReadStats::default());

        assert_eq!(reader.resolve(0).unwrap().unwrap().as_str(), values[0]);
        assert!(reader.cache_charge_bytes().unwrap() <= reader.cache_max_bytes());
        assert_eq!(reader.read_stats().page_validation.calls, 1);
        assert_eq!(
            reader.read_stats().page_validation.bytes,
            reader.read_stats().page.bytes
        );
        assert_eq!(clone.resolve(0).unwrap().unwrap().as_str(), values[0]);
        assert_eq!(reader.read_stats().page_cache_misses, 1);
        assert_eq!(clone.read_stats().page_cache_hits, 1);
        assert_eq!(reader.read_stats().logical_returned.calls, 1);
        assert_eq!(
            reader.read_stats().logical_returned.bytes,
            values[0].len() as u64
        );
        assert_eq!(clone.read_stats().logical_returned.calls, 1);
        assert_eq!(
            clone.read_stats().logical_returned.bytes,
            values[0].len() as u64
        );
    }

    #[test]
    fn resource_snapshot_charges_the_root_once_per_shared_state() {
        let bytes = encoded(&multi_page_values());
        let expected_file_bytes = bytes.len() as u64;
        let expected_root_bytes = read_u64_at(&bytes, 56);
        let expected_root_charge = std::mem::size_of::<SymbolRoot>()
            + page_count(&bytes) as usize * std::mem::size_of::<SymbolPageDescriptor>()
            + read_u64_at(&bytes, 48) as usize;
        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let clone = reader.try_clone_reader().unwrap();

        assert_eq!(reader.state_identity(), clone.state_identity());
        let before = reader.resource_snapshot().unwrap();
        assert_eq!(before.retained_open_files, 0);
        assert_eq!(before.source_file_bytes, expected_file_bytes);
        assert_eq!(before.root_encoded_bytes, expected_root_bytes);
        assert_eq!(
            before.root_retained_charge_bytes,
            expected_root_charge as u64
        );
        assert_eq!(before.eager_dictionary_retained_charge_bytes, 0);
        assert_eq!(before.page_cache_charge_bytes, 0);
        assert_eq!(before.page_cache_max_bytes, 256 * 1024);
        assert_eq!(
            before.total_retained_charge_bytes(),
            before.root_retained_charge_bytes
        );

        clone.resolve(0).unwrap().unwrap();
        let after = reader.resource_snapshot().unwrap();
        assert!(after.page_cache_charge_bytes > 0);
        assert_eq!(after, clone.resource_snapshot().unwrap());
        assert_eq!(
            after.total_retained_charge_bytes(),
            after
                .root_retained_charge_bytes
                .saturating_add(after.page_cache_charge_bytes)
        );
    }

    #[test]
    fn validate_all_detects_an_otherwise_untouched_bad_page() {
        let values = (0..5_000)
            .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
            .collect::<Vec<_>>();
        let mut bytes = encoded(&values);
        let last_page = page_count(&bytes) as usize - 1;
        let corrupt_offset = page_offset(&bytes, last_page) + SYMBOLS_V3_PAGE_HEADER_LEN;
        bytes[corrupt_offset] ^= 1;
        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.validate_all().unwrap_err();
        assert_eq!(error.to_string(), "symbols page CRC mismatch");
    }
}
