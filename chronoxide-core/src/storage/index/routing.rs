use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentRoutingIndex {
    labels: BTreeMap<String, BTreeMap<String, ExactPostingsMetadata>>,
}

impl SegmentRoutingIndex {
    /// Builds one routing entry for every exact-postings key.
    ///
    /// Missing source metadata is an inconsistent index build, never an
    /// instruction to omit the key: omission could make early pruning return
    /// a false negative.
    pub fn from_indexes(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<Self> {
        Self::from_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V8Raw,
        )
    }

    pub fn from_indexes_adaptive(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<Self> {
        Self::from_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V9Adaptive,
        )
    }

    fn from_indexes_with_length_format(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
        format: ExactPostingsLengthFormat,
    ) -> io::Result<Self> {
        let mut index = Self::default();
        for (name_sym, value_sym, refs) in postings.entries() {
            let (name, value, metadata) =
                routing_entry_from_indexes(symbols, ranges, name_sym, value_sym, refs, format)?;
            let previous = index
                .labels
                .entry(name.to_string())
                .or_default()
                .insert(value.to_string(), metadata);
            if previous.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing source indexes contain a duplicate logical label key",
                ));
            }
        }
        Ok(index)
    }

    pub(in crate::storage::index) fn validate_against_indexes(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<()> {
        self.validate_against_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V8Raw,
        )
    }

    pub(in crate::storage::index) fn validate_against_indexes_adaptive(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<()> {
        self.validate_against_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V9Adaptive,
        )
    }

    fn validate_against_indexes_with_length_format(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
        format: ExactPostingsLengthFormat,
    ) -> io::Result<()> {
        if self.len() != postings.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index entry count does not match exact postings",
            ));
        }
        for (name_sym, value_sym, refs) in postings.entries() {
            let (name, value, expected) =
                routing_entry_from_indexes(symbols, ranges, name_sym, value_sym, refs, format)?;
            if self.exact_postings_metadata(name, value) != Some(expected) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing index metadata does not match exact postings and time ranges",
                ));
            }
        }
        Ok(())
    }

    pub fn exact_postings_metadata(
        &self,
        name: &str,
        value: &str,
    ) -> Option<ExactPostingsMetadata> {
        self.labels
            .get(name)
            .and_then(|values| values.get(value))
            .copied()
    }

    pub(in crate::storage::index) fn encode(&self) -> io::Result<Vec<u8>> {
        let mut entries = Vec::new();
        for (name, values) in &self.labels {
            for (value, metadata) in values {
                entries.push((routing_key_bytes(name, value)?, *metadata));
            }
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let bucket_count = routing_bucket_count(entries.len())?;
        let buckets_offset = ROUTING_INDEX_HEADER_LEN as u64;
        let key_bytes_offset = buckets_offset
            .checked_add(
                u64::try_from(bucket_count)
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "routing bucket count exceeds u64",
                        )
                    })?
                    .checked_mul(ROUTING_INDEX_BUCKET_LEN as u64)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "routing index too large")
                    })?,
            )
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "routing index too large")
            })?;

        let mut buckets = vec![RoutingBucketRecord::default(); bucket_count];
        let mut key_bytes = Vec::new();
        for (key, metadata) in entries {
            let hash = routing_key_hash(&key);
            let mut bucket = (hash as usize) & (bucket_count - 1);
            loop {
                if buckets[bucket].is_empty() {
                    let key_offset = u32_len(key_bytes.len(), "routing key bytes offset")?;
                    let key_len = u32_len(key.len(), "routing key length")?;
                    key_bytes.extend_from_slice(&key);
                    buckets[bucket] = RoutingBucketRecord {
                        hash,
                        key_offset,
                        key_len,
                        metadata,
                    };
                    break;
                }
                bucket = (bucket + 1) & (bucket_count - 1);
            }
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ROUTING_INDEX_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&ROUTING_INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&u32_len(self.len(), "routing entry count")?.to_le_bytes());
        bytes.extend_from_slice(&u32_len(bucket_count, "routing bucket count")?.to_le_bytes());
        bytes.extend_from_slice(&buckets_offset.to_le_bytes());
        bytes.extend_from_slice(&key_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(
            &u64::try_from(key_bytes.len())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "routing key bytes length exceeds u64",
                    )
                })?
                .to_le_bytes(),
        );
        for bucket in buckets {
            bucket.encode(&mut bytes);
        }
        bytes.extend_from_slice(&key_bytes);
        Ok(bytes)
    }

    pub(in crate::storage::index) fn decode(bytes: &[u8]) -> io::Result<Self> {
        let header = RoutingIndexHeader::decode(bytes, bytes.len() as u64)?;
        let mut labels = BTreeMap::new();
        let mut decoded_entries = 0u32;
        for bucket_index in 0..header.bucket_count {
            let offset = header.bucket_offset(bucket_index)?;
            let bucket = RoutingBucketRecord::decode(read_bytes_at(
                bytes,
                offset,
                ROUTING_INDEX_BUCKET_LEN,
            )?)?;
            let Some(key_range) = bucket.validate_touched(header)? else {
                continue;
            };
            let key = read_bytes_at(bytes, key_range.offset, key_range.len)?;
            let (name, value) = validate_routing_bucket_key(bucket, key)?;
            if labels
                .get(name)
                .is_some_and(|values: &BTreeMap<String, ExactPostingsMetadata>| {
                    values.contains_key(value)
                })
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing index contains a duplicate logical key",
                ));
            }
            labels
                .entry(name.to_owned())
                .or_insert_with(BTreeMap::new)
                .insert(value.to_owned(), bucket.metadata);
            decoded_entries = decoded_entries.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing entry count overflow")
            })?;
        }
        if decoded_entries != header.entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index entry count mismatch",
            ));
        }
        Ok(Self { labels })
    }

    pub(in crate::storage::index) fn len(&self) -> usize {
        self.labels.values().map(BTreeMap::len).sum()
    }
}

fn routing_entry_from_indexes<'a>(
    symbols: &'a SegmentSymbols,
    ranges: &LabelValueTimeRangeIndex,
    name_sym: u32,
    value_sym: u32,
    refs: &[u32],
    format: ExactPostingsLengthFormat,
) -> io::Result<(&'a str, &'a str, ExactPostingsMetadata)> {
    let name = symbols.resolve(name_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing label-name symbol {name_sym} cannot be resolved"),
        )
    })?;
    let value = symbols.resolve(value_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing label-value symbol {value_sym} cannot be resolved"),
        )
    })?;
    let range = ranges.get(name_sym, value_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing source ({name_sym}, {value_sym}) has no label-value time range"),
        )
    })?;
    if range.min_time_ms > range.max_time_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing source ({name_sym}, {value_sym}) has a reversed time range"),
        ));
    }
    Ok((
        name,
        value,
        ExactPostingsMetadata {
            byte_len: match format {
                ExactPostingsLengthFormat::V8Raw => exact_postings_blob_len(refs)?,
                ExactPostingsLengthFormat::V9Adaptive => adaptive_exact_postings_blob_len(refs)?,
            },
            time_range: range,
        },
    ))
}

#[derive(Debug, Clone, Copy)]
enum ExactPostingsLengthFormat {
    V8Raw,
    V9Adaptive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::index) struct RoutingIndexHeader {
    pub(in crate::storage::index) entry_count: u32,
    pub(in crate::storage::index) bucket_count: u32,
    pub(in crate::storage::index) buckets_offset: u64,
    pub(in crate::storage::index) key_bytes_offset: u64,
    pub(in crate::storage::index) key_bytes_len: u64,
}

impl RoutingIndexHeader {
    pub(in crate::storage::index) fn decode(bytes: &[u8], blob_len: u64) -> io::Result<Self> {
        if bytes.len() < ROUTING_INDEX_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing index header truncated",
            ));
        }
        let mut cursor = 0usize;
        let magic = read_u32(bytes, &mut cursor)?;
        if magic != ROUTING_INDEX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index magic mismatch",
            ));
        }
        let version = read_u16(bytes, &mut cursor)?;
        if version != ROUTING_INDEX_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported routing index version",
            ));
        }
        let flags = read_u16(bytes, &mut cursor)?;
        if flags != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index flags are non-zero",
            ));
        }
        let entry_count = read_u32(bytes, &mut cursor)?;
        let bucket_count = read_u32(bytes, &mut cursor)?;
        let buckets_offset = read_u64(bytes, &mut cursor)?;
        let key_bytes_offset = read_u64(bytes, &mut cursor)?;
        let key_bytes_len = read_u64(bytes, &mut cursor)?;
        let header = Self {
            entry_count,
            bucket_count,
            buckets_offset,
            key_bytes_offset,
            key_bytes_len,
        };
        header.validate(blob_len)?;
        Ok(header)
    }

    fn validate(self, blob_len: u64) -> io::Result<()> {
        if self.bucket_count == 0 || !self.bucket_count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index bucket count must be a non-zero power of two",
            ));
        }
        if self.buckets_offset < ROUTING_INDEX_HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index bucket offset overlaps header",
            ));
        }
        let bucket_bytes = u64::from(self.bucket_count)
            .checked_mul(ROUTING_INDEX_BUCKET_LEN as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket table too large")
            })?;
        let buckets_end = self
            .buckets_offset
            .checked_add(bucket_bytes)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket table too large")
            })?;
        if buckets_end > blob_len || buckets_end > self.key_bytes_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket table is out of bounds",
            ));
        }
        let key_bytes_end = self
            .key_bytes_offset
            .checked_add(self.key_bytes_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing key bytes too large")
            })?;
        if key_bytes_end > blob_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing key bytes are out of bounds",
            ));
        }
        if u64::from(self.entry_count) >= u64::from(self.bucket_count) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index load factor is invalid",
            ));
        }
        Ok(())
    }

    pub(in crate::storage::index) fn bucket_offset(self, bucket_index: u32) -> io::Result<u64> {
        if bucket_index >= self.bucket_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "routing bucket index out of bounds",
            ));
        }
        self.buckets_offset
            .checked_add(u64::from(bucket_index) * ROUTING_INDEX_BUCKET_LEN as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket offset overflow")
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::index) struct RoutingBucketRecord {
    pub(in crate::storage::index) hash: u64,
    pub(in crate::storage::index) key_offset: u32,
    pub(in crate::storage::index) key_len: u32,
    pub(in crate::storage::index) metadata: ExactPostingsMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::index) struct RoutingBucketKeyRange {
    pub(in crate::storage::index) offset: u64,
    pub(in crate::storage::index) len: usize,
}

impl Default for RoutingBucketRecord {
    fn default() -> Self {
        Self {
            hash: 0,
            key_offset: 0,
            key_len: 0,
            metadata: ExactPostingsMetadata {
                byte_len: 0,
                time_range: LabelValueTimeRange {
                    min_time_ms: 0,
                    max_time_ms: 0,
                },
            },
        }
    }
}

impl RoutingBucketRecord {
    pub(in crate::storage::index) fn is_empty(self) -> bool {
        self.key_len == 0
    }

    fn encode(self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.hash.to_le_bytes());
        bytes.extend_from_slice(&self.key_offset.to_le_bytes());
        bytes.extend_from_slice(&self.key_len.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.time_range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.time_range.max_time_ms.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.byte_len.to_le_bytes());
    }

    pub(in crate::storage::index) fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != ROUTING_INDEX_BUCKET_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing bucket record truncated",
            ));
        }
        let mut cursor = 0usize;
        let hash = read_u64(bytes, &mut cursor)?;
        let key_offset = read_u32(bytes, &mut cursor)?;
        let key_len = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        let byte_len = read_u64(bytes, &mut cursor)?;
        Ok(Self {
            hash,
            key_offset,
            key_len,
            metadata: ExactPostingsMetadata {
                byte_len,
                time_range: LabelValueTimeRange {
                    min_time_ms,
                    max_time_ms,
                },
            },
        })
    }

    pub(in crate::storage::index) fn validate_touched(
        self,
        header: RoutingIndexHeader,
    ) -> io::Result<Option<RoutingBucketKeyRange>> {
        if self.key_len == 0 {
            if self.hash != 0
                || self.key_offset != 0
                || self.metadata.byte_len != 0
                || self.metadata.time_range.min_time_ms != 0
                || self.metadata.time_range.max_time_ms != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing empty bucket is not canonical",
                ));
            }
            return Ok(None);
        }
        if self.metadata.byte_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket postings byte length is zero",
            ));
        }
        if self.metadata.time_range.min_time_ms > self.metadata.time_range.max_time_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket time range is reversed",
            ));
        }
        let relative_offset = u64::from(self.key_offset);
        let relative_end = relative_offset
            .checked_add(u64::from(self.key_len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing bucket key range overflow",
                )
            })?;
        if relative_end > header.key_bytes_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket key range exceeds declared key bytes",
            ));
        }
        let offset = header
            .key_bytes_offset
            .checked_add(relative_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing bucket key offset overflow",
                )
            })?;
        let len = usize::try_from(self.key_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket key length exceeds platform usize",
            )
        })?;
        Ok(Some(RoutingBucketKeyRange { offset, len }))
    }
}

pub(in crate::storage::index) fn validate_routing_bucket_key(
    bucket: RoutingBucketRecord,
    key: &[u8],
) -> io::Result<(&str, &str)> {
    let parts = routing_key_parts(key)?;
    if routing_key_hash(key) != bucket.hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "routing bucket hash does not match its stored key",
        ));
    }
    Ok(parts)
}

fn exact_postings_blob_len(refs: &[u32]) -> io::Result<u64> {
    let refs_len = u64::try_from(refs.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "postings list length exceeds u64",
        )
    })?;
    refs_len
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(4))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large"))
}

fn adaptive_exact_postings_blob_len(refs: &[u32]) -> io::Result<u64> {
    let raw_len = exact_postings_blob_len(refs)?;
    let first = refs.first().copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "adaptive postings list is empty",
        )
    })?;
    let mut delta_len = 4u64
        .checked_add(uleb128_u32_len(first) as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large"))?;
    let mut previous = first;
    for &series_ref in &refs[1..] {
        let gap = series_ref
            .checked_sub(previous)
            .filter(|gap| *gap != 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "adaptive postings refs are not strictly ordered and unique",
                )
            })?;
        delta_len = delta_len
            .checked_add(uleb128_u32_len(gap) as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large")
            })?;
        previous = series_ref;
    }
    Ok(raw_len.min(delta_len))
}

const fn uleb128_u32_len(mut value: u32) -> usize {
    let mut len = 1usize;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn u32_len(len: usize, description: &'static str) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} exceeds u32"),
        )
    })
}

fn routing_bucket_count(entry_count: usize) -> io::Result<usize> {
    let min_buckets = entry_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "routing index too large"))?
        .max(2);
    let bucket_count = min_buckets.checked_next_power_of_two().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "routing bucket count too large",
        )
    })?;
    u32_len(bucket_count, "routing bucket count")?;
    Ok(bucket_count)
}

pub(in crate::storage::index) fn routing_key_bytes(
    label_name: &str,
    label_value: &str,
) -> io::Result<Vec<u8>> {
    let capacity = 4usize
        .checked_add(label_name.len())
        .and_then(|len| len.checked_add(label_value.len()))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "routing key length overflows")
        })?;
    u32_len(capacity, "routing key length")?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("routing key allocation failed: {error}"),
        )
    })?;
    bytes.extend_from_slice(&u32_len(label_name.len(), "routing label name length")?.to_le_bytes());
    bytes.extend_from_slice(label_name.as_bytes());
    bytes.extend_from_slice(label_value.as_bytes());
    Ok(bytes)
}

pub(in crate::storage::index) fn routing_key_parts(bytes: &[u8]) -> io::Result<(&str, &str)> {
    let mut cursor = 0usize;
    let name_len = read_u32(bytes, &mut cursor)? as usize;
    let name = read_bytes(bytes, &mut cursor, name_len)?;
    let value_len = bytes.len().saturating_sub(cursor);
    let value = read_bytes(bytes, &mut cursor, value_len)?;
    let name = std::str::from_utf8(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "routing label name is not valid utf-8",
        )
    })?;
    let value = std::str::from_utf8(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "routing label value is not valid utf-8",
        )
    })?;
    Ok((name, value))
}

pub(in crate::storage::index) fn routing_key_hash(bytes: &[u8]) -> u64 {
    routing_hash_parts(std::iter::once(bytes))
}

pub(in crate::storage::index) fn routing_key_hash_parts(
    label_name: &str,
    label_value: &str,
) -> io::Result<u64> {
    let name_len = u32_len(label_name.len(), "routing label name length")?;
    let key_len = 4usize
        .checked_add(label_name.len())
        .and_then(|len| len.checked_add(label_value.len()))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "routing key length overflows")
        })?;
    u32_len(key_len, "routing key length")?;
    let encoded_name_len = name_len.to_le_bytes();
    Ok(routing_hash_parts([
        encoded_name_len.as_slice(),
        label_name.as_bytes(),
        label_value.as_bytes(),
    ]))
}

fn routing_hash_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;

    let mut hash = FNV_OFFSET_BASIS;
    for part in parts {
        for byte in part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}
