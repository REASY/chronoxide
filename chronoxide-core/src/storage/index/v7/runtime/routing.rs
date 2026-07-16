use crate::storage::index::{
    ExactPostingsMetadata, ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN,
    RoutingBucketKeyRange, RoutingBucketRecord, RoutingIndexHeader, routing_key_hash_parts,
    routing_key_parts, validate_routing_bucket_key,
};

use super::*;

#[derive(Debug)]
struct ValidatedRoutingHeader {
    root: Schema6IndexRootV7,
    routing_offset: u64,
    routing_len: u64,
    value: RoutingIndexHeader,
}

impl ValidatedRoutingHeader {
    fn charged_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

#[derive(Debug)]
struct ValidatedRoutingBucket {
    root: Schema6IndexRootV7,
    header: RoutingIndexHeader,
    bucket_index: u32,
    value: RoutingBucketRecord,
    key_range: Option<RoutingBucketKeyRange>,
}

impl ValidatedRoutingBucket {
    fn charged_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

#[derive(Debug)]
struct ValidatedRoutingKey {
    root: Schema6IndexRootV7,
    relative_offset: u64,
    bytes: Box<[u8]>,
}

impl ValidatedRoutingKey {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(u64::try_from(self.bytes.len()).map_err(|_| {
                invalid_data("governed routing key length exceeds the governor counter")
            })?)
            .ok_or_else(|| invalid_data("governed routing key charge overflows"))
    }
}

/// Decoded outcome of one routing point lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Schema6RoutingLookupResult {
    IndexAbsent,
    Missing,
    Match(ExactPostingsMetadata),
}

/// Query-local routing result bound to one segment generation and validated
/// index root. It is intentionally neither `Copy` nor `Clone`.
#[derive(Debug)]
pub(crate) struct GovernedSchema6RoutingLookup {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    root_pin: MetadataCachePin<Schema6IndexRootV7>,
    value: Schema6RoutingLookupResult,
}

impl GovernedSchema6RoutingLookup {
    #[cfg(test)]
    pub(super) fn substitute_root_for_test(&mut self) {
        self.root.layout.routing.len ^= 1;
    }
}

impl GovernedSchema6IndexSession {
    /// Looks up one normalized label-name/value pair through the optional
    /// routing hash table. Every occupied bucket in the collision chain has
    /// its exact key bytes validated before this method returns a match or a
    /// clean miss.
    pub(crate) fn routing_exact_postings_metadata(
        &self,
        root: &GovernedSchema6IndexRoot,
        label_name: &str,
        label_value: &str,
    ) -> Result<GovernedSchema6RoutingLookup, Schema6IndexReaderError> {
        self.ensure_provenance(&root.provenance)?;
        let root_context = *root.value;
        let locator = root_context.layout.routing;
        if locator == super::super::BlobLocator::default() {
            return Ok(self.bind_routing_lookup(root, Schema6RoutingLookupResult::IndexAbsent));
        }

        let lookup_hash =
            routing_key_hash_parts(label_name, label_value).map_err(MetadataCacheError::from_io)?;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let header =
            self.load_routing_header(root_context, locator.offset, locator.len, &reader)?;
        let mut bucket_index = (lookup_hash as u32) & (header.value.bucket_count - 1);

        for _ in 0..header.value.bucket_count {
            let bucket = self.load_routing_bucket(
                root_context,
                locator.offset,
                header.value,
                bucket_index,
                &reader,
            )?;
            let Some(key_range) = bucket.key_range else {
                return Ok(self.bind_routing_lookup(root, Schema6RoutingLookupResult::Missing));
            };
            let stored_key =
                self.load_routing_key(root_context, locator.offset, key_range, &reader)?;
            let stored_parts = validate_routing_bucket_key(bucket.value, &stored_key.bytes);
            let (stored_name, stored_value) = match stored_parts {
                Ok(parts) => parts,
                Err(error) => {
                    drop(stored_key);
                    drop(bucket);
                    drop(header);
                    return Err(reader.record_validation_error(error).into());
                }
            };
            if bucket.value.hash == lookup_hash
                && stored_name == label_name
                && stored_value == label_value
            {
                return Ok(self.bind_routing_lookup(
                    root,
                    Schema6RoutingLookupResult::Match(bucket.value.metadata),
                ));
            }
            bucket_index = (bucket_index + 1) & (header.value.bucket_count - 1);
        }

        drop(header);
        Err(reader
            .record_validation_error(invalid_data(
                "routing index probe exhausted without empty bucket",
            ))
            .into())
    }

    /// Exposes a routing outcome only while its generation and root pin still
    /// agree with the session's immutable index artifact.
    pub(crate) fn routing_lookup_result(
        &self,
        lookup: &GovernedSchema6RoutingLookup,
    ) -> Result<Schema6RoutingLookupResult, Schema6IndexReaderError> {
        self.ensure_provenance(&lookup.provenance)?;
        if *lookup.root_pin != lookup.root {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(lookup.value)
    }

    fn bind_routing_lookup(
        &self,
        root: &GovernedSchema6IndexRoot,
        value: Schema6RoutingLookupResult,
    ) -> GovernedSchema6RoutingLookup {
        GovernedSchema6RoutingLookup {
            provenance: self.guard.provenance(),
            root: *root.value,
            root_pin: root.value.clone(),
            value,
        }
    }

    fn load_routing_header(
        &self,
        root: Schema6IndexRootV7,
        routing_offset: u64,
        routing_len: u64,
        reader: &GovernedArtifactReader,
    ) -> Result<MetadataCachePin<ValidatedRoutingHeader>, Schema6IndexReaderError> {
        let key = metadata_key(
            reader,
            routing_offset,
            ROUTING_INDEX_HEADER_LEN as u64,
            MetadataCacheClass::IndexDirectory,
        )?;
        let declared = std::mem::size_of::<ValidatedRoutingHeader>() as u64;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let header = RoutingIndexHeader::decode(bytes, routing_len)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedRoutingHeader {
                root,
                routing_offset,
                routing_len,
                value: header,
            };
            let charged = value.charged_bytes();
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root
            || value.routing_offset != routing_offset
            || value.routing_len != routing_len
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(value)
    }

    fn load_routing_bucket(
        &self,
        root: Schema6IndexRootV7,
        routing_offset: u64,
        header: RoutingIndexHeader,
        bucket_index: u32,
        reader: &GovernedArtifactReader,
    ) -> Result<MetadataCachePin<ValidatedRoutingBucket>, Schema6IndexReaderError> {
        let relative_offset = header.bucket_offset(bucket_index).map_err(|error| {
            reader.record_validation_error(invalid_data(format!(
                "routing bucket offset is invalid: {error}"
            )))
        })?;
        let offset = routing_offset.checked_add(relative_offset).ok_or_else(|| {
            reader.record_validation_error(invalid_data("routing bucket file offset overflows"))
        })?;
        let key = metadata_key(
            reader,
            offset,
            ROUTING_INDEX_BUCKET_LEN as u64,
            MetadataCacheClass::IndexPage,
        )?;
        let declared = std::mem::size_of::<ValidatedRoutingBucket>() as u64;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let bucket = RoutingBucketRecord::decode(bytes).map_err(MetadataCacheError::from_io)?;
            let key_range = bucket
                .validate_touched(header)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedRoutingBucket {
                root,
                header,
                bucket_index,
                value: bucket,
                key_range,
            };
            let charged = value.charged_bytes();
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root || value.header != header || value.bucket_index != bucket_index {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(value)
    }

    fn load_routing_key(
        &self,
        root: Schema6IndexRootV7,
        routing_offset: u64,
        key_range: RoutingBucketKeyRange,
        reader: &GovernedArtifactReader,
    ) -> Result<MetadataCachePin<ValidatedRoutingKey>, Schema6IndexReaderError> {
        let length = u64::try_from(key_range.len).map_err(|_| {
            reader.record_validation_error(invalid_data(
                "routing key length exceeds the governor counter",
            ))
        })?;
        key_range.offset.checked_add(length).ok_or_else(|| {
            reader.record_validation_error(invalid_data("routing key range overflows"))
        })?;
        let offset = routing_offset
            .checked_add(key_range.offset)
            .ok_or_else(|| {
                reader.record_validation_error(invalid_data("routing key file offset overflows"))
            })?;
        let key = metadata_key(reader, offset, length, MetadataCacheClass::IndexPage)?;
        let declared = (std::mem::size_of::<ValidatedRoutingKey>() as u64)
            .checked_add(length)
            .ok_or_else(|| {
                reader.record_validation_error(invalid_data(
                    "governed routing key declared charge overflows",
                ))
            })?;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            let bytes = bytes.into_boxed_slice();
            routing_key_parts(&bytes).map_err(MetadataCacheError::from_io)?;
            let value = ValidatedRoutingKey {
                root,
                relative_offset: key_range.offset,
                bytes,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root
            || value.relative_offset != key_range.offset
            || u64::try_from(value.bytes.len()).ok() != Some(length)
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(value)
    }
}
