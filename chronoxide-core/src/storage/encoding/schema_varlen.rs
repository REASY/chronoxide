use std::collections::{HashMap, HashSet};
use std::io;
use std::marker::PhantomData;

use crate::storage::encoding::{decode_varint, encode_varint, varint_len};

pub(crate) trait SchemaVarLenEncoding: Sized {
    type Schema: Clone;

    fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()>;
    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema>;
    fn encode_value_with_schema(&self, schema: &Self::Schema, out: &mut Vec<u8>) -> io::Result<()>;
    fn decode_value_with_schema(
        schema: &Self::Schema,
        buf: &[u8],
        cursor: &mut usize,
    ) -> io::Result<Self>;
}

#[derive(Debug)]
pub(crate) struct SchemaVarLenCodec<T: SchemaVarLenEncoding> {
    schema_bytes: Vec<Vec<u8>>,
    schemas: Vec<T::Schema>,
    schema_index: HashMap<Vec<u8>, u32>,
    schema_scratch: Vec<u8>,
    value_scratch: Vec<u8>,
    last_schema_id: Option<u32>,
    values: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T: SchemaVarLenEncoding> SchemaVarLenCodec<T> {
    fn encode_value(&mut self, value: &T) -> io::Result<()> {
        self.schema_scratch.clear();
        value.encode_schema_from_value(&mut self.schema_scratch)?;

        let existing_schema_id = match self.last_schema_id {
            Some(last_id) => {
                let idx = last_id as usize;
                if idx < self.schema_bytes.len() && self.schema_bytes[idx] == self.schema_scratch {
                    Some(last_id)
                } else {
                    self.schema_index.get(&self.schema_scratch).copied()
                }
            }
            None => self.schema_index.get(&self.schema_scratch).copied(),
        };

        let mut pending_schema = None;
        let schema_id = if let Some(schema_id) = existing_schema_id {
            schema_id
        } else {
            let schema_id = u32::try_from(self.schemas.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "schema id overflow"))?;
            let mut cursor = 0usize;
            let schema = T::decode_schema(&self.schema_scratch, &mut cursor)?;
            if cursor != self.schema_scratch.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "schema buffer has trailing bytes",
                ));
            }
            pending_schema = Some(schema);
            schema_id
        };

        self.value_scratch.clear();
        let schema = match pending_schema.as_ref() {
            Some(schema) => schema,
            None => self.schemas.get(schema_id as usize).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "schema id out of range")
            })?,
        };
        value.encode_value_with_schema(schema, &mut self.value_scratch)?;

        let value_append_len = varint_len(u64::from(schema_id))
            .checked_add(self.value_scratch.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "encoded value length overflows",
                )
            })?;
        self.values.try_reserve(value_append_len).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("encoded value allocation failed: {error}"),
            )
        })?;

        if let Some(schema) = pending_schema {
            self.schemas.try_reserve(1).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("schema value allocation failed: {error}"),
                )
            })?;
            self.schema_bytes.try_reserve(1).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("schema byte table allocation failed: {error}"),
                )
            })?;
            self.schema_index.try_reserve(1).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("schema index allocation failed: {error}"),
                )
            })?;
            let table_schema_bytes = try_clone_bytes(&self.schema_scratch, "schema byte table")?;
            let index_schema_bytes = try_clone_bytes(&self.schema_scratch, "schema index key")?;
            self.schemas.push(schema);
            self.schema_bytes.push(table_schema_bytes);
            self.schema_index.insert(index_schema_bytes, schema_id);
        }
        self.last_schema_id = Some(schema_id);
        encode_varint(schema_id as u64, &mut self.values);
        self.values.extend_from_slice(&self.value_scratch);
        Ok(())
    }

    fn write_schema_header(schema_bytes: &[Vec<u8>], out: &mut Vec<u8>) {
        encode_varint(schema_bytes.len() as u64, out);
        for schema in schema_bytes {
            encode_varint(schema.len() as u64, out);
            out.extend_from_slice(schema);
        }
    }

    fn schema_header_len(schema_bytes: &[Vec<u8>]) -> usize {
        let mut len = varint_len(schema_bytes.len() as u64);
        for schema in schema_bytes {
            len = len
                .saturating_add(varint_len(schema.len() as u64))
                .saturating_add(schema.len());
        }
        len
    }

    pub(crate) fn new(first: T) -> io::Result<Self> {
        let mut codec = Self {
            schema_bytes: Vec::new(),
            schemas: Vec::new(),
            schema_index: HashMap::new(),
            schema_scratch: Vec::new(),
            value_scratch: Vec::new(),
            last_schema_id: None,
            values: Vec::new(),
            _marker: PhantomData,
        };
        codec.encode_value(&first)?;
        Ok(codec)
    }

    pub(crate) fn push(&mut self, value: T) -> io::Result<()> {
        self.encode_value(&value)
    }

    pub(crate) fn encoded_len_bytes(&self) -> usize {
        Self::schema_header_len(&self.schema_bytes).saturating_add(self.values.len())
    }

    pub(crate) fn snapshot_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        Self::write_schema_header(&self.schema_bytes, &mut out);
        out.extend_from_slice(&self.values);
        out
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        Self::write_schema_header(&self.schema_bytes, &mut out);
        let mut values = self.values;
        out.append(&mut values);
        out
    }

    pub(crate) fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<T>> {
        let mut cursor = 0usize;
        let schema_count = decode_varint(buf, &mut cursor)?;
        let schema_count = usize::try_from(schema_count)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema count overflow"))?;
        if (count == 0 && schema_count != 0) || (count != 0 && !(1..=count).contains(&schema_count))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema count is inconsistent with the value count",
            ));
        }
        ensure_minimum_encoded_items(
            buf.len().saturating_sub(cursor),
            schema_count,
            1,
            "schema headers",
        )?;

        let mut schemas = try_vec_with_capacity(schema_count, "decoded schemas")?;
        let mut encoded_schemas = HashSet::new();
        encoded_schemas.try_reserve(schema_count).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("encoded schema set allocation failed: {error}"),
            )
        })?;
        for _ in 0..schema_count {
            let len = decode_varint(buf, &mut cursor)?;
            let len_usize = usize::try_from(len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "schema length overflow")
            })?;
            let schema_end = cursor.checked_add(len_usize).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "schema range overflows")
            })?;
            if schema_end > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "schema buffer truncated",
                ));
            }
            let schema_buf = &buf[cursor..schema_end];
            cursor = schema_end;
            if !encoded_schemas.insert(schema_buf) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate schema definition is noncanonical",
                ));
            }
            let mut schema_cursor = 0usize;
            let schema = T::decode_schema(schema_buf, &mut schema_cursor)?;
            if schema_cursor != schema_buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "schema buffer has trailing bytes",
                ));
            }
            schemas.push(schema);
        }

        ensure_minimum_encoded_items(
            buf.len().saturating_sub(cursor),
            count,
            1,
            "schema-tagged values",
        )?;
        let mut values = try_vec_with_capacity(count, "decoded schema-tagged values")?;
        let mut next_first_seen_schema = 0usize;
        for _ in 0..count {
            let schema_id = decode_varint(buf, &mut cursor)?;
            let schema_idx = usize::try_from(schema_id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema id overflow"))?;
            let schema = schemas.get(schema_idx).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "schema id out of range")
            })?;
            if schema_idx > next_first_seen_schema {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "schema IDs are not in deterministic first-seen order",
                ));
            }
            if schema_idx == next_first_seen_schema {
                next_first_seen_schema =
                    next_first_seen_schema.checked_add(1).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "schema first-use count overflows",
                        )
                    })?;
            }
            values.push(T::decode_value_with_schema(schema, buf, &mut cursor)?);
        }

        if next_first_seen_schema != schema_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema table contains an unused schema",
            ));
        }

        if cursor != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "value buffer has trailing bytes",
            ));
        }
        Ok(values)
    }
}

fn try_clone_bytes(bytes: &[u8], field: &'static str) -> io::Result<Vec<u8>> {
    let mut cloned = Vec::new();
    cloned.try_reserve_exact(bytes.len()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{field} allocation failed: {error}"),
        )
    })?;
    cloned.extend_from_slice(bytes);
    Ok(cloned)
}

fn ensure_minimum_encoded_items(
    available_bytes: usize,
    item_count: usize,
    minimum_bytes_per_item: usize,
    field: &'static str,
) -> io::Result<()> {
    let minimum_bytes = item_count
        .checked_mul(minimum_bytes_per_item)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{field} minimum encoded size overflows"),
            )
        })?;
    if minimum_bytes > available_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} count is infeasible for the remaining encoded bytes"),
        ));
    }
    Ok(())
}

fn try_vec_with_capacity<T>(count: usize, field: &'static str) -> io::Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{field} allocation failed: {error}"),
        )
    })?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::encoding::{decode_varint, encode_varint};

    #[derive(Debug, Clone, PartialEq)]
    struct TestSchema {
        tag: u8,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TestValue {
        tag: u8,
        value: u32,
    }

    impl SchemaVarLenEncoding for TestValue {
        type Schema = TestSchema;

        fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
            encode_varint(self.tag as u64, out);
            Ok(())
        }

        fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
            let tag = decode_varint(buf, cursor)?;
            Ok(Self::Schema { tag: tag as u8 })
        }

        fn encode_value_with_schema(
            &self,
            _schema: &Self::Schema,
            out: &mut Vec<u8>,
        ) -> io::Result<()> {
            encode_varint(self.value as u64, out);
            Ok(())
        }

        fn decode_value_with_schema(
            schema: &Self::Schema,
            buf: &[u8],
            cursor: &mut usize,
        ) -> io::Result<Self> {
            let value = decode_varint(buf, cursor)?;
            Ok(Self {
                tag: schema.tag,
                value: value as u32,
            })
        }
    }

    #[test]
    fn schema_varlen_codec_roundtrip() {
        let mut codec = SchemaVarLenCodec::new(TestValue { tag: 1, value: 10 }).unwrap();
        codec.push(TestValue { tag: 1, value: 20 }).unwrap();
        codec.push(TestValue { tag: 2, value: 30 }).unwrap();

        let bytes = codec.snapshot_bytes();
        let decoded: Vec<TestValue> = SchemaVarLenCodec::decode_values(&bytes, 3).unwrap();
        assert_eq!(
            decoded,
            vec![
                TestValue { tag: 1, value: 10 },
                TestValue { tag: 1, value: 20 },
                TestValue { tag: 2, value: 30 },
            ]
        );
    }

    #[test]
    fn schema_varlen_rejects_infeasible_counts_before_allocation() {
        let mut excessive_schemas = Vec::new();
        encode_varint(u64::from(u32::MAX), &mut excessive_schemas);
        let error =
            SchemaVarLenCodec::<TestValue>::decode_values(&excessive_schemas, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("schema count is inconsistent"));

        let mut excessive_values = Vec::new();
        encode_varint(1, &mut excessive_values);
        encode_varint(1, &mut excessive_values);
        encode_varint(7, &mut excessive_values);
        let error =
            SchemaVarLenCodec::<TestValue>::decode_values(&excessive_values, u32::MAX as usize)
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("schema-tagged values count is infeasible")
        );
    }

    fn encoded_test_values(schemas: &[u8], values: &[(u8, u32)]) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode_varint(schemas.len() as u64, &mut bytes);
        for schema in schemas {
            encode_varint(1, &mut bytes);
            encode_varint(u64::from(*schema), &mut bytes);
        }
        for (schema_id, value) in values {
            encode_varint(u64::from(*schema_id), &mut bytes);
            encode_varint(u64::from(*value), &mut bytes);
        }
        bytes
    }

    #[test]
    fn schema_varlen_rejects_noncanonical_schema_tables_and_first_use() {
        let duplicate = encoded_test_values(&[7, 7], &[(0, 10), (1, 20)]);
        let error = SchemaVarLenCodec::<TestValue>::decode_values(&duplicate, 2).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("duplicate schema"));

        let skipped = encoded_test_values(&[7, 8], &[(1, 10), (0, 20)]);
        let error = SchemaVarLenCodec::<TestValue>::decode_values(&skipped, 2).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("first-seen order"));

        let unused = encoded_test_values(&[7, 8], &[(0, 10), (0, 20)]);
        let error = SchemaVarLenCodec::<TestValue>::decode_values(&unused, 2).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("unused schema"));
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FallibleTestValue {
        tag: u8,
        value: u32,
        fail_value: bool,
    }

    impl SchemaVarLenEncoding for FallibleTestValue {
        type Schema = TestSchema;

        fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
            encode_varint(self.tag as u64, out);
            Ok(())
        }

        fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
            let tag = decode_varint(buf, cursor)?;
            Ok(TestSchema { tag: tag as u8 })
        }

        fn encode_value_with_schema(
            &self,
            _schema: &Self::Schema,
            out: &mut Vec<u8>,
        ) -> io::Result<()> {
            encode_varint(self.value as u64, out);
            if self.fail_value {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "injected value failure",
                ))
            } else {
                Ok(())
            }
        }

        fn decode_value_with_schema(
            schema: &Self::Schema,
            buf: &[u8],
            cursor: &mut usize,
        ) -> io::Result<Self> {
            Ok(Self {
                tag: schema.tag,
                value: decode_varint(buf, cursor)? as u32,
                fail_value: false,
            })
        }
    }

    #[test]
    fn failed_push_is_transactional_for_schema_and_value_bytes() {
        let first = FallibleTestValue {
            tag: 1,
            value: 10,
            fail_value: false,
        };
        let mut codec = SchemaVarLenCodec::new(first.clone()).unwrap();
        let before = codec.snapshot_bytes();

        let error = codec
            .push(FallibleTestValue {
                tag: 2,
                value: 20,
                fail_value: true,
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(codec.snapshot_bytes(), before);

        let second = FallibleTestValue {
            tag: 2,
            value: 30,
            fail_value: false,
        };
        codec.push(second.clone()).unwrap();
        let decoded =
            SchemaVarLenCodec::<FallibleTestValue>::decode_values(&codec.snapshot_bytes(), 2)
                .unwrap();
        assert_eq!(decoded, vec![first, second]);
    }
}
