use std::collections::HashMap;
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
    last_schema_id: Option<u32>,
    values: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T: SchemaVarLenEncoding> SchemaVarLenCodec<T> {
    fn lookup_schema_id(&mut self) -> io::Result<u32> {
        if let Some(id) = self.schema_index.get(&self.schema_scratch) {
            return Ok(*id);
        }

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
        self.schemas.push(schema);
        self.schema_bytes.push(self.schema_scratch.clone());
        self.schema_index
            .insert(self.schema_scratch.clone(), schema_id);
        Ok(schema_id)
    }

    fn encode_value(&mut self, value: &T) -> io::Result<()> {
        self.schema_scratch.clear();
        value.encode_schema_from_value(&mut self.schema_scratch)?;

        let schema_id = match self.last_schema_id {
            Some(last_id) => {
                let idx = last_id as usize;
                if idx < self.schema_bytes.len() && self.schema_bytes[idx] == self.schema_scratch {
                    last_id
                } else {
                    self.lookup_schema_id()?
                }
            }
            None => self.lookup_schema_id()?,
        };

        self.last_schema_id = Some(schema_id);
        encode_varint(schema_id as u64, &mut self.values);
        value.encode_value_with_schema(&self.schemas[schema_id as usize], &mut self.values)?;
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

        let mut schemas = Vec::with_capacity(schema_count);
        for _ in 0..schema_count {
            let len = decode_varint(buf, &mut cursor)?;
            let len_usize = usize::try_from(len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "schema length overflow")
            })?;
            if cursor.saturating_add(len_usize) > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "schema buffer truncated",
                ));
            }
            let schema_buf = &buf[cursor..cursor + len_usize];
            cursor = cursor.saturating_add(len_usize);
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

        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let schema_id = decode_varint(buf, &mut cursor)?;
            let schema_idx = usize::try_from(schema_id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema id overflow"))?;
            let schema = schemas.get(schema_idx).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "schema id out of range")
            })?;
            values.push(T::decode_value_with_schema(schema, buf, &mut cursor)?);
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
}
