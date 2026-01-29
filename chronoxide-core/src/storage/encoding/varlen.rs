use std::io;
use std::marker::PhantomData;

use crate::storage::encoding::{decode_varint, encode_varint};

pub(crate) trait VarLenEncoding: Sized {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()>;
    fn decode_from(buf: &[u8]) -> io::Result<Self>;
}

#[derive(Debug)]
pub(crate) struct VarLenCodec<T: VarLenEncoding> {
    values: Vec<u8>,
    scratch: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T: VarLenEncoding> VarLenCodec<T> {
    fn encode_value(&mut self, value: &T) -> io::Result<()> {
        self.scratch.clear();
        value.encode_into(&mut self.scratch)?;
        let len = self.scratch.len();
        encode_varint(len as u64, &mut self.values);
        self.values.extend_from_slice(&self.scratch);
        Ok(())
    }

    pub(crate) fn new(first: T) -> io::Result<Self> {
        let mut codec = Self {
            values: Vec::new(),
            scratch: Vec::new(),
            _marker: PhantomData,
        };
        codec.encode_value(&first)?;
        Ok(codec)
    }

    pub(crate) fn push(&mut self, value: T) -> io::Result<()> {
        self.encode_value(&value)
    }

    pub(crate) fn encoded_len_bytes(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.clone()
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.values
    }

    pub(crate) fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<T>> {
        let mut values = Vec::with_capacity(count);
        let mut cursor = 0usize;
        for _ in 0..count {
            let len = decode_varint(buf, &mut cursor)?;
            let len_usize = usize::try_from(len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "value length overflow"))?;
            if cursor.saturating_add(len_usize) > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "value buffer truncated",
                ));
            }
            let value_buf = &buf[cursor..cursor + len_usize];
            cursor = cursor.saturating_add(len_usize);
            values.push(T::decode_from(value_buf)?);
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

    #[derive(Debug, PartialEq)]
    struct TestValue(u32);

    impl VarLenEncoding for TestValue {
        fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
            encode_varint(self.0 as u64, out);
            Ok(())
        }

        fn decode_from(buf: &[u8]) -> io::Result<Self> {
            let mut cursor = 0usize;
            let value = decode_varint(buf, &mut cursor)?;
            if cursor != buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "test value trailing bytes",
                ));
            }
            Ok(Self(value as u32))
        }
    }

    #[test]
    fn var_len_codec_roundtrip() {
        let mut codec = VarLenCodec::<TestValue>::new(TestValue(5)).unwrap();
        codec.push(TestValue(300)).unwrap();
        let bytes = codec.snapshot_bytes();
        let decoded = VarLenCodec::<TestValue>::decode_values(&bytes, 2).unwrap();
        assert_eq!(decoded, vec![TestValue(5), TestValue(300)]);
    }
}
