pub(crate) mod alp;
pub(crate) mod alp_spiral;
pub(crate) mod bitstream;
pub(crate) mod chimp;
pub(crate) mod elf;
pub(crate) mod gorilla;
pub(crate) mod schema_varlen;
pub(crate) mod varint;
pub(crate) mod varlen;
pub(crate) mod zigzag;

use std::io;

pub(crate) use alp::{AlpEncoder, AlpRdEncoder, decode_alp_rd_values, decode_alp_values};
pub(crate) use alp_spiral::{
    AlpRdSpiralEncoder, AlpSpiralEncoder, decode_alp_rd_spiral_values, decode_alp_spiral_values,
};
pub(crate) use elf::{ElfEncoder, decode_elf_values};
pub(crate) use gorilla::{GorillaEncoder, decode_gorilla_values, encode_gorilla_values};
pub(crate) use schema_varlen::{SchemaVarLenCodec, SchemaVarLenEncoding};
pub use varint::varint_len;
pub(crate) use varint::{decode_varint, encode_varint};
pub(crate) use varlen::{VarLenCodec, VarLenEncoding};
pub use zigzag::{decode_zigzag_i64, encode_zigzag_i64};

pub(crate) fn encode_float_values_with<E>(
    values: &[f64],
    new_encoder: impl FnOnce() -> E,
    mut push: impl FnMut(&mut E, f64) -> io::Result<()>,
    finish: impl FnOnce(E) -> Vec<u8>,
) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "values must be non-empty",
        ));
    }

    let mut encoder = new_encoder();
    for value in values {
        push(&mut encoder, *value)?;
    }
    Ok(finish(encoder))
}
