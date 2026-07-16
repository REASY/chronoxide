#!/usr/bin/env python3
"""Read-only size models for compact schema-7 series/chunk-index layouts.

The model does not claim to encode schema 7. It inventories the current
``series.bin`` v2 and ``chunk_index.bin`` v1 bytes, checks whether every chunk
can be represented by the proposed 24-byte inline descriptor, and applies
explicit byte-count assumptions. It reports both the conservative 56-byte
screening model from the storage-layout review and the selected 40-byte paged
model, including its roots, descriptors, fixed-page padding, and overflow.
"""

from __future__ import annotations

import argparse
import json
import stat
import struct
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO


STANDARD_ARTIFACTS = (
    "meta.json",
    "symbols.bin",
    "series.bin",
    "chunks.bin",
    "ooo_chunks.bin",
    "chunk_index.bin",
    "indexes.puffin",
    "footer.bin",
)

METADATA_ARTIFACTS = (
    "meta.json",
    "symbols.bin",
    "series.bin",
    "chunk_index.bin",
    "indexes.puffin",
    "footer.bin",
)

SERIES_MAGIC = int.from_bytes(b"SERI", "little")
SERIES_VERSION = 2
SERIES_HEADER = struct.Struct("<IHHIIIIQQQQQ")
SERIES_HEADER_BYTES = SERIES_HEADER.size
SERIES_ROW = struct.Struct("<QBBHQIIIII")
SERIES_ROW_BYTES = SERIES_ROW.size

CHUNK_INDEX_MAGIC = int.from_bytes(b"CHIX", "little")
CHUNK_INDEX_VERSION = 1
CHUNK_INDEX_HEADER = struct.Struct("<IHHI")
CHUNK_INDEX_HEADER_BYTES = CHUNK_INDEX_HEADER.size
CHUNK_INDEX_DIRECTORY_ENTRY_BYTES = 8
CHUNK_ENTRY = struct.Struct("<BBHQQQIII")
CHUNK_ENTRY_BYTES = CHUNK_ENTRY.size
CHUNK_HEADER_BYTES = 40

INDEX_MAGIC = int.from_bytes(b"SIDX", "little")
INDEX_TRAILER_MAGIC = int.from_bytes(b"SIDT", "little")
INDEX_V7_TERMINAL_MAGIC = int.from_bytes(b"S7ND", "little")
INDEX_V7_VERSION = 7
INDEX_HEADER_BYTES = 16
INDEX_TRAILER_BYTES = 256
INDEX_HEADER = struct.Struct("<IHHII")
INDEX_V7_EXACT_RECORD_BYTES = 40
INDEX_V8_EXACT_RECORD_BYTES = 48
INDEX_EXACT_PAGE_BYTES = 16_384
INDEX_EXACT_PAGE_DESCRIPTOR_BYTES = 32
INDEX_EXACT_DIRECTORY_HEADER_BYTES = 64
INDEX_V7_EXACT_RECORDS_PER_PAGE = 409
INDEX_V8_EXACT_RECORDS_PER_PAGE = 341
INDEX_V7_AUXILIARY_RECORD_BYTES = 40
INDEX_V8_AUXILIARY_RECORD_BYTES = 48
INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES = 64
INDEX_EXACT_DIRECTORY_MAGIC = int.from_bytes(b"EXD7", "little")
INDEX_EXACT_DIRECTORY_VERSION = 1
INDEX_AUXILIARY_DIRECTORY_MAGIC = int.from_bytes(b"AUX7", "little")
INDEX_AUXILIARY_DIRECTORY_VERSION = 1
INDEX_TRAILER_CRC_OFFSET = 160
INDEX_TRAILER_TERMINAL_MAGIC_OFFSET = 252

INDEX_LOCATORS = (
    ("routing", 24, False),
    ("metric ranges", 40, True),
    ("exact directory", 56, True),
    ("exact pages", 72, False),
    ("exact postings", 88, False),
    ("auxiliary directory", 104, True),
    ("auxiliary payloads", 120, False),
)
INDEX_PHYSICAL_REGION_ORDER = (
    "routing",
    "metric ranges",
    "exact postings",
    "auxiliary payloads",
    "exact directory",
    "exact pages",
    "auxiliary directory",
)

CONSERVATIVE_SERIES_RECORD_BYTES = 56
CONSERVATIVE_INLINE_DESCRIPTOR_BYTES = 24
CONSERVATIVE_OVERFLOW_DESCRIPTOR_BYTES = 40

PAGED_ROOT_BYTES = 176
PAGED_DESCRIPTOR_BYTES = 16
PAGED_PAGE_BYTES = 16_384
PAGED_PAGE_HEADER_BYTES = 24
PAGED_RECORD_BYTES = 40
PAGED_RECORDS_PER_PAGE = 409
PAGED_COLD_PAGE_BYTES = 16_384
PAGED_OVERFLOW_ROOT_BYTES = 64
PAGED_OVERFLOW_ENTRY_BYTES = 44
PAGED_SCALAR_LANE_LEN_BITS = 21
PAGED_SCALAR_LANE_LEN_MAX = (1 << PAGED_SCALAR_LANE_LEN_BITS) - 1
PAGED_HOT_OFFSET_ALIGNMENT = 4_096
U32_MAX = (1 << 32) - 1
KNOWN_KIND_MASK = 0b0001_1111
KNOWN_CHUNK_KINDS = frozenset(range(5))
ROWS_PER_BLOCK = 131_072

assert PAGED_PAGE_HEADER_BYTES + PAGED_RECORDS_PER_PAGE * PAGED_RECORD_BYTES == PAGED_PAGE_BYTES

# This is a screening gate, not an adoption gate. Actual encoded bytes and
# semantic/performance A/B evidence remain required before format adoption.
MIN_CORPUS_SAVINGS_BASIS_POINTS = 500
MIN_SERIES_INDEX_SAVINGS_BASIS_POINTS = 2_000
MAX_OVERFLOW_BASIS_POINTS = 100


class ModelError(ValueError):
    pass


@dataclass(frozen=True)
class SeriesHeader:
    num_series: int
    num_keysets: int
    num_value_dicts: int
    table_offset: int
    keysets_offset: int
    value_dicts_offset: int
    keyset_blocks_offset: int
    meta_offset: int


@dataclass
class SegmentResult:
    series_count: int = 0
    chunk_count: int = 0
    zero_chunk_series: int = 0
    one_chunk_series: int = 0
    multi_chunk_series: int = 0
    max_chunks_per_series: int = 0
    conservative_inline_eligible_series: int = 0
    conservative_overflow_series: int = 0
    conservative_overflow_chunks: int = 0
    paged_inline_eligible_series: int = 0
    paged_overflow_series: int = 0
    paged_overflow_chunks: int = 0
    series_header_bytes: int = 0
    series_table_bytes: int = 0
    keysets_bytes: int = 0
    value_dicts_bytes: int = 0
    keyset_blocks_bytes: int = 0
    series_metadata_bytes: int = 0
    chunk_index_header_bytes: int = 0
    chunk_index_directory_bytes: int = 0
    chunk_index_entry_bytes: int = 0
    series_metadata_entries: int = 0
    series_metadata_payload_bytes: int = 0
    relative_time_u32_failures: int = 0
    outside_segment_time_failures: int = 0
    chunk_offset_u32_failures: int = 0
    chunk_length_min_failures: int = 0
    scalar_lane_shape_failures: int = 0
    scalar_lane_len_21bit_failures: int = 0
    file_id_bit_failures: int = 0
    series_kind_mismatches: int = 0
    max_min_time_delta_ms: int = 0
    max_max_time_delta_ms: int = 0
    max_chunk_offset: int = 0
    max_chunk_length: int = 0
    max_scalar_lane_length: int = 0
    paged_hot_page_count: int = 0
    paged_cold_page_count: int = 0
    paged_cold_final_page_bytes: int = 0
    paged_root_bytes: int = 0
    paged_descriptor_bytes: int = 0
    paged_page_bytes: int = 0
    paged_hot_offset_alignment_bytes: int = 0
    paged_page_padding_bytes: int = 0
    paged_overflow_root_bytes: int = 0
    paged_overflow_blob_header_bytes: int = 0
    paged_overflow_entry_bytes: int = 0
    index_exact_entry_count: int = 0
    index_v7_exact_page_count: int = 0
    index_v8_exact_page_count: int = 0
    index_auxiliary_entry_count: int = 0


@dataclass(frozen=True)
class BlobLocator:
    offset: int
    length: int

    @property
    def end(self) -> int:
        return self.offset + self.length

    @property
    def present(self) -> bool:
        return self.offset != 0


@dataclass(frozen=True)
class IndexV7Shape:
    exact_entry_count: int
    exact_page_count: int
    v8_exact_page_count: int
    auxiliary_entry_count: int


def _error(path: Path, message: str) -> ModelError:
    return ModelError(f"{path}: {message}")


def _regular_file_size(path: Path) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise _error(path, "file does not exist") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise _error(path, "expected a regular file (symbolic links are rejected)")
    return metadata.st_size


def _read_exact(source: BinaryIO, length: int, path: Path, what: str) -> bytes:
    value = source.read(length)
    if len(value) != length:
        raise _error(path, f"truncated {what}: expected {length} bytes, got {len(value)}")
    return value


def _read_exact_at(
    source: BinaryIO, offset: int, length: int, path: Path, what: str
) -> bytes:
    source.seek(offset)
    return _read_exact(source, length, path, what)


def _crc32c(value: bytes | bytearray) -> int:
    """Return the standard reflected Castagnoli CRC-32C."""

    crc = 0xFFFF_FFFF
    for byte in value:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F6_3B78 if crc & 1 else 0)
    return crc ^ 0xFFFF_FFFF


def _crc32c_with_zeroed_u32(value: bytes, offset: int) -> int:
    if offset < 0 or offset + 4 > len(value):
        raise ValueError("CRC field lies outside the value")
    scratch = bytearray(value)
    scratch[offset : offset + 4] = b"\0" * 4
    return _crc32c(scratch)


def _align_up(value: int, alignment: int) -> int:
    return ((value + alignment - 1) // alignment) * alignment


def _page_count(entry_count: int, records_per_page: int) -> int:
    if entry_count < 0 or records_per_page <= 0:
        raise ValueError("entry_count must be non-negative and page density positive")
    return (entry_count + records_per_page - 1) // records_per_page


def paged_series_layout_components(
    series_count: int, cold_label_bytes: int
) -> dict[str, int]:
    if series_count < 0 or cold_label_bytes < 0:
        raise ValueError("series_count and cold_label_bytes must be non-negative")
    hot_page_count = (
        series_count + PAGED_RECORDS_PER_PAGE - 1
    ) // PAGED_RECORDS_PER_PAGE
    cold_page_count = (
        cold_label_bytes + PAGED_COLD_PAGE_BYTES - 1
    ) // PAGED_COLD_PAGE_BYTES
    root_bytes = PAGED_ROOT_BYTES
    hot_descriptor_bytes = hot_page_count * PAGED_DESCRIPTOR_BYTES
    cold_descriptor_bytes = cold_page_count * PAGED_DESCRIPTOR_BYTES
    descriptor_bytes = hot_descriptor_bytes + cold_descriptor_bytes
    root_end = root_bytes + descriptor_bytes
    alignment_bytes = _align_up(root_end, PAGED_HOT_OFFSET_ALIGNMENT) - root_end
    page_bytes = hot_page_count * PAGED_PAGE_BYTES
    page_padding_bytes = page_bytes - (
        hot_page_count * PAGED_PAGE_HEADER_BYTES + series_count * PAGED_RECORD_BYTES
    )
    cold_final_page_bytes = 0
    if cold_page_count:
        cold_final_page_bytes = (
            cold_label_bytes - (cold_page_count - 1) * PAGED_COLD_PAGE_BYTES
        )
    return {
        "hot_page_count": hot_page_count,
        "cold_page_count": cold_page_count,
        "total_page_count": hot_page_count + cold_page_count,
        "root_bytes": root_bytes,
        "hot_descriptor_bytes": hot_descriptor_bytes,
        "cold_descriptor_bytes": cold_descriptor_bytes,
        "descriptor_bytes": descriptor_bytes,
        "alignment_bytes": alignment_bytes,
        "page_bytes": page_bytes,
        "page_padding_bytes": page_padding_bytes,
        "cold_page_bytes": cold_label_bytes,
        "cold_final_page_bytes": cold_final_page_bytes,
    }


def _read_meta(segment: Path) -> tuple[int, int, int | None]:
    path = segment / "meta.json"
    size = _regular_file_size(path)
    if size > 1_048_576:
        raise _error(path, "meta.json exceeds the 1 MiB inventory limit")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise _error(path, f"invalid JSON: {error}") from None
    if not isinstance(value, dict):
        raise _error(path, "top-level JSON value is not an object")
    start_ms = value.get("start_ms")
    end_ms = value.get("end_ms")
    series = value.get("series")
    if not isinstance(start_ms, int) or isinstance(start_ms, bool) or start_ms < 0:
        raise _error(path, "start_ms is not a non-negative integer")
    if not isinstance(end_ms, int) or isinstance(end_ms, bool) or end_ms <= start_ms:
        raise _error(path, "end_ms is not an integer greater than start_ms")
    if series is not None and (
        not isinstance(series, int) or isinstance(series, bool) or series < 0
    ):
        raise _error(path, "series is not a non-negative integer")
    return start_ms, end_ms, series


def _read_series_header(path: Path, source: BinaryIO, file_size: int) -> SeriesHeader:
    values = SERIES_HEADER.unpack(_read_exact(source, SERIES_HEADER_BYTES, path, "series header"))
    (
        magic,
        version,
        flags,
        num_series,
        num_keysets,
        num_value_dicts,
        reserved,
        table_offset,
        keysets_offset,
        value_dicts_offset,
        keyset_blocks_offset,
        meta_offset,
    ) = values
    if magic != SERIES_MAGIC or version != SERIES_VERSION:
        raise _error(path, f"expected series.bin v{SERIES_VERSION}")
    if flags != 0 or reserved != 0:
        raise _error(path, "series header flags or reserved field are non-zero")
    if table_offset != SERIES_HEADER_BYTES:
        raise _error(path, "series table does not start after the header")
    expected_keysets = table_offset + num_series * SERIES_ROW_BYTES
    if keysets_offset != expected_keysets:
        raise _error(path, "series table length does not equal series_count * 40")
    offsets = (keysets_offset, value_dicts_offset, keyset_blocks_offset, meta_offset, file_size)
    if any(right < left for left, right in zip(offsets, offsets[1:])):
        raise _error(path, "series section offsets are not non-decreasing")
    return SeriesHeader(
        num_series,
        num_keysets,
        num_value_dicts,
        table_offset,
        keysets_offset,
        value_dicts_offset,
        keyset_blocks_offset,
        meta_offset,
    )


def _segment_directories(corpus: Path) -> list[Path]:
    try:
        metadata = corpus.lstat()
    except FileNotFoundError:
        raise _error(corpus, "corpus does not exist") from None
    if not stat.S_ISDIR(metadata.st_mode):
        raise _error(corpus, "corpus must be a directory and not a symbolic link")
    segments = []
    for path in corpus.iterdir():
        if not path.name.startswith("seg-"):
            continue
        metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode):
            raise _error(path, "segment path must be a directory and not a symbolic link")
        segments.append(path)
    if not segments:
        raise _error(corpus, "corpus contains no seg-* directories")
    return sorted(segments, key=lambda path: path.name)


def _decode_locator(trailer: bytes, offset: int) -> BlobLocator:
    locator_offset, locator_len = struct.unpack_from("<QQ", trailer, offset)
    return BlobLocator(locator_offset, locator_len)


def _validate_index_v7_exact_directory(
    path: Path,
    source: BinaryIO,
    locator: BlobLocator,
    *,
    exact_entry_count: int,
    exact_page_count: int,
) -> None:
    directory = _read_exact_at(
        source, locator.offset, locator.length, path, "exact directory"
    )
    if len(directory) < INDEX_EXACT_DIRECTORY_HEADER_BYTES:
        raise _error(path, "exact directory is shorter than its fixed header")
    (
        magic,
        version,
        flags,
        header_len,
        descriptor_len,
        page_len,
        record_len,
        directory_entry_count,
        directory_page_count,
        records_per_page,
        descriptors_offset,
        descriptors_len,
        stored_crc,
        reserved,
    ) = struct.unpack_from("<IHHIIIIQIIQQII", directory)
    if magic != INDEX_EXACT_DIRECTORY_MAGIC or version != INDEX_EXACT_DIRECTORY_VERSION:
        raise _error(path, "expected exact directory v1")
    if flags != 0 or reserved != 0:
        raise _error(path, "exact directory flags or reserved field are non-zero")
    if header_len != INDEX_EXACT_DIRECTORY_HEADER_BYTES:
        raise _error(path, "exact directory header length is invalid")
    if descriptor_len != INDEX_EXACT_PAGE_DESCRIPTOR_BYTES:
        raise _error(path, "exact directory descriptor length is invalid")
    if page_len != INDEX_EXACT_PAGE_BYTES:
        raise _error(path, "exact directory page length is invalid")
    if record_len != INDEX_V7_EXACT_RECORD_BYTES:
        raise _error(path, "exact directory record length is invalid")
    if directory_entry_count != exact_entry_count:
        raise _error(path, "exact directory entry count disagrees with the trailer")
    if directory_page_count != exact_page_count:
        raise _error(path, "exact directory page count disagrees with the trailer")
    if records_per_page != INDEX_V7_EXACT_RECORDS_PER_PAGE:
        raise _error(path, "exact directory records-per-page value is invalid")
    if descriptors_offset != INDEX_EXACT_DIRECTORY_HEADER_BYTES:
        raise _error(path, "exact directory descriptor offset is invalid")
    expected_descriptors_len = exact_page_count * INDEX_EXACT_PAGE_DESCRIPTOR_BYTES
    if descriptors_len != expected_descriptors_len:
        raise _error(path, "exact directory descriptor length is inconsistent")
    if locator.length != INDEX_EXACT_DIRECTORY_HEADER_BYTES + expected_descriptors_len:
        raise _error(path, "exact directory locator length is inconsistent")
    if _crc32c_with_zeroed_u32(directory, 56) != stored_crc:
        raise _error(path, "exact directory CRC mismatch")

    decoded_entries = 0
    previous_last_key: tuple[int, int] | None = None
    for page_index in range(exact_page_count):
        descriptor_offset = (
            INDEX_EXACT_DIRECTORY_HEADER_BYTES
            + page_index * INDEX_EXACT_PAGE_DESCRIPTOR_BYTES
        )
        (
            first_name,
            first_value,
            last_name,
            last_value,
            record_count,
            reserved0,
            _page_crc32c,
            reserved1,
        ) = struct.unpack_from("<IIIIIIII", directory, descriptor_offset)
        if reserved0 != 0 or reserved1 != 0:
            raise _error(path, "exact page descriptor reserved field is non-zero")
        first_key = (first_name, first_value)
        last_key = (last_name, last_value)
        if first_key > last_key:
            raise _error(path, "exact page descriptor key range is reversed")
        if previous_last_key is not None and previous_last_key >= first_key:
            raise _error(path, "exact page descriptors are unordered or overlapping")
        expected_record_count = min(
            exact_entry_count - decoded_entries,
            INDEX_V7_EXACT_RECORDS_PER_PAGE,
        )
        if record_count == 0 or record_count != expected_record_count:
            raise _error(path, "exact page descriptor record count is invalid")
        decoded_entries += record_count
        previous_last_key = last_key
    if decoded_entries != exact_entry_count:
        raise _error(path, "exact page descriptor counts disagree with the trailer")


def _validate_index_v7_auxiliary_directory(
    path: Path,
    source: BinaryIO,
    locator: BlobLocator,
    payloads: BlobLocator,
    *,
    auxiliary_entry_count: int,
) -> None:
    directory = _read_exact_at(
        source, locator.offset, locator.length, path, "auxiliary directory"
    )
    if len(directory) < INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES:
        raise _error(path, "auxiliary directory is shorter than its fixed header")
    (
        magic,
        version,
        flags,
        header_len,
        record_len,
        directory_entry_count,
        records_offset,
        records_len,
        stored_crc,
        reserved,
    ) = struct.unpack_from("<IHHIIQQQI20s", directory)
    if (
        magic != INDEX_AUXILIARY_DIRECTORY_MAGIC
        or version != INDEX_AUXILIARY_DIRECTORY_VERSION
    ):
        raise _error(path, "expected auxiliary directory v1")
    if flags != 0 or reserved != bytes(20):
        raise _error(path, "auxiliary directory flags or reserved bytes are non-zero")
    if header_len != INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES:
        raise _error(path, "auxiliary directory header length is invalid")
    if record_len != INDEX_V7_AUXILIARY_RECORD_BYTES:
        raise _error(path, "auxiliary directory record length is invalid")
    if directory_entry_count != auxiliary_entry_count:
        raise _error(path, "auxiliary directory entry count disagrees with the trailer")
    if records_offset != INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES:
        raise _error(path, "auxiliary directory record offset is invalid")
    expected_records_len = auxiliary_entry_count * INDEX_V7_AUXILIARY_RECORD_BYTES
    if records_len != expected_records_len:
        raise _error(path, "auxiliary directory records length is inconsistent")
    if locator.length != INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES + expected_records_len:
        raise _error(path, "auxiliary directory locator length is inconsistent")
    if _crc32c_with_zeroed_u32(directory, 40) != stored_crc:
        raise _error(path, "auxiliary directory CRC mismatch")

    previous_key: tuple[int, int] | None = None
    for record_index in range(auxiliary_entry_count):
        offset = (
            INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES
            + record_index * INDEX_V7_AUXILIARY_RECORD_BYTES
        )
        (
            kind,
            record_flags,
            label_name_sym,
            payload_offset,
            payload_len,
            min_time_ms,
            max_time_ms,
        ) = struct.unpack_from("<HHIQQQQ", directory, offset)
        if kind not in (2, 3):
            raise _error(path, "auxiliary directory record kind is unsupported")
        if record_flags != 0:
            raise _error(path, "auxiliary directory record flags are non-zero")
        key = (kind, label_name_sym)
        if previous_key is not None and previous_key >= key:
            raise _error(path, "auxiliary directory records are unordered or duplicated")
        if payload_len == 0:
            raise _error(path, "auxiliary directory record has an empty payload")
        payload_end = payload_offset + payload_len
        if payload_offset < payloads.offset or payload_end > payloads.end:
            raise _error(path, "auxiliary directory record exceeds its payload region")
        if min_time_ms > max_time_ms:
            raise _error(path, "auxiliary directory record has a reversed time range")
        previous_key = key


def _read_index_v7_shape(path: Path) -> IndexV7Shape:
    file_size = _regular_file_size(path)
    if file_size < INDEX_HEADER_BYTES + INDEX_TRAILER_BYTES:
        raise _error(path, "index container is shorter than its fixed root")
    with path.open("rb", buffering=0) as source:
        header = _read_exact_at(source, 0, INDEX_HEADER_BYTES, path, "index header")
        magic, version, flags, header_len, reserved = INDEX_HEADER.unpack(header)
        if magic != INDEX_MAGIC or version != INDEX_V7_VERSION:
            raise _error(path, "expected indexes.puffin v7")
        if flags != 0 or reserved != 0 or header_len != INDEX_HEADER_BYTES:
            raise _error(path, "index header fields are invalid")

        trailer_offset = file_size - INDEX_TRAILER_BYTES
        trailer = _read_exact_at(
            source, trailer_offset, INDEX_TRAILER_BYTES, path, "index trailer"
        )
        trailer_magic, trailer_version, trailer_flags, trailer_len, reserved0 = (
            struct.unpack_from("<IHHII", trailer)
        )
        if trailer_magic != INDEX_TRAILER_MAGIC or trailer_version != INDEX_V7_VERSION:
            raise _error(path, "expected index trailer v7")
        if trailer_flags != 0 or reserved0 != 0 or trailer_len != INDEX_TRAILER_BYTES:
            raise _error(path, "index trailer fields are invalid")
        if struct.unpack_from("<Q", trailer, 16)[0] != file_size:
            raise _error(path, "index trailer file length disagrees with the file")
        if trailer[164:INDEX_TRAILER_TERMINAL_MAGIC_OFFSET] != bytes(88):
            raise _error(path, "index trailer reserved bytes are non-zero")
        if (
            struct.unpack_from("<I", trailer, INDEX_TRAILER_TERMINAL_MAGIC_OFFSET)[0]
            != INDEX_V7_TERMINAL_MAGIC
        ):
            raise _error(path, "index trailer terminal magic is invalid")
        stored_trailer_crc = struct.unpack_from(
            "<I", trailer, INDEX_TRAILER_CRC_OFFSET
        )[0]
        if (
            _crc32c_with_zeroed_u32(trailer, INDEX_TRAILER_CRC_OFFSET)
            != stored_trailer_crc
        ):
            raise _error(path, "index trailer CRC mismatch")

        locators: dict[str, BlobLocator] = {}
        for name, offset, required in INDEX_LOCATORS:
            locator = _decode_locator(trailer, offset)
            if (locator.offset == 0) != (locator.length == 0):
                raise _error(path, f"{name} locator is half-empty")
            if required and not locator.present:
                raise _error(path, f"required {name} locator is absent")
            locators[name] = locator

        previous_end = INDEX_HEADER_BYTES
        for name in INDEX_PHYSICAL_REGION_ORDER:
            locator = locators[name]
            if not locator.present:
                continue
            if locator.offset < INDEX_HEADER_BYTES or locator.end > trailer_offset:
                raise _error(path, f"{name} locator lies outside the payload region")
            if locator.offset < previous_end:
                raise _error(path, "index regions overlap or are out of physical order")
            previous_end = locator.end

        exact_entry_count = struct.unpack_from("<Q", trailer, 136)[0]
        exact_page_count = struct.unpack_from("<I", trailer, 144)[0]
        exact_record_len = struct.unpack_from("<I", trailer, 148)[0]
        exact_page_len = struct.unpack_from("<I", trailer, 152)[0]
        auxiliary_entry_count = struct.unpack_from("<I", trailer, 156)[0]
        if exact_record_len != INDEX_V7_EXACT_RECORD_BYTES:
            raise _error(path, "index trailer exact record length is invalid")
        if exact_page_len != INDEX_EXACT_PAGE_BYTES:
            raise _error(path, "index trailer exact page length is invalid")
        expected_v7_pages = _page_count(
            exact_entry_count, INDEX_V7_EXACT_RECORDS_PER_PAGE
        )
        if exact_page_count != expected_v7_pages:
            raise _error(path, "index trailer exact page count is inconsistent")
        if exact_entry_count == 0:
            if locators["exact pages"].present or locators["exact postings"].present:
                raise _error(path, "empty exact index has payload regions")
        elif not (
            locators["exact pages"].present and locators["exact postings"].present
        ):
            raise _error(path, "non-empty exact index is missing payload regions")
        if auxiliary_entry_count == 0:
            if locators["auxiliary payloads"].present:
                raise _error(path, "empty auxiliary index has a payload region")
        elif not locators["auxiliary payloads"].present:
            raise _error(path, "non-empty auxiliary index is missing its payload region")

        expected_exact_directory_len = (
            INDEX_EXACT_DIRECTORY_HEADER_BYTES
            + exact_page_count * INDEX_EXACT_PAGE_DESCRIPTOR_BYTES
        )
        if locators["exact directory"].length != expected_exact_directory_len:
            raise _error(path, "exact directory root length is inconsistent")
        if locators["exact pages"].length != exact_page_count * INDEX_EXACT_PAGE_BYTES:
            raise _error(path, "exact pages root length is inconsistent")
        expected_auxiliary_directory_len = (
            INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES
            + auxiliary_entry_count * INDEX_V7_AUXILIARY_RECORD_BYTES
        )
        if locators["auxiliary directory"].length != expected_auxiliary_directory_len:
            raise _error(path, "auxiliary directory root length is inconsistent")

        _validate_index_v7_exact_directory(
            path,
            source,
            locators["exact directory"],
            exact_entry_count=exact_entry_count,
            exact_page_count=exact_page_count,
        )
        _validate_index_v7_auxiliary_directory(
            path,
            source,
            locators["auxiliary directory"],
            locators["auxiliary payloads"],
            auxiliary_entry_count=auxiliary_entry_count,
        )

    return IndexV7Shape(
        exact_entry_count=exact_entry_count,
        exact_page_count=exact_page_count,
        v8_exact_page_count=_page_count(
            exact_entry_count, INDEX_V8_EXACT_RECORDS_PER_PAGE
        ),
        auxiliary_entry_count=auxiliary_entry_count,
    )


def _chunk_inline_fits(
    entry: tuple[int, int, int, int, int, int, int, int, int],
    *,
    start_ms: int,
    end_ms: int,
    chunk_file_sizes: dict[int, int],
    series_kind_mask: int,
    result: SegmentResult,
) -> tuple[bool, bool]:
    file_id, kind, _flags, min_ms, max_ms, offset, length, scalar_offset, scalar_len = entry
    if kind not in KNOWN_CHUNK_KINDS:
        raise ModelError(f"unknown chunk kind {kind}")
    if min_ms > max_ms:
        raise ModelError("chunk minimum timestamp exceeds maximum timestamp")
    chunk_file_size = chunk_file_sizes.get(file_id)
    if chunk_file_size is None:
        raise ModelError(f"chunk entry names unsupported or absent file_id {file_id}")
    if offset + length > chunk_file_size:
        raise ModelError("chunk entry exceeds its chunk file")
    if scalar_len != 0 and scalar_offset + scalar_len > length:
        raise ModelError("scalar lane exceeds its chunk")

    outside_segment = min_ms < start_ms or max_ms >= end_ms
    relative_time_failure = outside_segment or max_ms - start_ms > U32_MAX
    offset_failure = offset > U32_MAX
    length_failure = length < CHUNK_HEADER_BYTES
    scalar_failure = not (
        (scalar_offset == 0 and scalar_len == 0)
        or (
            scalar_offset == CHUNK_HEADER_BYTES
            and scalar_len >= 16
            and kind in (2, 3, 4)
        )
    )
    scalar_len_21bit_failure = scalar_len > PAGED_SCALAR_LANE_LEN_MAX
    file_id_bit_failure = file_id > 1
    kind_mismatch = series_kind_mask != (1 << kind)

    result.outside_segment_time_failures += int(outside_segment)
    result.relative_time_u32_failures += int(relative_time_failure)
    result.chunk_offset_u32_failures += int(offset_failure)
    result.chunk_length_min_failures += int(length_failure)
    result.scalar_lane_shape_failures += int(scalar_failure)
    result.scalar_lane_len_21bit_failures += int(scalar_len_21bit_failure)
    result.file_id_bit_failures += int(file_id_bit_failure)
    result.series_kind_mismatches += int(kind_mismatch)
    if not outside_segment:
        result.max_min_time_delta_ms = max(result.max_min_time_delta_ms, min_ms - start_ms)
        result.max_max_time_delta_ms = max(result.max_max_time_delta_ms, max_ms - start_ms)
    result.max_chunk_offset = max(result.max_chunk_offset, offset)
    result.max_chunk_length = max(result.max_chunk_length, length)
    result.max_scalar_lane_length = max(result.max_scalar_lane_length, scalar_len)

    # The conservative descriptor retains file_id as u8 and scalar_lane_len as
    # u32. The selected paged control word retains a one-bit file ID and packs
    # scalar_lane_len into 21 bits. Chunk flags are authenticated in the chunk
    # header and are intentionally not indexed. The schema-7 writer computes
    # the indexed chunk/scalar-prefix CRC, so it is not a legacy-corpus fit
    # failure.
    conservative = not (
        relative_time_failure
        or offset_failure
        or length_failure
        or scalar_failure
        or kind_mismatch
    )
    paged = (
        conservative
        and not scalar_len_21bit_failure
        and not file_id_bit_failure
    )
    return conservative, paged


def analyze_segment(segment: Path) -> tuple[SegmentResult, Counter[int], Counter[int], Counter[int]]:
    start_ms, end_ms, meta_series = _read_meta(segment)
    series_path = segment / "series.bin"
    index_path = segment / "chunk_index.bin"
    chunks_path = segment / "chunks.bin"
    ooo_chunks_path = segment / "ooo_chunks.bin"
    indexes_path = segment / "indexes.puffin"
    series_size = _regular_file_size(series_path)
    index_size = _regular_file_size(index_path)
    chunks_size = _regular_file_size(chunks_path)
    chunk_file_sizes = {0: chunks_size}
    try:
        ooo_chunks_path.lstat()
    except FileNotFoundError:
        pass
    else:
        chunk_file_sizes[1] = _regular_file_size(ooo_chunks_path)

    result = SegmentResult()
    index_shape = _read_index_v7_shape(indexes_path)
    result.index_exact_entry_count = index_shape.exact_entry_count
    result.index_v7_exact_page_count = index_shape.exact_page_count
    result.index_v8_exact_page_count = index_shape.v8_exact_page_count
    result.index_auxiliary_entry_count = index_shape.auxiliary_entry_count
    file_ids: Counter[int] = Counter()
    kinds: Counter[int] = Counter()
    flags: Counter[int] = Counter()

    with series_path.open("rb", buffering=0) as series_file, index_path.open(
        "rb", buffering=0
    ) as index_file:
        series_header = _read_series_header(series_path, series_file, series_size)
        if meta_series is not None and meta_series != series_header.num_series:
            raise _error(segment / "meta.json", "series count disagrees with series.bin")
        index_header = CHUNK_INDEX_HEADER.unpack(
            _read_exact(index_file, CHUNK_INDEX_HEADER_BYTES, index_path, "chunk-index header")
        )
        magic, version, index_flags, index_series = index_header
        if magic != CHUNK_INDEX_MAGIC or version != CHUNK_INDEX_VERSION:
            raise _error(index_path, f"expected chunk_index.bin v{CHUNK_INDEX_VERSION}")
        if index_flags != 0:
            raise _error(index_path, "chunk-index flags are non-zero")
        if index_series != series_header.num_series:
            raise _error(index_path, "series count disagrees with series.bin")

        directory_bytes = CHUNK_INDEX_DIRECTORY_ENTRY_BYTES * (index_series + 1)
        entries_start = CHUNK_INDEX_HEADER_BYTES + directory_bytes
        if entries_start > index_size:
            raise _error(index_path, "chunk-index directory exceeds the file")
        entries_bytes = index_size - entries_start
        if entries_bytes % CHUNK_ENTRY_BYTES != 0:
            raise _error(index_path, "chunk-index entry region is not 40-byte aligned")

        result.series_count = index_series
        result.series_header_bytes = SERIES_HEADER_BYTES
        result.series_table_bytes = index_series * SERIES_ROW_BYTES
        result.keysets_bytes = series_header.value_dicts_offset - series_header.keysets_offset
        result.value_dicts_bytes = (
            series_header.keyset_blocks_offset - series_header.value_dicts_offset
        )
        result.keyset_blocks_bytes = series_header.meta_offset - series_header.keyset_blocks_offset
        result.series_metadata_bytes = series_size - series_header.meta_offset
        if result.series_metadata_bytes != 0:
            raise _error(series_path, "series v2 metadata region must be empty")
        result.chunk_index_header_bytes = CHUNK_INDEX_HEADER_BYTES
        result.chunk_index_directory_bytes = directory_bytes
        result.chunk_index_entry_bytes = entries_bytes

        series_file.seek(series_header.table_offset)
        index_file.seek(CHUNK_INDEX_HEADER_BYTES)
        previous_directory_offset: int | None = None
        remaining = index_series
        while remaining:
            count = min(remaining, ROWS_PER_BLOCK)
            table = _read_exact(
                series_file,
                count * SERIES_ROW_BYTES,
                series_path,
                "series table block",
            )
            directory_count = count + int(previous_directory_offset is None)
            directory_raw = _read_exact(
                index_file,
                directory_count * CHUNK_INDEX_DIRECTORY_ENTRY_BYTES,
                index_path,
                "chunk-index directory block",
            )
            directory = [value[0] for value in struct.iter_unpack("<Q", directory_raw)]
            if previous_directory_offset is not None:
                directory.insert(0, previous_directory_offset)
            if directory[0] < entries_start or any(
                right < left for left, right in zip(directory, directory[1:])
            ):
                raise _error(index_path, "chunk-index directory offsets are invalid")
            previous_directory_offset = directory[-1]

            entry_block_len = directory[-1] - directory[0]
            if entry_block_len % CHUNK_ENTRY_BYTES != 0:
                raise _error(index_path, "series chunk range is not entry-aligned")
            current_index_position = index_file.tell()
            index_file.seek(directory[0])
            entry_block = _read_exact(
                index_file,
                entry_block_len,
                index_path,
                "chunk-index entry block",
            )
            index_file.seek(current_index_position)
            entry_iterator = iter(CHUNK_ENTRY.iter_unpack(entry_block))

            for row_index, row in enumerate(SERIES_ROW.iter_unpack(table)):
                (
                    _series_id,
                    kind_mask,
                    row_flags,
                    row_reserved,
                    chunk_range_offset,
                    chunk_range_len,
                    keyset_id,
                    _keyset_row,
                    meta_offset,
                    meta_len,
                ) = row
                if row_flags != 0 or row_reserved != 0:
                    raise _error(series_path, "series row flags or reserved field are non-zero")
                if kind_mask == 0 or kind_mask & ~KNOWN_KIND_MASK:
                    raise _error(series_path, "series row has an invalid kind mask")
                if keyset_id >= series_header.num_keysets:
                    raise _error(series_path, "series row keyset ID is out of bounds")
                if meta_offset != 0 or meta_len != 0:
                    raise _error(
                        series_path,
                        "series v2 metadata fields must be zero for the schema-7 model",
                    )

                left = directory[row_index]
                right = directory[row_index + 1]
                if chunk_range_offset != left or chunk_range_len != right - left:
                    raise _error(series_path, "series chunk locator disagrees with chunk directory")
                chunk_count = (right - left) // CHUNK_ENTRY_BYTES
                result.chunk_count += chunk_count
                result.max_chunks_per_series = max(result.max_chunks_per_series, chunk_count)
                result.zero_chunk_series += int(chunk_count == 0)
                result.one_chunk_series += int(chunk_count == 1)
                result.multi_chunk_series += int(chunk_count > 1)

                entries = [next(entry_iterator) for _ in range(chunk_count)]
                for entry in entries:
                    file_ids[entry[0]] += 1
                    kinds[entry[1]] += 1
                    flags[entry[2]] += 1
                conservative_eligible = chunk_count == 1
                paged_eligible = conservative_eligible
                if conservative_eligible:
                    try:
                        conservative_eligible, paged_eligible = _chunk_inline_fits(
                            entries[0],
                            start_ms=start_ms,
                            end_ms=end_ms,
                            chunk_file_sizes=chunk_file_sizes,
                            series_kind_mask=kind_mask,
                            result=result,
                        )
                    except ModelError as error:
                        raise _error(index_path, str(error)) from None
                else:
                    for entry in entries:
                        try:
                            _chunk_inline_fits(
                                entry,
                                start_ms=start_ms,
                                end_ms=end_ms,
                                chunk_file_sizes=chunk_file_sizes,
                                series_kind_mask=kind_mask,
                                result=result,
                            )
                        except ModelError as error:
                            raise _error(index_path, str(error)) from None
                if conservative_eligible:
                    result.conservative_inline_eligible_series += 1
                else:
                    result.conservative_overflow_series += 1
                    result.conservative_overflow_chunks += chunk_count
                if paged_eligible:
                    result.paged_inline_eligible_series += 1
                else:
                    result.paged_overflow_series += 1
                    result.paged_overflow_chunks += chunk_count

            try:
                next(entry_iterator)
            except StopIteration:
                pass
            else:
                raise _error(index_path, "chunk entry block was not fully assigned to series")
            remaining -= count

        if previous_directory_offset != index_size:
            raise _error(index_path, "final directory offset does not equal file size")
        if result.chunk_count * CHUNK_ENTRY_BYTES != entries_bytes:
            raise _error(index_path, "directory chunk count disagrees with entry bytes")

    cold_label_bytes = (
        result.keysets_bytes + result.value_dicts_bytes + result.keyset_blocks_bytes
    )
    paged_layout = paged_series_layout_components(
        result.series_count, cold_label_bytes
    )
    result.paged_hot_page_count = paged_layout["hot_page_count"]
    result.paged_cold_page_count = paged_layout["cold_page_count"]
    result.paged_cold_final_page_bytes = paged_layout["cold_final_page_bytes"]
    result.paged_root_bytes = paged_layout["root_bytes"]
    result.paged_descriptor_bytes = paged_layout["descriptor_bytes"]
    result.paged_hot_offset_alignment_bytes = paged_layout["alignment_bytes"]
    result.paged_page_bytes = paged_layout["page_bytes"]
    result.paged_page_padding_bytes = paged_layout["page_padding_bytes"]
    result.paged_overflow_root_bytes = PAGED_OVERFLOW_ROOT_BYTES
    result.paged_overflow_blob_header_bytes = result.paged_overflow_series * 32
    result.paged_overflow_entry_bytes = (
        result.paged_overflow_chunks * PAGED_OVERFLOW_ENTRY_BYTES
    )

    return result, file_ids, kinds, flags


def _basis_points(numerator: int, denominator: int) -> int:
    return 0 if denominator == 0 else numerator * 10_000 // denominator


def model_corpus(corpus: Path) -> dict[str, Any]:
    segments = _segment_directories(corpus)
    artifact_files = defaultdict(int)
    artifact_bytes = defaultdict(int)
    aggregate = SegmentResult()
    file_ids: Counter[int] = Counter()
    kinds: Counter[int] = Counter()
    flags: Counter[int] = Counter()
    segment_shapes: list[dict[str, Any]] = []

    for segment in segments:
        for artifact in STANDARD_ARTIFACTS:
            path = segment / artifact
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            artifact_files[artifact] += 1
            artifact_bytes[artifact] += _regular_file_size(path)
        result, segment_file_ids, segment_kinds, segment_flags = analyze_segment(segment)
        segment_shapes.append(
            {
                "segment": segment.name,
                "series_count": result.series_count,
                "chunk_count": result.chunk_count,
                "zero_chunk_series": result.zero_chunk_series,
                "one_chunk_series": result.one_chunk_series,
                "multi_chunk_series": result.multi_chunk_series,
                "exact_entry_count": result.index_exact_entry_count,
                "v7_exact_page_count": result.index_v7_exact_page_count,
                "v8_exact_page_count": result.index_v8_exact_page_count,
                "auxiliary_entry_count": result.index_auxiliary_entry_count,
            }
        )
        for field in result.__dataclass_fields__:
            value = getattr(result, field)
            if field.startswith("max_"):
                setattr(aggregate, field, max(getattr(aggregate, field), value))
            else:
                setattr(aggregate, field, getattr(aggregate, field) + value)
        file_ids.update(segment_file_ids)
        kinds.update(segment_kinds)
        flags.update(segment_flags)

    if aggregate.series_count != sum(
        (aggregate.zero_chunk_series, aggregate.one_chunk_series, aggregate.multi_chunk_series)
    ):
        raise _error(corpus, "series chunk-shape counts do not sum to series count")
    if aggregate.series_count != (
        aggregate.conservative_inline_eligible_series
        + aggregate.conservative_overflow_series
    ):
        raise _error(corpus, "conservative inline and overflow counts do not sum to series count")
    if aggregate.series_count != (
        aggregate.paged_inline_eligible_series + aggregate.paged_overflow_series
    ):
        raise _error(corpus, "paged inline and overflow counts do not sum to series count")

    total_artifact_bytes = sum(artifact_bytes.values())
    current_metadata_bytes = sum(artifact_bytes[name] for name in METADATA_ARTIFACTS)
    current_series_index_bytes = artifact_bytes["series.bin"] + artifact_bytes["chunk_index.bin"]
    current_hot_bytes = (
        aggregate.series_table_bytes
        + aggregate.chunk_index_directory_bytes
        + aggregate.chunk_index_entry_bytes
    )
    current_structural_bytes = (
        aggregate.series_header_bytes
        + current_hot_bytes
        + aggregate.chunk_index_header_bytes
    )
    cold_label_bytes = (
        aggregate.keysets_bytes
        + aggregate.value_dicts_bytes
        + aggregate.keyset_blocks_bytes
    )
    if (
        current_structural_bytes + cold_label_bytes + aggregate.series_metadata_bytes
        != current_series_index_bytes
    ):
        raise _error(corpus, "modeled current components do not equal series/index bytes")

    index_v7_exact_record_bytes = (
        aggregate.index_exact_entry_count * INDEX_V7_EXACT_RECORD_BYTES
    )
    index_v8_exact_record_bytes = (
        aggregate.index_exact_entry_count * INDEX_V8_EXACT_RECORD_BYTES
    )
    index_v7_exact_page_bytes = (
        aggregate.index_v7_exact_page_count * INDEX_EXACT_PAGE_BYTES
    )
    index_v8_exact_page_bytes = (
        aggregate.index_v8_exact_page_count * INDEX_EXACT_PAGE_BYTES
    )
    index_v7_exact_descriptor_bytes = (
        aggregate.index_v7_exact_page_count * INDEX_EXACT_PAGE_DESCRIPTOR_BYTES
    )
    index_v8_exact_descriptor_bytes = (
        aggregate.index_v8_exact_page_count * INDEX_EXACT_PAGE_DESCRIPTOR_BYTES
    )
    index_v7_auxiliary_record_bytes = (
        aggregate.index_auxiliary_entry_count * INDEX_V7_AUXILIARY_RECORD_BYTES
    )
    index_v8_auxiliary_record_bytes = (
        aggregate.index_auxiliary_entry_count * INDEX_V8_AUXILIARY_RECORD_BYTES
    )
    index_v8_overhead_bytes = (
        index_v8_exact_page_bytes
        - index_v7_exact_page_bytes
        + index_v8_exact_descriptor_bytes
        - index_v7_exact_descriptor_bytes
        + index_v8_auxiliary_record_bytes
        - index_v7_auxiliary_record_bytes
    )
    projected_index_v8_bytes = artifact_bytes["indexes.puffin"] + index_v8_overhead_bytes
    index_unchanged_remainder_bytes = artifact_bytes["indexes.puffin"] - (
        index_v7_exact_page_bytes
        + index_v7_exact_descriptor_bytes
        + index_v7_auxiliary_record_bytes
    )
    if index_unchanged_remainder_bytes < 0:
        raise _error(corpus, "modeled v7 index components exceed indexes.puffin bytes")
    if projected_index_v8_bytes != (
        index_unchanged_remainder_bytes
        + index_v8_exact_page_bytes
        + index_v8_exact_descriptor_bytes
        + index_v8_auxiliary_record_bytes
    ):
        raise _error(corpus, "modeled v8 index components do not equal projected bytes")

    conservative_fixed_bytes = (
        aggregate.series_count * CONSERVATIVE_SERIES_RECORD_BYTES
    )
    conservative_overflow_bytes = (
        aggregate.conservative_overflow_chunks * CONSERVATIVE_OVERFLOW_DESCRIPTOR_BYTES
    )
    conservative_hot_bytes = conservative_fixed_bytes + conservative_overflow_bytes
    conservative_savings_bytes = current_hot_bytes - conservative_hot_bytes
    conservative_series_index_bytes = current_series_index_bytes - conservative_savings_bytes
    conservative_total_bytes = total_artifact_bytes - conservative_savings_bytes

    paged_series_bytes = (
        aggregate.paged_root_bytes
        + aggregate.paged_descriptor_bytes
        + aggregate.paged_hot_offset_alignment_bytes
        + aggregate.paged_page_bytes
        + cold_label_bytes
    )
    paged_chunk_index_bytes = (
        aggregate.paged_overflow_root_bytes
        + aggregate.paged_overflow_blob_header_bytes
        + aggregate.paged_overflow_entry_bytes
    )
    paged_series_index_bytes = paged_series_bytes + paged_chunk_index_bytes
    paged_series_index_savings_bytes = current_series_index_bytes - paged_series_index_bytes
    paged_net_savings_bytes = paged_series_index_savings_bytes - index_v8_overhead_bytes
    paged_total_bytes = total_artifact_bytes - paged_net_savings_bytes
    paged_metadata_bytes = current_metadata_bytes - paged_net_savings_bytes

    conservative_fit_failure_count = sum(
        (
            aggregate.zero_chunk_series,
            aggregate.relative_time_u32_failures,
            aggregate.chunk_offset_u32_failures,
            aggregate.chunk_length_min_failures,
            aggregate.scalar_lane_shape_failures,
            aggregate.series_kind_mismatches,
        )
    )
    paged_fit_failure_count = conservative_fit_failure_count + sum(
        (aggregate.scalar_lane_len_21bit_failures, aggregate.file_id_bit_failures)
    )

    def screening_gate(
        *, savings_bytes: int, overflow_series: int, fit_failure_count: int
    ) -> dict[str, Any]:
        overflow_basis_points = _basis_points(overflow_series, aggregate.series_count)
        corpus_basis_points = _basis_points(savings_bytes, total_artifact_bytes)
        series_index_basis_points = _basis_points(savings_bytes, current_series_index_bytes)
        checks = {
            "common_case_field_fits": fit_failure_count == 0,
            "overflow_rate_at_most_threshold": (
                overflow_basis_points <= MAX_OVERFLOW_BASIS_POINTS
            ),
            "corpus_savings_at_least_threshold": (
                corpus_basis_points >= MIN_CORPUS_SAVINGS_BASIS_POINTS
            ),
            "series_index_savings_at_least_threshold": (
                series_index_basis_points >= MIN_SERIES_INDEX_SAVINGS_BASIS_POINTS
            ),
        }
        return {
            "observed_overflow_basis_points": overflow_basis_points,
            "checks": checks,
            "passes": all(checks.values()),
        }

    return {
        "model_version": 4,
        "model_name": "schema7-compact-series-and-v8-index-layout-models",
        "corpus": str(corpus.resolve()),
        "assumptions": {
            "current_series_row_bytes": SERIES_ROW_BYTES,
            "current_chunk_directory_entry_bytes": CHUNK_INDEX_DIRECTORY_ENTRY_BYTES,
            "current_chunk_descriptor_bytes": CHUNK_ENTRY_BYTES,
            "scalar_lane_offset": (
                "implicit: 0 when scalar_lane_len is 0, otherwise 40"
            ),
            "indexed_prefix_crc": (
                "covers ChunkHeader plus a present typed-scalar header; "
                "computed by schema 7, not a legacy-corpus fit requirement"
            ),
            "symbols_scope": (
                "the reference corpus contains symbols.bin v2; symbol bytes are held "
                "constant because schema 6 and schema 7 both use symbols.bin v3. The "
                "modeled byte delta is valid for their A/B, while the absolute projected "
                "total remains a held-constant schema-5-corpus total, not a measured "
                "schema-6 total"
            ),
            "index_v8_scope": (
                "exact-postings, FST, and label-time-range payload bodies, routing, "
                "metric ranges, header, trailer length, and directory-header lengths "
                "are held constant; exact/auxiliary records and exact page density use "
                "the schema-7 v8 contract"
            ),
            "conservative_56_byte_model": {
                "series_record_bytes": CONSERVATIVE_SERIES_RECORD_BYTES,
                "inline_descriptor_bytes": CONSERVATIVE_INLINE_DESCRIPTOR_BYTES,
                "overflow_descriptor_bytes": CONSERVATIVE_OVERFLOW_DESCRIPTOR_BYTES,
                "scope": "hot routing/index bytes; all other current bytes held constant",
            },
            "selected_40_byte_paged_model": {
                "series_header_bytes": PAGED_ROOT_BYTES,
                "page_descriptor_bytes": PAGED_DESCRIPTOR_BYTES,
                "hot_pages_offset_alignment": PAGED_HOT_OFFSET_ALIGNMENT,
                "page_bytes": PAGED_PAGE_BYTES,
                "page_header_bytes": PAGED_PAGE_HEADER_BYTES,
                "record_bytes": PAGED_RECORD_BYTES,
                "records_per_page": PAGED_RECORDS_PER_PAGE,
                "cold_page_bytes": PAGED_COLD_PAGE_BYTES,
                "cold_final_page_length": "exact bytes; no fixed-page padding",
                "root_directory_descriptors": "one per hot page plus one per cold page",
                "series_header_cold_page_fields": (
                    "u32 cold_page_len and u32 cold_page_count in the 176-byte root"
                ),
                "scalar_lane_len_bits": PAGED_SCALAR_LANE_LEN_BITS,
                "chunk_index_root_bytes": PAGED_OVERFLOW_ROOT_BYTES,
                "overflow_blob_header_bytes": 32,
                "overflow_chunk_entry_bytes": PAGED_OVERFLOW_ENTRY_BYTES,
                "overflow_body": "44 * chunk_count; chunk_count must be non-zero",
                "series_metadata": "unsupported; v2 metadata fields and region must be empty",
                "chunk_flags_indexed": False,
                "file_id_bits": 1,
            },
        },
        "artifacts": {
            name: {"file_count": artifact_files[name], "bytes": artifact_bytes[name]}
            for name in STANDARD_ARTIFACTS
        },
        "current_layout": {
            "segment_count": len(segments),
            "total_artifact_bytes": total_artifact_bytes,
            "metadata_bytes": current_metadata_bytes,
            "series_and_chunk_index_bytes": current_series_index_bytes,
            "hot_routing_index_bytes": current_hot_bytes,
            "structural_bytes_including_headers": current_structural_bytes,
            "cold_label_bytes": cold_label_bytes,
            "components": {
                "series_header_bytes": aggregate.series_header_bytes,
                "series_table_bytes": aggregate.series_table_bytes,
                "keysets_bytes": aggregate.keysets_bytes,
                "value_dicts_bytes": aggregate.value_dicts_bytes,
                "keyset_blocks_bytes": aggregate.keyset_blocks_bytes,
                "series_metadata_bytes": aggregate.series_metadata_bytes,
                "chunk_index_header_bytes": aggregate.chunk_index_header_bytes,
                "chunk_index_directory_bytes": aggregate.chunk_index_directory_bytes,
                "chunk_index_entry_bytes": aggregate.chunk_index_entry_bytes,
            },
        },
        "index_layout": {
            "observed_v7": {
                "indexes_puffin_bytes": artifact_bytes["indexes.puffin"],
                "exact_entry_count": aggregate.index_exact_entry_count,
                "exact_record_bytes_logical": index_v7_exact_record_bytes,
                "exact_page_count": aggregate.index_v7_exact_page_count,
                "exact_page_bytes": index_v7_exact_page_bytes,
                "exact_page_descriptor_bytes": index_v7_exact_descriptor_bytes,
                "auxiliary_entry_count": aggregate.index_auxiliary_entry_count,
                "auxiliary_record_bytes": index_v7_auxiliary_record_bytes,
                "unchanged_remainder_bytes": index_unchanged_remainder_bytes,
            },
            "projected_v8": {
                "indexes_puffin_bytes": projected_index_v8_bytes,
                "exact_entry_count": aggregate.index_exact_entry_count,
                "exact_record_bytes_logical": index_v8_exact_record_bytes,
                "exact_page_count": aggregate.index_v8_exact_page_count,
                "exact_page_bytes": index_v8_exact_page_bytes,
                "exact_page_descriptor_bytes": index_v8_exact_descriptor_bytes,
                "auxiliary_entry_count": aggregate.index_auxiliary_entry_count,
                "auxiliary_record_bytes": index_v8_auxiliary_record_bytes,
                "unchanged_remainder_bytes": index_unchanged_remainder_bytes,
            },
            "v8_delta": {
                "exact_record_bytes_logical": (
                    index_v8_exact_record_bytes - index_v7_exact_record_bytes
                ),
                "exact_page_count": (
                    aggregate.index_v8_exact_page_count
                    - aggregate.index_v7_exact_page_count
                ),
                "exact_page_bytes": (
                    index_v8_exact_page_bytes - index_v7_exact_page_bytes
                ),
                "exact_page_descriptor_bytes": (
                    index_v8_exact_descriptor_bytes
                    - index_v7_exact_descriptor_bytes
                ),
                "auxiliary_record_bytes": (
                    index_v8_auxiliary_record_bytes
                    - index_v7_auxiliary_record_bytes
                ),
                "indexes_puffin_bytes": index_v8_overhead_bytes,
            },
        },
        "segments": segment_shapes,
        "observed": {
            "series_count": aggregate.series_count,
            "chunk_count": aggregate.chunk_count,
            "chunk_shape": {
                "zero_chunk_series": aggregate.zero_chunk_series,
                "one_chunk_series": aggregate.one_chunk_series,
                "multi_chunk_series": aggregate.multi_chunk_series,
                "max_chunks_per_series": aggregate.max_chunks_per_series,
            },
            "conservative_inline_eligible_series": (
                aggregate.conservative_inline_eligible_series
            ),
            "conservative_overflow_series": aggregate.conservative_overflow_series,
            "conservative_overflow_chunks": aggregate.conservative_overflow_chunks,
            "paged_inline_eligible_series": aggregate.paged_inline_eligible_series,
            "paged_overflow_series": aggregate.paged_overflow_series,
            "paged_overflow_chunks": aggregate.paged_overflow_chunks,
            "field_fit_failures": {
                "series_metadata_entries": aggregate.series_metadata_entries,
                "series_metadata_payload_bytes": aggregate.series_metadata_payload_bytes,
                "relative_time_u32": aggregate.relative_time_u32_failures,
                "outside_segment_time": aggregate.outside_segment_time_failures,
                "chunk_offset_u32": aggregate.chunk_offset_u32_failures,
                "chunk_length_at_least_header": aggregate.chunk_length_min_failures,
                "scalar_lane_shape": aggregate.scalar_lane_shape_failures,
                "scalar_lane_len_21bit": aggregate.scalar_lane_len_21bit_failures,
                "file_id_bit": aggregate.file_id_bit_failures,
                "series_kind_mismatch": aggregate.series_kind_mismatches,
            },
            "maxima": {
                "min_time_delta_ms": aggregate.max_min_time_delta_ms,
                "max_time_delta_ms": aggregate.max_max_time_delta_ms,
                "chunk_offset": aggregate.max_chunk_offset,
                "chunk_length": aggregate.max_chunk_length,
                "scalar_lane_length": aggregate.max_scalar_lane_length,
            },
            "file_id_counts": {str(key): value for key, value in sorted(file_ids.items())},
            "chunk_kind_counts": {str(key): value for key, value in sorted(kinds.items())},
            "chunk_flag_counts": {str(key): value for key, value in sorted(flags.items())},
        },
        "models": {
            "conservative_56_byte": {
                "fixed_series_record_bytes": conservative_fixed_bytes,
                "overflow_descriptor_bytes": conservative_overflow_bytes,
                "modeled_hot_routing_index_bytes": conservative_hot_bytes,
                "modeled_savings_bytes": conservative_savings_bytes,
                "hot_savings_basis_points": _basis_points(
                    conservative_savings_bytes, current_hot_bytes
                ),
                "series_index_savings_basis_points": _basis_points(
                    conservative_savings_bytes, current_series_index_bytes
                ),
                "corpus_savings_basis_points": _basis_points(
                    conservative_savings_bytes, total_artifact_bytes
                ),
                "projected_series_and_chunk_index_bytes": conservative_series_index_bytes,
                "projected_total_artifact_bytes": conservative_total_bytes,
            },
            "selected_40_byte_paged": {
                "hot_page_count": aggregate.paged_hot_page_count,
                "cold_page_count": aggregate.paged_cold_page_count,
                "total_page_count": (
                    aggregate.paged_hot_page_count + aggregate.paged_cold_page_count
                ),
                "series_header_bytes": aggregate.paged_root_bytes,
                "hot_page_descriptor_bytes": (
                    aggregate.paged_hot_page_count * PAGED_DESCRIPTOR_BYTES
                ),
                "cold_page_descriptor_bytes": (
                    aggregate.paged_cold_page_count * PAGED_DESCRIPTOR_BYTES
                ),
                "page_descriptor_bytes": aggregate.paged_descriptor_bytes,
                "hot_offset_alignment_bytes": aggregate.paged_hot_offset_alignment_bytes,
                "hot_page_bytes": aggregate.paged_page_bytes,
                "hot_page_zero_padding_bytes": aggregate.paged_page_padding_bytes,
                "cold_label_bytes": cold_label_bytes,
                "cold_final_pages_bytes": aggregate.paged_cold_final_page_bytes,
                "chunk_index_root_bytes": aggregate.paged_overflow_root_bytes,
                "overflow_blob_header_bytes": aggregate.paged_overflow_blob_header_bytes,
                "overflow_chunk_entry_bytes": aggregate.paged_overflow_entry_bytes,
                "projected_series_bytes": paged_series_bytes,
                "projected_chunk_index_bytes": paged_chunk_index_bytes,
                "projected_series_and_chunk_index_bytes": paged_series_index_bytes,
                "series_chunk_layout_savings_bytes": paged_series_index_savings_bytes,
                "index_v8_overhead_bytes": index_v8_overhead_bytes,
                "projected_indexes_puffin_bytes": projected_index_v8_bytes,
                "modeled_savings_bytes": paged_net_savings_bytes,
                "series_index_savings_basis_points": _basis_points(
                    paged_net_savings_bytes, current_series_index_bytes
                ),
                "corpus_savings_basis_points": _basis_points(
                    paged_net_savings_bytes, total_artifact_bytes
                ),
                "metadata_savings_basis_points": _basis_points(
                    paged_net_savings_bytes, current_metadata_bytes
                ),
                "projected_metadata_bytes": paged_metadata_bytes,
                "projected_total_artifact_bytes": paged_total_bytes,
                "unmodeled": [
                    "actual replay/sealing and query performance",
                    "CRC computation CPU cost",
                    "absolute symbols.bin v3 bytes for this schema-5 reference corpus",
                ],
            },
        },
        "screening_gate": {
            "purpose": "pre-prototype materiality screen, not an adoption decision",
            "thresholds": {
                "minimum_corpus_savings_basis_points": MIN_CORPUS_SAVINGS_BASIS_POINTS,
                "minimum_series_index_savings_basis_points": (
                    MIN_SERIES_INDEX_SAVINGS_BASIS_POINTS
                ),
                "maximum_overflow_basis_points": MAX_OVERFLOW_BASIS_POINTS,
            },
            "conservative_56_byte": screening_gate(
                savings_bytes=conservative_savings_bytes,
                overflow_series=aggregate.conservative_overflow_series,
                fit_failure_count=conservative_fit_failure_count,
            ),
            "selected_40_byte_paged": screening_gate(
                savings_bytes=paged_net_savings_bytes,
                overflow_series=aggregate.paged_overflow_series,
                fit_failure_count=paged_fit_failure_count,
            ),
        },
    }


def _write_json(report: dict[str, Any], output: Path | None) -> None:
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if output is None:
        sys.stdout.write(encoded)
        return
    try:
        with output.open("x", encoding="utf-8") as destination:
            destination.write(encoded)
    except FileExistsError:
        raise _error(output, "refusing to reuse an existing model output") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="write JSON exclusively to this path; stdout is used when omitted",
    )
    args = parser.parse_args(argv)
    try:
        report = model_corpus(args.corpus)
        _write_json(report, args.output)
    except (ModelError, OSError) as error:
        print(f"storage series layout model: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
