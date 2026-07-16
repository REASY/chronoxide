#!/usr/bin/env python3
"""Integrity-check schema-7/schema-8 exact postings and inventory adaptive encoding.

The corpus is opened read-only.  The report is a complete inventory, not a
sample: every v8/v9 exact-directory page and every exact-postings payload is
integrity-checked and decoded before it contributes to the totals. A decoded
logical fingerprint permits corresponding v8 and v9 corpora to be compared
without depending on their physical encodings.
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import hashlib
import json
import mmap
import platform
import stat
import struct
import sys
from array import array
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Iterable

try:
    import numpy as _numpy
except ImportError:  # pragma: no cover - exercised on hosts without NumPy.
    _numpy = None


REPORT_SCHEMA = "chronoxide/adaptive-postings-inventory/v1"
FINGERPRINT_DOMAIN = b"chronoxide/adaptive-postings-inventory-input/v1"
LOGICAL_FINGERPRINT_DOMAIN = b"chronoxide/decoded-exact-postings/v1"

INDEX_MAGIC = int.from_bytes(b"SIDX", "little")
INDEX_TRAILER_MAGIC = int.from_bytes(b"SIDT", "little")
INDEX_VERSION = 8
INDEX_V9_VERSION = 9
INDEX_HEADER_LEN = 16
INDEX_TRAILER_LEN = 256
INDEX_TERMINAL_MAGIC = int.from_bytes(b"S8ND", "little")
INDEX_V9_TERMINAL_MAGIC = int.from_bytes(b"S9ND", "little")

EXACT_DIRECTORY_MAGIC = int.from_bytes(b"EXD8", "little")
EXACT_DIRECTORY_VERSION = 2
EXACT_V9_DIRECTORY_MAGIC = int.from_bytes(b"EXD9", "little")
EXACT_V9_DIRECTORY_VERSION = 3
EXACT_DIRECTORY_HEADER_LEN = 64
EXACT_PAGE_DESCRIPTOR_LEN = 32
EXACT_PAGE_MAGIC = int.from_bytes(b"XPG8", "little")
EXACT_PAGE_VERSION = 2
EXACT_V9_PAGE_MAGIC = int.from_bytes(b"XPG9", "little")
EXACT_V9_PAGE_VERSION = 3
EXACT_PAGE_LEN = 16_384
EXACT_PAGE_HEADER_LEN = 16
EXACT_RECORD_LEN = 48
EXACT_RECORDS_PER_PAGE = 341

AUXILIARY_DIRECTORY_MAGIC = int.from_bytes(b"AUX8", "little")
AUXILIARY_DIRECTORY_VERSION = 2
AUXILIARY_DIRECTORY_HEADER_LEN = 64
AUXILIARY_RECORD_LEN = 48

TRAILER_FILE_LEN_OFFSET = 16
TRAILER_ROUTING_LOCATOR_OFFSET = 24
TRAILER_METRIC_LOCATOR_OFFSET = 40
TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET = 56
TRAILER_EXACT_PAGES_LOCATOR_OFFSET = 72
TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET = 88
TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET = 104
TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET = 120
TRAILER_EXACT_ENTRY_COUNT_OFFSET = 136
TRAILER_EXACT_PAGE_COUNT_OFFSET = 144
TRAILER_EXACT_RECORD_LEN_OFFSET = 148
TRAILER_EXACT_PAGE_LEN_OFFSET = 152
TRAILER_AUX_ENTRY_COUNT_OFFSET = 156
TRAILER_CRC_OFFSET = 160
TRAILER_SERIES_COUNT_OFFSET = 164
TRAILER_SYMBOL_COUNT_OFFSET = 168
TRAILER_EXACT_DIRECTORY_CRC_OFFSET = 172
TRAILER_AUX_DIRECTORY_CRC_OFFSET = 176
TRAILER_RESERVED_OFFSET = 180
TRAILER_TERMINAL_MAGIC_OFFSET = 252

SERIES_MAGIC = int.from_bytes(b"SERI", "little")
SERIES_VERSION = 3
SERIES_HEADER_LEN = 176
SERIES_DESCRIPTOR_LEN = 16
SERIES_HOT_PAGE_LEN = 16_384
SERIES_HOT_PAGE_HEADER_LEN = 24
SERIES_HOT_RECORD_LEN = 40
SERIES_RECORDS_PER_PAGE = 409
SERIES_COLD_PAGE_LEN = 16_384
SERIES_ROOT_ALIGNMENT = 4_096
SERIES_ROOT_CRC_OFFSET = 52

SYMBOLS_MAGIC = int.from_bytes(b"SYMB", "little")
SYMBOLS_VERSION = 3
SYMBOLS_HEADER_LEN = 80
SYMBOLS_PAGE_DESCRIPTOR_LEN = 48
SYMBOLS_PAGE_HEADER_LEN = 32
SYMBOLS_ROOT_CRC_OFFSET = 72

RAW_HEADER_BYTES = 4
DELTA_HEADER_BYTES = 4
NUMPY_THRESHOLD_REFS = 1_024
HASH_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class IndexFormat:
    version: int
    label: str
    terminal_magic: int
    exact_directory_magic: int
    exact_directory_version: int
    exact_page_magic: int
    exact_page_version: int


INDEX_FORMATS = {
    INDEX_VERSION: IndexFormat(
        INDEX_VERSION,
        "v8",
        INDEX_TERMINAL_MAGIC,
        EXACT_DIRECTORY_MAGIC,
        EXACT_DIRECTORY_VERSION,
        EXACT_PAGE_MAGIC,
        EXACT_PAGE_VERSION,
    ),
    INDEX_V9_VERSION: IndexFormat(
        INDEX_V9_VERSION,
        "v9",
        INDEX_V9_TERMINAL_MAGIC,
        EXACT_V9_DIRECTORY_MAGIC,
        EXACT_V9_DIRECTORY_VERSION,
        EXACT_V9_PAGE_MAGIC,
        EXACT_V9_PAGE_VERSION,
    ),
}

LENGTH_BUCKETS: tuple[tuple[str, int, int | None], ...] = (
    ("1", 1, 1),
    ("2-4", 2, 4),
    ("5-8", 5, 8),
    ("9-16", 9, 16),
    ("17-32", 17, 32),
    ("33-64", 33, 64),
    ("65-128", 65, 128),
    ("129-256", 129, 256),
    ("257-512", 257, 512),
    ("513-1024", 513, 1_024),
    ("1025-4096", 1_025, 4_096),
    ("4097-16384", 4_097, 16_384),
    ("16385+", 16_385, None),
)


class InventoryError(Exception):
    """A selected input is missing, unsupported, or corrupt."""


@dataclass(frozen=True)
class Locator:
    offset: int
    length: int

    @property
    def end(self) -> int:
        return self.offset + self.length

    @property
    def empty(self) -> bool:
        return self.offset == 0 and self.length == 0


@dataclass
class BucketStats:
    list_count: int = 0
    ref_count: int = 0
    raw_v8_bytes: int = 0
    delta_candidate_bytes: int = 0
    selected_v9_bytes: int = 0
    raw_selected_lists: int = 0
    delta_selected_lists: int = 0
    actual_encoded_postings_bytes: int = 0
    actual_raw_lists: int = 0
    actual_delta_lists: int = 0

    def add(
        self,
        ref_count: int,
        raw_bytes: int,
        delta_bytes: int,
        codec: str,
        actual_bytes: int,
        actual_codec: str,
    ) -> None:
        self.list_count += 1
        self.ref_count += ref_count
        self.raw_v8_bytes += raw_bytes
        self.delta_candidate_bytes += delta_bytes
        self.selected_v9_bytes += min(raw_bytes, delta_bytes)
        if codec == "raw32":
            self.raw_selected_lists += 1
        else:
            self.delta_selected_lists += 1
        self.actual_encoded_postings_bytes += actual_bytes
        if actual_codec == "raw32":
            self.actual_raw_lists += 1
        else:
            self.actual_delta_lists += 1

    def merge(self, other: "BucketStats") -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))

    def to_json(self, name: str, minimum: int, maximum: int | None) -> dict[str, Any]:
        return {
            "bucket": name,
            "minimum_refs": minimum,
            "maximum_refs": maximum,
            "list_count": self.list_count,
            "ref_count": self.ref_count,
            "raw_v8_bytes": self.raw_v8_bytes,
            "delta_candidate_bytes": self.delta_candidate_bytes,
            "selected_v9_bytes": self.selected_v9_bytes,
            "savings_bytes": self.raw_v8_bytes - self.selected_v9_bytes,
            "raw32_selected_lists": self.raw_selected_lists,
            "delta_uleb128_selected_lists": self.delta_selected_lists,
            "actual_encoded_postings_bytes": self.actual_encoded_postings_bytes,
            "actual_raw32_lists": self.actual_raw_lists,
            "actual_delta_uleb128_lists": self.actual_delta_lists,
        }


@dataclass
class Totals:
    list_count: int = 0
    ref_count: int = 0
    raw_v8_bytes: int = 0
    delta_candidate_bytes: int = 0
    selected_v9_bytes: int = 0
    raw_selected_lists: int = 0
    delta_selected_lists: int = 0
    actual_encoded_postings_bytes: int = 0
    actual_raw_lists: int = 0
    actual_delta_lists: int = 0
    lengths: Counter[int] = field(default_factory=Counter)
    buckets: dict[str, BucketStats] = field(
        default_factory=lambda: {name: BucketStats() for name, _, _ in LENGTH_BUCKETS}
    )

    def add(
        self,
        ref_count: int,
        raw_bytes: int,
        delta_bytes: int,
        *,
        actual_bytes: int | None = None,
        actual_codec: str = "raw32",
    ) -> None:
        if actual_bytes is None:
            actual_bytes = raw_bytes
        if actual_codec not in ("raw32", "delta_uleb128"):
            raise ValueError(f"unknown actual codec {actual_codec!r}")
        codec = "delta_uleb128" if delta_bytes < raw_bytes else "raw32"
        self.list_count += 1
        self.ref_count += ref_count
        self.raw_v8_bytes += raw_bytes
        self.delta_candidate_bytes += delta_bytes
        self.selected_v9_bytes += min(raw_bytes, delta_bytes)
        self.lengths[ref_count] += 1
        if codec == "raw32":
            self.raw_selected_lists += 1
        else:
            self.delta_selected_lists += 1
        self.actual_encoded_postings_bytes += actual_bytes
        if actual_codec == "raw32":
            self.actual_raw_lists += 1
        else:
            self.actual_delta_lists += 1
        for name, minimum, maximum in LENGTH_BUCKETS:
            if ref_count >= minimum and (maximum is None or ref_count <= maximum):
                self.buckets[name].add(
                    ref_count,
                    raw_bytes,
                    delta_bytes,
                    codec,
                    actual_bytes,
                    actual_codec,
                )
                return
        raise AssertionError(f"no length bucket for {ref_count}")

    def merge(self, other: "Totals") -> None:
        for name in (
            "list_count",
            "ref_count",
            "raw_v8_bytes",
            "delta_candidate_bytes",
            "selected_v9_bytes",
            "raw_selected_lists",
            "delta_selected_lists",
            "actual_encoded_postings_bytes",
            "actual_raw_lists",
            "actual_delta_lists",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.lengths.update(other.lengths)
        for name in self.buckets:
            self.buckets[name].merge(other.buckets[name])

    def to_json(
        self,
        index_bytes: int,
        *,
        candidate_indexes_v9_bytes: int | None = None,
        raw_equivalent_indexes_bytes: int | None = None,
    ) -> dict[str, Any]:
        savings = self.raw_v8_bytes - self.selected_v9_bytes
        if candidate_indexes_v9_bytes is None:
            candidate_indexes_v9_bytes = index_bytes - savings
        if raw_equivalent_indexes_bytes is None:
            raw_equivalent_indexes_bytes = index_bytes
        return {
            "list_count": self.list_count,
            "ref_count": self.ref_count,
            "raw_v8_bytes": self.raw_v8_bytes,
            "delta_candidate_bytes": self.delta_candidate_bytes,
            "selected_v9_bytes": self.selected_v9_bytes,
            "savings_bytes": savings,
            "savings_basis_points": _basis_points(savings, self.raw_v8_bytes),
            "codec_selection": {
                "raw32_lists": self.raw_selected_lists,
                "delta_uleb128_lists": self.delta_selected_lists,
            },
            "actual_encoded_postings_bytes": self.actual_encoded_postings_bytes,
            "actual_codec_counts": {
                "raw32_lists": self.actual_raw_lists,
                "delta_uleb128_lists": self.actual_delta_lists,
            },
            "current_indexes_bytes": index_bytes,
            "raw_equivalent_indexes_bytes": raw_equivalent_indexes_bytes,
            "candidate_indexes_v9_bytes": candidate_indexes_v9_bytes,
            "index_savings_basis_points": _basis_points(
                savings, raw_equivalent_indexes_bytes
            ),
            "ref_count_quantiles_nearest_rank": _quantiles(self.lengths),
            "ref_count_distribution": [
                self.buckets[name].to_json(name, minimum, maximum)
                for name, minimum, maximum in LENGTH_BUCKETS
            ],
        }


@dataclass(frozen=True)
class BoundRoot:
    count: int
    root_length: int
    root_crc32c: int
    root_sha256: str


@dataclass
class SegmentResult:
    name: str
    indexes_bytes: int
    indexes_sha256: str
    series_root: BoundRoot
    symbols_root: BoundRoot
    totals: Totals
    index_version: int

    def to_json(self) -> dict[str, Any]:
        savings = self.totals.raw_v8_bytes - self.totals.selected_v9_bytes
        if self.index_version == INDEX_VERSION:
            candidate_indexes_v9_bytes = self.indexes_bytes - savings
            raw_equivalent_indexes_bytes = self.indexes_bytes
        else:
            candidate_indexes_v9_bytes = self.indexes_bytes
            raw_equivalent_indexes_bytes = self.indexes_bytes + savings
        return {
            "segment": self.name,
            "index_container_version": self.index_version,
            "indexes_puffin_sha256": self.indexes_sha256,
            "bound_roots": {
                "series": _root_json(self.series_root),
                "symbols": _root_json(self.symbols_root),
            },
            "totals": self.totals.to_json(
                self.indexes_bytes,
                candidate_indexes_v9_bytes=candidate_indexes_v9_bytes,
                raw_equivalent_indexes_bytes=raw_equivalent_indexes_bytes,
            ),
        }


def _root_json(root: BoundRoot) -> dict[str, Any]:
    return {
        "count": root.count,
        "integrity_checked_root_bytes": root.root_length,
        "root_crc32c": f"{root.root_crc32c:08x}",
        "root_sha256": root.root_sha256,
    }


def _basis_points(numerator: int, denominator: int) -> int:
    return 0 if denominator == 0 else numerator * 10_000 // denominator


def _quantiles(lengths: Counter[int]) -> dict[str, int | None]:
    total = sum(lengths.values())
    if total == 0:
        return {name: None for name in ("p50", "p90", "p95", "p99", "maximum")}
    ordered = sorted(lengths.items())
    output: dict[str, int | None] = {}
    for name, numerator in (("p50", 50), ("p90", 90), ("p95", 95), ("p99", 99)):
        rank = (total * numerator + 99) // 100
        cumulative = 0
        for value, count in ordered:
            cumulative += count
            if cumulative >= rank:
                output[name] = value
                break
    output["maximum"] = ordered[-1][0]
    return output


def _u16(value: bytes | bytearray | mmap.mmap, offset: int) -> int:
    return struct.unpack_from("<H", value, offset)[0]


def _u32(value: bytes | bytearray | mmap.mmap, offset: int) -> int:
    return struct.unpack_from("<I", value, offset)[0]


def _u64(value: bytes | bytearray | mmap.mmap, offset: int) -> int:
    return struct.unpack_from("<Q", value, offset)[0]


def _locator(value: bytes | mmap.mmap, offset: int) -> Locator:
    return Locator(_u64(value, offset), _u64(value, offset + 8))


def _build_crc32c_table() -> tuple[int, ...]:
    table = []
    for entry in range(256):
        crc = entry
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F6_3B78 if crc & 1 else 0)
        table.append(crc)
    return tuple(table)


_CRC32C_TABLE = _build_crc32c_table()


def _crc32c(value: bytes | bytearray | memoryview) -> int:
    """Portable reflected Castagnoli CRC-32C used by tests and small roots."""

    crc = 0xFFFF_FFFF
    for byte in value:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFF_FFFF


def _crc32c_with_zeroed_u32(value: bytes, offset: int) -> int:
    if offset < 0 or offset + 4 > len(value):
        raise ValueError("CRC field lies outside input")
    scratch = bytearray(value)
    scratch[offset : offset + 4] = b"\0\0\0\0"
    return _crc32c(scratch)


class _Crc32cSpans:
    """CRC mmap spans, using Abseil when its compatible system ABI exists."""

    _ABSL_SYMBOL = (
        "_ZN4absl7debian912crc_internal20ExtendCrc32cInternal"
        "ENS0_8crc32c_tESt17basic_string_viewIcSt11char_traitsIcEE"
    )

    def __init__(self) -> None:
        self.backend = "python-table"
        self._function: Any | None = None
        library_name = ctypes.util.find_library("absl_crc32c")
        if library_name is None:
            return
        try:
            library = ctypes.CDLL(library_name)
            function = getattr(library, self._ABSL_SYMBOL)
            # libstdc++ string_view is passed as (length, pointer) on this ABI.
            function.argtypes = [ctypes.c_uint32, ctypes.c_size_t, ctypes.c_void_p]
            function.restype = ctypes.c_uint32
            probe = ctypes.create_string_buffer(b"123456789")
            if function(0, 9, ctypes.addressof(probe)) != 0xE306_9283:
                return
        except (AttributeError, OSError):
            return
        self.backend = f"absl-crc32c:{library_name}"
        self._function = function
        self._library = library

    def span(self, source: mmap.mmap, base_address: int, offset: int, length: int) -> int:
        if offset < 0 or length < 0 or offset + length > len(source):
            raise ValueError("CRC span lies outside mapping")
        if self._function is not None:
            return int(self._function(0, length, base_address + offset))
        return _crc32c(memoryview(source)[offset : offset + length])


def _error(path: Path, message: str) -> InventoryError:
    return InventoryError(f"{path}: {message}")


def _regular_file(path: Path) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise _error(path, "required file does not exist") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise _error(path, "expected a regular file; symbolic links are rejected")
    return metadata.st_size


def _read_exact_at(source: BinaryIO, path: Path, offset: int, length: int, what: str) -> bytes:
    source.seek(offset)
    value = source.read(length)
    if len(value) != length:
        raise _error(path, f"truncated {what}: expected {length} bytes, got {len(value)}")
    return value


def _align_up(value: int, alignment: int) -> int:
    return ((value + alignment - 1) // alignment) * alignment


def _validate_series_root(path: Path) -> BoundRoot:
    file_len = _regular_file(path)
    if file_len < SERIES_HEADER_LEN:
        raise _error(path, "series v3 file is shorter than its fixed header")
    with path.open("rb") as source:
        header = _read_exact_at(source, path, 0, SERIES_HEADER_LEN, "series v3 header")
        if _u32(header, 0) != SERIES_MAGIC or _u16(header, 4) != SERIES_VERSION:
            raise _error(path, "expected series.bin v3")
        fixed = (
            (_u16(header, 6), 0, "flags"),
            (_u32(header, 8), SERIES_HEADER_LEN, "header length"),
            (_u32(header, 12), SERIES_DESCRIPTOR_LEN, "descriptor length"),
            (_u32(header, 16), SERIES_HOT_PAGE_LEN, "hot-page length"),
            (_u32(header, 20), SERIES_HOT_PAGE_HEADER_LEN, "hot-page header length"),
            (_u32(header, 24), SERIES_HOT_RECORD_LEN, "hot-record length"),
            (_u32(header, 28), SERIES_RECORDS_PER_PAGE, "records per page"),
            (_u32(header, 56), SERIES_COLD_PAGE_LEN, "cold-page length"),
        )
        for actual, expected, name in fixed:
            if actual != expected:
                raise _error(path, f"series v3 {name} is invalid")

        series_count = _u32(header, 32)
        page_count = _u32(header, 36)
        keyset_count = _u32(header, 40)
        value_dict_count = _u32(header, 44)
        cold_page_count = _u32(header, 60)
        directory_offset, directory_len = _u64(header, 64), _u64(header, 72)
        hot_pages_offset, hot_pages_len = _u64(header, 80), _u64(header, 88)
        keysets_offset, keysets_len = _u64(header, 96), _u64(header, 104)
        values_offset, values_len = _u64(header, 112), _u64(header, 120)
        blocks_offset, blocks_len = _u64(header, 128), _u64(header, 136)
        segment_start, segment_end = _u64(header, 144), _u64(header, 152)
        chunk_index_len, recorded_file_len = _u64(header, 160), _u64(header, 168)
        cold_bytes = keysets_len + values_len + blocks_len
        expected_pages = (series_count + SERIES_RECORDS_PER_PAGE - 1) // SERIES_RECORDS_PER_PAGE
        expected_cold_pages = (cold_bytes + SERIES_COLD_PAGE_LEN - 1) // SERIES_COLD_PAGE_LEN
        expected_directory_len = (expected_pages + expected_cold_pages) * SERIES_DESCRIPTOR_LEN
        expected_hot_offset = _align_up(
            SERIES_HEADER_LEN + expected_directory_len, SERIES_ROOT_ALIGNMENT
        )
        expected_hot_len = expected_pages * SERIES_HOT_PAGE_LEN
        expected_keysets = expected_hot_offset + expected_hot_len
        expected_values = expected_keysets + keysets_len
        expected_blocks = expected_values + values_len
        expected_file_len = expected_blocks + blocks_len
        expected = (
            (page_count, expected_pages, "page count"),
            (cold_page_count, expected_cold_pages, "cold-page count"),
            (directory_offset, SERIES_HEADER_LEN, "directory offset"),
            (directory_len, expected_directory_len, "directory length"),
            (hot_pages_offset, expected_hot_offset, "hot-pages offset"),
            (hot_pages_len, expected_hot_len, "hot-pages length"),
            (keysets_offset, expected_keysets, "keysets offset"),
            (values_offset, expected_values, "value-dictionaries offset"),
            (blocks_offset, expected_blocks, "keyset-blocks offset"),
            (recorded_file_len, expected_file_len, "derived file length"),
            (recorded_file_len, file_len, "recorded file length"),
        )
        for actual, wanted, name in expected:
            if actual != wanted:
                raise _error(path, f"series v3 {name} is noncanonical")
        if segment_start >= segment_end or chunk_index_len < 64:
            raise _error(path, "series v3 segment/chunk-index bounds are invalid")
        if series_count and (keyset_count == 0 or keyset_count > series_count):
            raise _error(path, "series v3 keyset count is invalid")
        for section_len, entry_count, name in (
            (keysets_len, keyset_count, "keysets"),
            (values_len, value_dict_count, "value dictionaries"),
            (blocks_len, keyset_count, "keyset blocks"),
        ):
            if section_len < (entry_count + 1) * 8:
                raise _error(path, f"series v3 {name} section is shorter than its offset table")

        root = _read_exact_at(source, path, 0, hot_pages_offset, "series v3 root")
    stored_crc = _u32(root, SERIES_ROOT_CRC_OFFSET)
    if _crc32c_with_zeroed_u32(root, SERIES_ROOT_CRC_OFFSET) != stored_crc:
        raise _error(path, "series v3 root CRC mismatch")
    descriptor_end = SERIES_HEADER_LEN + expected_directory_len
    if any(root[descriptor_end:]):
        raise _error(path, "series v3 root alignment padding is non-zero")
    cursor = SERIES_HEADER_LEN
    for page_index in range(page_count):
        descriptor = root[cursor : cursor + SERIES_DESCRIPTOR_LEN]
        expected_first = page_index * SERIES_RECORDS_PER_PAGE
        expected_count = min(SERIES_RECORDS_PER_PAGE, series_count - expected_first)
        if (
            _u32(descriptor, 0) != expected_first
            or _u32(descriptor, 4) != expected_count
            or _u32(descriptor, 12) != 0
        ):
            raise _error(path, "series v3 hot-page descriptor is noncanonical")
        cursor += SERIES_DESCRIPTOR_LEN
    for page_index in range(cold_page_count):
        descriptor = root[cursor : cursor + SERIES_DESCRIPTOR_LEN]
        remaining = cold_bytes - page_index * SERIES_COLD_PAGE_LEN
        expected_len = min(SERIES_COLD_PAGE_LEN, remaining)
        if (
            _u32(descriptor, 0) != page_index
            or _u32(descriptor, 4) != expected_len
            or _u32(descriptor, 12) != 0
        ):
            raise _error(path, "series v3 cold-page descriptor is noncanonical")
        cursor += SERIES_DESCRIPTOR_LEN
    return BoundRoot(series_count, len(root), stored_crc, hashlib.sha256(root).hexdigest())


def _validate_symbols_root(path: Path) -> BoundRoot:
    file_len = _regular_file(path)
    if file_len < SYMBOLS_HEADER_LEN:
        raise _error(path, "symbols v3 file is shorter than its fixed header")
    with path.open("rb") as source:
        header = _read_exact_at(source, path, 0, SYMBOLS_HEADER_LEN, "symbols v3 header")
        if _u32(header, 0) != SYMBOLS_MAGIC or _u16(header, 4) != SYMBOLS_VERSION:
            raise _error(path, "expected symbols.bin v3")
        if (
            _u16(header, 6) != 0
            or _u32(header, 8) != SYMBOLS_HEADER_LEN
            or _u32(header, 12) != SYMBOLS_PAGE_DESCRIPTOR_LEN
            or _u32(header, 76) != 0
        ):
            raise _error(path, "symbols v3 fixed header is invalid")
        symbol_count, page_count = _u32(header, 16), _u32(header, 20)
        if (symbol_count == 0) != (page_count == 0) or page_count > symbol_count:
            raise _error(path, "symbols v3 symbol/page counts are invalid")
        directory_len = page_count * SYMBOLS_PAGE_DESCRIPTOR_LEN
        fence_offset = SYMBOLS_HEADER_LEN + directory_len
        fence_len = _u64(header, 48)
        pages_offset = fence_offset + fence_len
        if (
            _u64(header, 24) != SYMBOLS_HEADER_LEN
            or _u64(header, 32) != directory_len
            or _u64(header, 40) != fence_offset
            or _u64(header, 56) != pages_offset
            or _u64(header, 64) != file_len
            or pages_offset > file_len
        ):
            raise _error(path, "symbols v3 root layout is noncanonical")
        root = _read_exact_at(source, path, 0, pages_offset, "symbols v3 root")
    stored_crc = _u32(root, SYMBOLS_ROOT_CRC_OFFSET)
    if _crc32c_with_zeroed_u32(root, SYMBOLS_ROOT_CRC_OFFSET) != stored_crc:
        raise _error(path, "symbols v3 root CRC mismatch")

    fences = root[fence_offset:pages_offset]
    expected_symbol_id = 0
    expected_page_offset = pages_offset
    expected_fence_offset = 0
    previous_last_fence: bytes | None = None
    for page_index in range(page_count):
        start = SYMBOLS_HEADER_LEN + page_index * SYMBOLS_PAGE_DESCRIPTOR_LEN
        descriptor = root[start : start + SYMBOLS_PAGE_DESCRIPTOR_LEN]
        first_id, count = _u32(descriptor, 0), _u32(descriptor, 4)
        page_offset, page_len = _u64(descriptor, 8), _u32(descriptor, 16)
        strings_len = _u32(descriptor, 40)
        if count == 0 or first_id != expected_symbol_id:
            raise _error(path, "symbols v3 symbol IDs are not contiguous")
        expected_symbol_id += count
        expected_page_len = SYMBOLS_PAGE_HEADER_LEN + (count + 1) * 4 + strings_len
        if page_offset != expected_page_offset or page_len != expected_page_len:
            raise _error(path, "symbols v3 page ranges are noncanonical")
        expected_page_offset += page_len
        if expected_page_offset > file_len or _u32(descriptor, 44) != 0:
            raise _error(path, "symbols v3 page descriptor is out of bounds")
        first_offset, first_len = _u32(descriptor, 24), _u32(descriptor, 28)
        last_offset, last_len = _u32(descriptor, 32), _u32(descriptor, 36)
        if first_offset != expected_fence_offset:
            raise _error(path, "symbols v3 first fence is noncanonical")
        expected_fence_offset += first_len
        if last_offset != expected_fence_offset:
            raise _error(path, "symbols v3 last fence is noncanonical")
        expected_fence_offset += last_len
        if expected_fence_offset > len(fences):
            raise _error(path, "symbols v3 fence lies outside the root")
        first_fence = fences[first_offset : first_offset + first_len]
        last_fence = fences[last_offset : last_offset + last_len]
        if (count == 1 and first_fence != last_fence) or (
            count > 1 and first_fence >= last_fence
        ):
            raise _error(path, "symbols v3 page fences are invalid")
        if previous_last_fence is not None and previous_last_fence >= first_fence:
            raise _error(path, "symbols v3 page fences are not strictly ordered")
        previous_last_fence = last_fence
    if (
        expected_symbol_id != symbol_count
        or expected_page_offset != file_len
        or expected_fence_offset != len(fences)
    ):
        raise _error(path, "symbols v3 root totals are inconsistent")
    return BoundRoot(symbol_count, len(root), stored_crc, hashlib.sha256(root).hexdigest())


def _validate_locator(
    locator: Locator,
    required: bool,
    name: str,
    path: Path,
    format_label: str,
) -> None:
    if (locator.offset == 0) != (locator.length == 0):
        raise _error(path, f"{format_label} {name} locator is half-empty")
    if required and locator.empty:
        raise _error(path, f"{format_label} required {name} locator is absent")


def _validate_auxiliary_directory(
    mapping: mmap.mmap,
    path: Path,
    locator: Locator,
    entry_count: int,
    root_crc: int,
    format_label: str,
) -> None:
    expected_len = AUXILIARY_DIRECTORY_HEADER_LEN + entry_count * AUXILIARY_RECORD_LEN
    if locator.length != expected_len:
        raise _error(path, f"{format_label} auxiliary-directory length is inconsistent")
    value = mapping[locator.offset : locator.end]
    if _u32(value, 40) != root_crc or _crc32c_with_zeroed_u32(value, 40) != root_crc:
        raise _error(path, f"{format_label} auxiliary-directory CRC mismatch")
    if (
        _u32(value, 0) != AUXILIARY_DIRECTORY_MAGIC
        or _u16(value, 4) != AUXILIARY_DIRECTORY_VERSION
        or _u16(value, 6) != 0
        or _u32(value, 8) != AUXILIARY_DIRECTORY_HEADER_LEN
        or _u32(value, 12) != AUXILIARY_RECORD_LEN
        or _u64(value, 16) != entry_count
        or _u64(value, 24) != AUXILIARY_DIRECTORY_HEADER_LEN
        or _u64(value, 32) != entry_count * AUXILIARY_RECORD_LEN
        or any(value[44:AUXILIARY_DIRECTORY_HEADER_LEN])
    ):
        raise _error(path, f"{format_label} auxiliary-directory header is invalid")


def _uleb128_len(value: int) -> int:
    if value < 0 or value > 0xFFFF_FFFF:
        raise ValueError("value does not fit u32")
    return max(1, (value.bit_length() + 6) // 7)


def _delta_body_len_python(mapping: mmap.mmap, offset: int, count: int, series_count: int) -> int:
    previous: int | None = None
    encoded = 0
    body = mapping[offset : offset + count * 4]
    for (series_ref,) in struct.iter_unpack("<I", body):
        if series_ref >= series_count:
            raise ValueError("series ref exceeds the bound series count")
        if previous is not None and series_ref <= previous:
            raise ValueError("series refs are not strictly ordered and unique")
        encoded += _uleb128_len(series_ref if previous is None else series_ref - previous)
        previous = series_ref
    return encoded


def _delta_body_len_numpy(mapping: mmap.mmap, offset: int, count: int, series_count: int) -> int:
    refs = _numpy.frombuffer(mapping, dtype="<u4", count=count, offset=offset)
    try:
        if int(refs[-1]) >= series_count:
            raise ValueError("series ref exceeds the bound series count")
        if bool(_numpy.any(refs[1:] <= refs[:-1])):
            raise ValueError("series refs are not strictly ordered and unique")
        gaps = _numpy.diff(refs)
        try:
            encoded = _uleb128_len(int(refs[0])) + int(gaps.size)
            for threshold in (0x7F, 0x3FFF, 0x1F_FFFF, 0x0FFF_FFFF):
                encoded += int(_numpy.count_nonzero(gaps > threshold))
            return encoded
        finally:
            del gaps
    finally:
        del refs


def _delta_body_len(mapping: mmap.mmap, offset: int, count: int, series_count: int) -> int:
    if _numpy is not None and count >= NUMPY_THRESHOLD_REFS:
        return _delta_body_len_numpy(mapping, offset, count, series_count)
    return _delta_body_len_python(mapping, offset, count, series_count)


def _hash_mapping_span(
    digest: Any,
    mapping: mmap.mmap,
    offset: int,
    length: int,
) -> None:
    end = offset + length
    while offset < end:
        next_offset = min(offset + HASH_CHUNK_BYTES, end)
        digest.update(mapping[offset:next_offset])
        offset = next_offset


def _begin_logical_segment(digest: Any, segment_name: str, list_count: int) -> None:
    encoded_name = segment_name.encode("utf-8")
    digest.update(struct.pack("<Q", len(encoded_name)))
    digest.update(encoded_name)
    digest.update(struct.pack("<Q", list_count))


def _begin_logical_list(
    digest: Any,
    key: tuple[int, int],
    ref_count: int,
) -> None:
    digest.update(struct.pack("<III", key[0], key[1], ref_count))


def _decode_canonical_uleb128_u32(
    mapping: mmap.mmap,
    cursor: int,
    end: int,
) -> tuple[int, int]:
    start = cursor
    value = 0
    for index in range(5):
        if cursor >= end:
            raise ValueError("varint is truncated")
        byte = mapping[cursor]
        cursor += 1
        if index == 4 and byte & 0xF0:
            raise ValueError("varint exceeds u32")
        value |= (byte & 0x7F) << (index * 7)
        if byte & 0x80 == 0:
            if cursor - start != _uleb128_len(value):
                raise ValueError("varint is not canonically encoded")
            return value, cursor
    raise ValueError("varint exceeds u32")


def _decode_delta_and_hash(
    mapping: mmap.mmap,
    offset: int,
    end: int,
    count: int,
    series_count: int,
    digest: Any,
) -> int:
    cursor = offset
    previous: int | None = None
    decoded_refs = array("I")
    for index in range(count):
        if cursor >= end:
            raise ValueError("varint is truncated")
        first_byte = mapping[cursor]
        if first_byte < 0x80:
            value = first_byte
            cursor += 1
        else:
            value, cursor = _decode_canonical_uleb128_u32(mapping, cursor, end)
        if index == 0:
            series_ref = value
        else:
            if value == 0:
                raise ValueError("delta gap is zero")
            assert previous is not None
            series_ref = previous + value
            if series_ref > 0xFFFF_FFFF:
                raise ValueError("delta addition overflows")
        if series_ref >= series_count:
            raise ValueError("series ref exceeds the bound series count")
        if previous is not None and series_ref <= previous:
            raise ValueError("series refs are not strictly ordered and unique")
        decoded_refs.append(series_ref)
        previous = series_ref
    if cursor != end:
        raise ValueError("delta body has trailing bytes")
    if sys.byteorder != "little":  # pragma: no cover - corpus hosts are little-endian.
        decoded_refs.byteswap()
    digest.update(decoded_refs)
    return cursor - offset


def _parse_index(
    path: Path,
    series_root: BoundRoot,
    symbols_root: BoundRoot,
    crc_spans: _Crc32cSpans,
    segment_name: str,
    logical_digest: Any,
) -> tuple[Totals, str, int, int]:
    file_len = _regular_file(path)
    if file_len < INDEX_HEADER_LEN + INDEX_TRAILER_LEN:
        raise _error(path, "index is shorter than its fixed root")
    with path.open("rb") as source:
        mapping = mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_COPY)
        try:
            base_address = ctypes.addressof(ctypes.c_char.from_buffer(mapping))
            header = mapping[:INDEX_HEADER_LEN]
            trailer_offset = file_len - INDEX_TRAILER_LEN
            trailer = mapping[trailer_offset:]
            version = _u16(header, 4)
            index_format = INDEX_FORMATS.get(version)
            if index_format is None:
                raise _error(path, f"unsupported indexes.puffin version {version}")
            label = index_format.label
            if (
                _u32(header, 0) != INDEX_MAGIC
                or _u16(header, 6) != 0
                or _u32(header, 8) != INDEX_HEADER_LEN
                or _u32(header, 12) != 0
            ):
                raise _error(path, f"expected canonical indexes.puffin {label} header")
            if (
                _u32(trailer, 0) != INDEX_TRAILER_MAGIC
                or _u16(trailer, 4) != version
                or _u16(trailer, 6) != 0
                or _u32(trailer, 8) != INDEX_TRAILER_LEN
                or _u32(trailer, 12) != 0
                or _u64(trailer, TRAILER_FILE_LEN_OFFSET) != file_len
                or _u32(trailer, TRAILER_TERMINAL_MAGIC_OFFSET)
                != index_format.terminal_magic
                or any(trailer[TRAILER_RESERVED_OFFSET:TRAILER_TERMINAL_MAGIC_OFFSET])
            ):
                raise _error(path, f"{label} fixed trailer is invalid")
            stored_trailer_crc = _u32(trailer, TRAILER_CRC_OFFSET)
            if _crc32c_with_zeroed_u32(trailer, TRAILER_CRC_OFFSET) != stored_trailer_crc:
                raise _error(path, f"{label} trailer CRC mismatch")
            series_count = _u32(trailer, TRAILER_SERIES_COUNT_OFFSET)
            symbol_count = _u32(trailer, TRAILER_SYMBOL_COUNT_OFFSET)
            if series_count != series_root.count or symbol_count != symbols_root.count:
                raise _error(
                    path, f"{label} counts do not match integrity-checked series/symbol roots"
                )

            exact_entry_count = _u64(trailer, TRAILER_EXACT_ENTRY_COUNT_OFFSET)
            exact_page_count = _u32(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET)
            expected_page_count = (
                exact_entry_count + EXACT_RECORDS_PER_PAGE - 1
            ) // EXACT_RECORDS_PER_PAGE
            auxiliary_entry_count = _u32(trailer, TRAILER_AUX_ENTRY_COUNT_OFFSET)
            if (
                exact_page_count != expected_page_count
                or _u32(trailer, TRAILER_EXACT_RECORD_LEN_OFFSET) != EXACT_RECORD_LEN
                or _u32(trailer, TRAILER_EXACT_PAGE_LEN_OFFSET) != EXACT_PAGE_LEN
            ):
                raise _error(path, f"{label} exact-page root counts are inconsistent")

            _begin_logical_segment(logical_digest, segment_name, exact_entry_count)

            regions = (
                ("routing", _locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET), False),
                ("metric ranges", _locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET), True),
                (
                    "exact postings",
                    _locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET),
                    False,
                ),
                (
                    "auxiliary payloads",
                    _locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET),
                    False,
                ),
                (
                    "exact directory",
                    _locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET),
                    True,
                ),
                ("exact pages", _locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET), False),
                (
                    "auxiliary directory",
                    _locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET),
                    True,
                ),
            )
            for name, locator, required in regions:
                _validate_locator(locator, required, name, path, label)
            by_name = {name: locator for name, locator, _ in regions}
            if by_name["exact postings"].empty != (exact_entry_count == 0):
                raise _error(
                    path, f"{label} exact-postings presence disagrees with entry count"
                )
            if by_name["exact pages"].empty != (exact_page_count == 0):
                raise _error(path, f"{label} exact-pages presence disagrees with page count")
            if by_name["auxiliary payloads"].empty != (auxiliary_entry_count == 0):
                raise _error(
                    path, f"{label} auxiliary-payload presence disagrees with entry count"
                )
            expected_offset = INDEX_HEADER_LEN
            for name, locator, _ in regions:
                if locator.empty:
                    continue
                if locator.offset != expected_offset or locator.end > trailer_offset:
                    raise _error(path, f"{label} {name} region is not canonically adjacent")
                expected_offset = locator.end
            if expected_offset != trailer_offset:
                raise _error(path, f"{label} final region is not adjacent to the trailer")

            exact_directory = by_name["exact directory"]
            expected_directory_len = (
                EXACT_DIRECTORY_HEADER_LEN
                + exact_page_count * EXACT_PAGE_DESCRIPTOR_LEN
            )
            if exact_directory.length != expected_directory_len:
                raise _error(path, f"{label} exact-directory length is inconsistent")
            exact_pages = by_name["exact pages"]
            if exact_pages.length != exact_page_count * EXACT_PAGE_LEN:
                raise _error(path, f"{label} exact-pages length is inconsistent")
            directory = mapping[exact_directory.offset : exact_directory.end]
            root_directory_crc = _u32(trailer, TRAILER_EXACT_DIRECTORY_CRC_OFFSET)
            if (
                _u32(directory, 56) != root_directory_crc
                or _crc32c_with_zeroed_u32(directory, 56) != root_directory_crc
            ):
                raise _error(path, f"{label} exact-directory CRC mismatch")
            if (
                _u32(directory, 0) != index_format.exact_directory_magic
                or _u16(directory, 4) != index_format.exact_directory_version
                or _u16(directory, 6) != 0
                or _u32(directory, 8) != EXACT_DIRECTORY_HEADER_LEN
                or _u32(directory, 12) != EXACT_PAGE_DESCRIPTOR_LEN
                or _u32(directory, 16) != EXACT_PAGE_LEN
                or _u32(directory, 20) != EXACT_RECORD_LEN
                or _u64(directory, 24) != exact_entry_count
                or _u32(directory, 32) != exact_page_count
                or _u32(directory, 36) != EXACT_RECORDS_PER_PAGE
                or _u64(directory, 40) != EXACT_DIRECTORY_HEADER_LEN
                or _u64(directory, 48) != exact_page_count * EXACT_PAGE_DESCRIPTOR_LEN
                or _u32(directory, 60) != 0
            ):
                raise _error(path, f"{label} exact-directory header is invalid")

            descriptors: list[tuple[tuple[int, int], tuple[int, int], int, int]] = []
            previous_last_key: tuple[int, int] | None = None
            decoded_entries = 0
            for page_index in range(exact_page_count):
                start = EXACT_DIRECTORY_HEADER_LEN + page_index * EXACT_PAGE_DESCRIPTOR_LEN
                descriptor = directory[start : start + EXACT_PAGE_DESCRIPTOR_LEN]
                first_key = (_u32(descriptor, 0), _u32(descriptor, 4))
                last_key = (_u32(descriptor, 8), _u32(descriptor, 12))
                record_count = _u32(descriptor, 16)
                remaining = exact_entry_count - decoded_entries
                expected_count = min(EXACT_RECORDS_PER_PAGE, remaining)
                if (
                    first_key[0] >= symbol_count
                    or first_key[1] >= symbol_count
                    or last_key[0] >= symbol_count
                    or last_key[1] >= symbol_count
                    or first_key > last_key
                    or (previous_last_key is not None and previous_last_key >= first_key)
                    or record_count != expected_count
                    or _u32(descriptor, 20) != 0
                    or _u32(descriptor, 28) != 0
                ):
                    raise _error(path, f"{label} exact-page descriptor is noncanonical")
                descriptors.append((first_key, last_key, record_count, _u32(descriptor, 24)))
                decoded_entries += record_count
                previous_last_key = last_key
            if decoded_entries != exact_entry_count:
                raise _error(path, f"{label} exact-directory counts disagree with the root")

            _validate_auxiliary_directory(
                mapping,
                path,
                by_name["auxiliary directory"],
                auxiliary_entry_count,
                _u32(trailer, TRAILER_AUX_DIRECTORY_CRC_OFFSET),
                label,
            )

            postings = by_name["exact postings"]
            expected_postings_offset = postings.offset
            totals = Totals()
            previous_key: tuple[int, int] | None = None
            for page_index, (first_key, last_key, record_count, page_crc) in enumerate(descriptors):
                page_offset = exact_pages.offset + page_index * EXACT_PAGE_LEN
                if crc_spans.span(mapping, base_address, page_offset, EXACT_PAGE_LEN) != page_crc:
                    raise _error(path, f"{label} exact page {page_index} CRC mismatch")
                if (
                    _u32(mapping, page_offset) != index_format.exact_page_magic
                    or _u16(mapping, page_offset + 4) != index_format.exact_page_version
                    or _u16(mapping, page_offset + 6) != 0
                    or _u32(mapping, page_offset + 8) != page_index
                    or _u32(mapping, page_offset + 12) != record_count
                ):
                    raise _error(path, f"{label} exact page {page_index} header is invalid")
                records_end = EXACT_PAGE_HEADER_LEN + record_count * EXACT_RECORD_LEN
                if any(mapping[page_offset + records_end : page_offset + EXACT_PAGE_LEN]):
                    raise _error(path, f"{label} exact page {page_index} padding is non-zero")
                page_first: tuple[int, int] | None = None
                page_last: tuple[int, int] | None = None
                for record_index in range(record_count):
                    record_offset = (
                        page_offset + EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN
                    )
                    key = (_u32(mapping, record_offset), _u32(mapping, record_offset + 4))
                    payload = Locator(
                        _u64(mapping, record_offset + 8),
                        _u64(mapping, record_offset + 16),
                    )
                    minimum_time, maximum_time = (
                        _u64(mapping, record_offset + 24),
                        _u64(mapping, record_offset + 32),
                    )
                    ref_count = _u32(mapping, record_offset + 40)
                    payload_crc = _u32(mapping, record_offset + 44)
                    raw_len = RAW_HEADER_BYTES + ref_count * 4
                    minimum_len = DELTA_HEADER_BYTES + ref_count
                    if (
                        key[0] >= symbol_count
                        or key[1] >= symbol_count
                        or (previous_key is not None and previous_key >= key)
                        or ref_count == 0
                        or ref_count > series_count
                        or (
                            version == INDEX_VERSION
                            and payload.length != raw_len
                        )
                        or (
                            version == INDEX_V9_VERSION
                            and not minimum_len <= payload.length <= raw_len
                        )
                        or payload.offset != expected_postings_offset
                        or payload.offset < postings.offset
                        or payload.end > postings.end
                        or minimum_time > maximum_time
                    ):
                        raise _error(
                            path, f"{label} exact record is noncanonical or out of bounds"
                        )
                    if (
                        crc_spans.span(
                            mapping, base_address, payload.offset, payload.length
                        )
                        != payload_crc
                    ):
                        raise _error(path, f"{label} exact postings CRC mismatch for key {key}")

                    _begin_logical_list(logical_digest, key, ref_count)
                    body_offset = payload.offset + RAW_HEADER_BYTES
                    try:
                        if version == INDEX_VERSION:
                            if _u32(mapping, payload.offset) != ref_count:
                                raise ValueError("count disagrees with the directory record")
                            delta_body_len = _delta_body_len(
                                mapping, body_offset, ref_count, series_count
                            )
                            _hash_mapping_span(
                                logical_digest, mapping, body_offset, ref_count * 4
                            )
                            actual_codec = "raw32"
                        else:
                            codec = mapping[payload.offset]
                            flags = mapping[payload.offset + 1]
                            reserved = _u16(mapping, payload.offset + 2)
                            if flags != 0 or reserved != 0:
                                raise ValueError(
                                    "header flags or reserved bytes are non-zero"
                                )
                            if codec == 0:
                                if payload.length != raw_len:
                                    raise ValueError("RAW32 payload length is inconsistent")
                                delta_body_len = _delta_body_len(
                                    mapping, body_offset, ref_count, series_count
                                )
                                _hash_mapping_span(
                                    logical_digest, mapping, body_offset, ref_count * 4
                                )
                                if DELTA_HEADER_BYTES + delta_body_len < raw_len:
                                    raise ValueError("RAW32 codec choice is noncanonical")
                                actual_codec = "raw32"
                            elif codec == 1:
                                delta_body_len = _decode_delta_and_hash(
                                    mapping,
                                    body_offset,
                                    payload.end,
                                    ref_count,
                                    series_count,
                                    logical_digest,
                                )
                                if DELTA_HEADER_BYTES + delta_body_len >= raw_len:
                                    raise ValueError("delta codec choice is noncanonical")
                                actual_codec = "delta_uleb128"
                            else:
                                raise ValueError("codec is unknown")
                    except ValueError as error:
                        raise _error(
                            path, f"{label} exact postings {error} for key {key}"
                        ) from error
                    delta_len = DELTA_HEADER_BYTES + delta_body_len
                    totals.add(
                        ref_count,
                        raw_len,
                        delta_len,
                        actual_bytes=payload.length,
                        actual_codec=actual_codec,
                    )
                    expected_postings_offset = payload.end
                    previous_key = key
                    page_first = key if page_first is None else page_first
                    page_last = key
                if page_first != first_key or page_last != last_key:
                    raise _error(path, f"{label} exact page {page_index} fences disagree")
            if expected_postings_offset != postings.end:
                raise _error(
                    path, f"{label} exact-postings records do not cover their root region"
                )
            index_sha256 = hashlib.sha256(mapping).hexdigest()
            return totals, index_sha256, file_len, version
        finally:
            mapping.close()


def _segment_directories(corpus: Path) -> list[Path]:
    try:
        metadata = corpus.lstat()
    except FileNotFoundError:
        raise _error(corpus, "corpus does not exist") from None
    if not stat.S_ISDIR(metadata.st_mode):
        raise _error(corpus, "corpus must be a directory; symbolic links are rejected")
    segments = []
    for path in corpus.iterdir():
        if not path.name.startswith("seg-"):
            continue
        child = path.lstat()
        if not stat.S_ISDIR(child.st_mode):
            raise _error(path, "selected segment path is not a directory")
        segments.append(path)
    segments.sort(key=lambda path: path.name)
    if not segments:
        raise _error(corpus, "corpus contains no seg-* directories")
    return segments


def _fingerprint(segments: Iterable[SegmentResult]) -> str:
    digest = hashlib.sha256()
    digest.update(FINGERPRINT_DOMAIN)
    values = list(segments)
    digest.update(struct.pack("<Q", len(values)))
    for segment in values:
        name = segment.name.encode("utf-8")
        digest.update(struct.pack("<Q", len(name)))
        digest.update(name)
        digest.update(struct.pack("<Q", segment.indexes_bytes))
        digest.update(bytes.fromhex(segment.indexes_sha256))
        for root in (segment.series_root, segment.symbols_root):
            digest.update(struct.pack("<Q", root.root_length))
            digest.update(bytes.fromhex(root.root_sha256))
    return digest.hexdigest()


def inventory_corpus(corpus: Path) -> dict[str, Any]:
    corpus = corpus.resolve(strict=True)
    crc_spans = _Crc32cSpans()
    segments: list[SegmentResult] = []
    aggregate = Totals()
    indexes_bytes = 0
    segment_directories = _segment_directories(corpus)
    logical_digest = hashlib.sha256()
    logical_digest.update(LOGICAL_FINGERPRINT_DOMAIN)
    logical_digest.update(struct.pack("<Q", len(segment_directories)))
    index_versions: set[int] = set()
    candidate_indexes_v9_bytes = 0
    raw_equivalent_indexes_bytes = 0
    for segment in segment_directories:
        series_root = _validate_series_root(segment / "series.bin")
        symbols_root = _validate_symbols_root(segment / "symbols.bin")
        totals, index_sha256, index_bytes, index_version = _parse_index(
            segment / "indexes.puffin",
            series_root,
            symbols_root,
            crc_spans,
            segment.name,
            logical_digest,
        )
        result = SegmentResult(
            segment.name,
            index_bytes,
            index_sha256,
            series_root,
            symbols_root,
            totals,
            index_version,
        )
        segments.append(result)
        aggregate.merge(totals)
        indexes_bytes += index_bytes
        savings = totals.raw_v8_bytes - totals.selected_v9_bytes
        if index_version == INDEX_VERSION:
            candidate_indexes_v9_bytes += index_bytes - savings
            raw_equivalent_indexes_bytes += index_bytes
        else:
            candidate_indexes_v9_bytes += index_bytes
            raw_equivalent_indexes_bytes += index_bytes + savings
        index_versions.add(index_version)
    source_formats = [
        f"indexes.puffin v{version}" for version in sorted(index_versions)
    ]
    source_format = (
        "footer schema 7 / indexes.puffin v8"
        if index_versions == {INDEX_VERSION}
        else "footer schema 8 / indexes.puffin v9"
        if index_versions == {INDEX_V9_VERSION}
        else "mixed indexes.puffin v8/v9 corpus"
    )
    return {
        "report_schema": REPORT_SCHEMA,
        "input": {
            "corpus_path": str(corpus),
            "segment_count": len(segments),
            "measurement_input_fingerprint": {
                "algorithm": "sha256",
                "domain": FINGERPRINT_DOMAIN.decode("ascii"),
                "digest": _fingerprint(segments),
                "scope": (
                    "ordered segment names, complete indexes.puffin bytes, and integrity-checked "
                    "series.bin/symbols.bin root bytes"
                ),
            },
            "decoded_exact_postings_logical_fingerprint": {
                "algorithm": "sha256",
                "domain": LOGICAL_FINGERPRINT_DOMAIN.decode("ascii"),
                "digest": logical_digest.hexdigest(),
                "scope": (
                    "ordered segment names and exact-list counts, then each canonical "
                    "(name_sym, value_sym, ref_count, decoded little-endian u32 refs)"
                ),
            },
            "integrity_checked_index_formats": source_formats,
        },
        "model": {
            "source_format": source_format,
            "candidate_format": "footer schema 8 / indexes.puffin v9",
            "candidate_payload_header_bytes": 4,
            "candidate_codecs": ["raw32", "delta_uleb128"],
            "codec_policy": "delta_uleb128 only when strictly smaller; raw32 wins ties",
            "delta_policy": "first reference absolute, then positive gaps; canonical uLEB128",
            "inventory_scope": "complete; no sampling",
        },
        "implementation": {
            "python": platform.python_version(),
            "numpy": None if _numpy is None else _numpy.__version__,
            "large_list_numpy_threshold_refs": NUMPY_THRESHOLD_REFS,
            "crc32c_backend": crc_spans.backend,
        },
        "aggregate": aggregate.to_json(
            indexes_bytes,
            candidate_indexes_v9_bytes=candidate_indexes_v9_bytes,
            raw_equivalent_indexes_bytes=raw_equivalent_indexes_bytes,
        ),
        "segments": [segment.to_json() for segment in segments],
    }


def _write_report(report: dict[str, Any], output: Path | None) -> None:
    if output is None:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as destination:
        json.dump(report, destination, indent=2, sort_keys=True)
        destination.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="new JSON path (exclusive create); omit to write JSON to stdout",
    )
    arguments = parser.parse_args(argv)
    try:
        report = inventory_corpus(arguments.corpus)
        _write_report(report, arguments.output)
    except (InventoryError, FileExistsError, OSError) as error:
        parser.exit(1, f"adaptive postings inventory failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
