#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

import adaptive_postings_inventory as inventory


def put_u16(value: bytearray, offset: int, item: int) -> None:
    struct.pack_into("<H", value, offset, item)


def put_u32(value: bytearray, offset: int, item: int) -> None:
    struct.pack_into("<I", value, offset, item)


def put_u64(value: bytearray, offset: int, item: int) -> None:
    struct.pack_into("<Q", value, offset, item)


def put_locator(value: bytearray, offset: int, item_offset: int, length: int) -> None:
    put_u64(value, offset, item_offset)
    put_u64(value, offset + 8, length)


def rewrite_crc32c(value: bytearray, offset: int) -> int:
    put_u32(value, offset, 0)
    checksum = inventory._crc32c(value)
    put_u32(value, offset, checksum)
    return checksum


def series_v3(series_count: int) -> bytes:
    keyset_count = value_count = 1
    keysets_len = values_len = blocks_len = 16
    cold_bytes = keysets_len + values_len + blocks_len
    page_count = (
        series_count + inventory.SERIES_RECORDS_PER_PAGE - 1
    ) // inventory.SERIES_RECORDS_PER_PAGE
    cold_page_count = 1
    directory_len = (
        page_count + cold_page_count
    ) * inventory.SERIES_DESCRIPTOR_LEN
    hot_offset = inventory._align_up(
        inventory.SERIES_HEADER_LEN + directory_len,
        inventory.SERIES_ROOT_ALIGNMENT,
    )
    hot_len = page_count * inventory.SERIES_HOT_PAGE_LEN
    keysets_offset = hot_offset + hot_len
    values_offset = keysets_offset + keysets_len
    blocks_offset = values_offset + values_len
    file_len = blocks_offset + blocks_len

    root = bytearray(hot_offset)
    put_u32(root, 0, inventory.SERIES_MAGIC)
    put_u16(root, 4, inventory.SERIES_VERSION)
    put_u32(root, 8, inventory.SERIES_HEADER_LEN)
    put_u32(root, 12, inventory.SERIES_DESCRIPTOR_LEN)
    put_u32(root, 16, inventory.SERIES_HOT_PAGE_LEN)
    put_u32(root, 20, inventory.SERIES_HOT_PAGE_HEADER_LEN)
    put_u32(root, 24, inventory.SERIES_HOT_RECORD_LEN)
    put_u32(root, 28, inventory.SERIES_RECORDS_PER_PAGE)
    put_u32(root, 32, series_count)
    put_u32(root, 36, page_count)
    put_u32(root, 40, keyset_count)
    put_u32(root, 44, value_count)
    put_u32(root, 56, inventory.SERIES_COLD_PAGE_LEN)
    put_u32(root, 60, cold_page_count)
    put_u64(root, 64, inventory.SERIES_HEADER_LEN)
    put_u64(root, 72, directory_len)
    put_u64(root, 80, hot_offset)
    put_u64(root, 88, hot_len)
    put_u64(root, 96, keysets_offset)
    put_u64(root, 104, keysets_len)
    put_u64(root, 112, values_offset)
    put_u64(root, 120, values_len)
    put_u64(root, 128, blocks_offset)
    put_u64(root, 136, blocks_len)
    put_u64(root, 144, 1)
    put_u64(root, 152, 2)
    put_u64(root, 160, 64)
    put_u64(root, 168, file_len)
    cursor = inventory.SERIES_HEADER_LEN
    for page_index in range(page_count):
        first = page_index * inventory.SERIES_RECORDS_PER_PAGE
        count = min(inventory.SERIES_RECORDS_PER_PAGE, series_count - first)
        put_u32(root, cursor, first)
        put_u32(root, cursor + 4, count)
        cursor += inventory.SERIES_DESCRIPTOR_LEN
    put_u32(root, cursor, 0)
    put_u32(root, cursor + 4, cold_bytes)
    rewrite_crc32c(root, inventory.SERIES_ROOT_CRC_OFFSET)
    return bytes(root) + bytes(file_len - len(root))


def symbols_v3(values: list[bytes]) -> bytes:
    strings = b"".join(values)
    offsets = [0]
    for value in values:
        offsets.append(offsets[-1] + len(value))
    offsets_bytes = struct.pack(f"<{len(offsets)}I", *offsets)
    page = bytearray(inventory.SYMBOLS_PAGE_HEADER_LEN + len(offsets_bytes) + len(strings))
    page[
        inventory.SYMBOLS_PAGE_HEADER_LEN : inventory.SYMBOLS_PAGE_HEADER_LEN
        + len(offsets_bytes)
    ] = offsets_bytes
    page[inventory.SYMBOLS_PAGE_HEADER_LEN + len(offsets_bytes) :] = strings
    first, last = values[0], values[-1]
    fences = first + last
    root_len = inventory.SYMBOLS_HEADER_LEN + inventory.SYMBOLS_PAGE_DESCRIPTOR_LEN + len(fences)
    root = bytearray(root_len)
    put_u32(root, 0, inventory.SYMBOLS_MAGIC)
    put_u16(root, 4, inventory.SYMBOLS_VERSION)
    put_u32(root, 8, inventory.SYMBOLS_HEADER_LEN)
    put_u32(root, 12, inventory.SYMBOLS_PAGE_DESCRIPTOR_LEN)
    put_u32(root, 16, len(values))
    put_u32(root, 20, 1)
    put_u64(root, 24, inventory.SYMBOLS_HEADER_LEN)
    put_u64(root, 32, inventory.SYMBOLS_PAGE_DESCRIPTOR_LEN)
    put_u64(root, 40, inventory.SYMBOLS_HEADER_LEN + inventory.SYMBOLS_PAGE_DESCRIPTOR_LEN)
    put_u64(root, 48, len(fences))
    put_u64(root, 56, root_len)
    put_u64(root, 64, root_len + len(page))
    descriptor = inventory.SYMBOLS_HEADER_LEN
    put_u32(root, descriptor, 0)
    put_u32(root, descriptor + 4, len(values))
    put_u64(root, descriptor + 8, root_len)
    put_u32(root, descriptor + 16, len(page))
    put_u32(root, descriptor + 24, 0)
    put_u32(root, descriptor + 28, len(first))
    put_u32(root, descriptor + 32, len(first))
    put_u32(root, descriptor + 36, len(last))
    put_u32(root, descriptor + 40, len(strings))
    root[-len(fences) :] = fences
    rewrite_crc32c(root, inventory.SYMBOLS_ROOT_CRC_OFFSET)
    return bytes(root + page)


def auxiliary_directory() -> tuple[bytes, int]:
    value = bytearray(inventory.AUXILIARY_DIRECTORY_HEADER_LEN)
    put_u32(value, 0, inventory.AUXILIARY_DIRECTORY_MAGIC)
    put_u16(value, 4, inventory.AUXILIARY_DIRECTORY_VERSION)
    put_u32(value, 8, inventory.AUXILIARY_DIRECTORY_HEADER_LEN)
    put_u32(value, 12, inventory.AUXILIARY_RECORD_LEN)
    put_u64(value, 24, inventory.AUXILIARY_DIRECTORY_HEADER_LEN)
    checksum = rewrite_crc32c(value, 40)
    return bytes(value), checksum


def encode_uleb128(value: int) -> bytes:
    output = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        output.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(output)


def adaptive_payload(refs: list[int]) -> bytes:
    raw = bytes((0, 0, 0, 0)) + struct.pack(f"<{len(refs)}I", *refs)
    previous = None
    delta_body = bytearray()
    for ref in refs:
        delta_body.extend(encode_uleb128(ref if previous is None else ref - previous))
        previous = ref
    delta = bytes((1, 0, 0, 0)) + bytes(delta_body)
    return delta if len(delta) < len(raw) else raw


def indexes(
    lists: list[tuple[tuple[int, int], list[int]]],
    series_count: int,
    symbol_count: int,
    version: int,
    encoded_payloads: list[bytes] | None = None,
) -> bytes:
    index_format = inventory.INDEX_FORMATS[version]
    payloads = encoded_payloads
    if payloads is None:
        if version == inventory.INDEX_VERSION:
            payloads = [
                struct.pack(f"<{len(refs) + 1}I", len(refs), *refs)
                for _, refs in lists
            ]
        else:
            payloads = [adaptive_payload(refs) for _, refs in lists]
    if len(payloads) != len(lists):
        raise ValueError("payload count does not match list count")
    postings = b"".join(payloads)
    page_count = (
        len(lists) + inventory.EXACT_RECORDS_PER_PAGE - 1
    ) // inventory.EXACT_RECORDS_PER_PAGE
    directory_len = (
        inventory.EXACT_DIRECTORY_HEADER_LEN
        + page_count * inventory.EXACT_PAGE_DESCRIPTOR_LEN
    )
    metric = b"M"
    postings_offset = inventory.INDEX_HEADER_LEN + len(metric)
    directory_offset = postings_offset + len(postings)
    pages_offset = directory_offset + directory_len
    auxiliary_offset = pages_offset + page_count * inventory.EXACT_PAGE_LEN
    auxiliary, auxiliary_crc = auxiliary_directory()
    trailer_offset = auxiliary_offset + len(auxiliary)
    file_len = trailer_offset + inventory.INDEX_TRAILER_LEN

    pages = []
    descriptors = []
    payload_offset = postings_offset
    cursor = 0
    for page_index in range(page_count):
        selected = lists[cursor : cursor + inventory.EXACT_RECORDS_PER_PAGE]
        page = bytearray(inventory.EXACT_PAGE_LEN)
        put_u32(page, 0, index_format.exact_page_magic)
        put_u16(page, 4, index_format.exact_page_version)
        put_u32(page, 8, page_index)
        put_u32(page, 12, len(selected))
        for record_index, ((name, value), refs) in enumerate(selected):
            payload = payloads[cursor + record_index]
            record = inventory.EXACT_PAGE_HEADER_LEN + record_index * inventory.EXACT_RECORD_LEN
            put_u32(page, record, name)
            put_u32(page, record + 4, value)
            put_u64(page, record + 8, payload_offset)
            put_u64(page, record + 16, len(payload))
            put_u64(page, record + 24, 1)
            put_u64(page, record + 32, 2)
            put_u32(page, record + 40, len(refs))
            put_u32(page, record + 44, inventory._crc32c(payload))
            payload_offset += len(payload)
        page_crc = inventory._crc32c(page)
        descriptors.append((selected[0][0], selected[-1][0], len(selected), page_crc))
        pages.append(bytes(page))
        cursor += len(selected)

    directory = bytearray(directory_len)
    put_u32(directory, 0, index_format.exact_directory_magic)
    put_u16(directory, 4, index_format.exact_directory_version)
    put_u32(directory, 8, inventory.EXACT_DIRECTORY_HEADER_LEN)
    put_u32(directory, 12, inventory.EXACT_PAGE_DESCRIPTOR_LEN)
    put_u32(directory, 16, inventory.EXACT_PAGE_LEN)
    put_u32(directory, 20, inventory.EXACT_RECORD_LEN)
    put_u64(directory, 24, len(lists))
    put_u32(directory, 32, page_count)
    put_u32(directory, 36, inventory.EXACT_RECORDS_PER_PAGE)
    put_u64(directory, 40, inventory.EXACT_DIRECTORY_HEADER_LEN)
    put_u64(directory, 48, page_count * inventory.EXACT_PAGE_DESCRIPTOR_LEN)
    for page_index, (first, last, count, page_crc) in enumerate(descriptors):
        start = (
            inventory.EXACT_DIRECTORY_HEADER_LEN
            + page_index * inventory.EXACT_PAGE_DESCRIPTOR_LEN
        )
        put_u32(directory, start, first[0])
        put_u32(directory, start + 4, first[1])
        put_u32(directory, start + 8, last[0])
        put_u32(directory, start + 12, last[1])
        put_u32(directory, start + 16, count)
        put_u32(directory, start + 24, page_crc)
    directory_crc = rewrite_crc32c(directory, 56)

    header = bytearray(inventory.INDEX_HEADER_LEN)
    put_u32(header, 0, inventory.INDEX_MAGIC)
    put_u16(header, 4, version)
    put_u32(header, 8, inventory.INDEX_HEADER_LEN)
    trailer = bytearray(inventory.INDEX_TRAILER_LEN)
    put_u32(trailer, 0, inventory.INDEX_TRAILER_MAGIC)
    put_u16(trailer, 4, version)
    put_u32(trailer, 8, inventory.INDEX_TRAILER_LEN)
    put_u64(trailer, inventory.TRAILER_FILE_LEN_OFFSET, file_len)
    put_locator(
        trailer,
        inventory.TRAILER_METRIC_LOCATOR_OFFSET,
        inventory.INDEX_HEADER_LEN,
        len(metric),
    )
    put_locator(
        trailer,
        inventory.TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET,
        postings_offset,
        len(postings),
    )
    put_locator(
        trailer,
        inventory.TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
        directory_offset,
        len(directory),
    )
    put_locator(
        trailer,
        inventory.TRAILER_EXACT_PAGES_LOCATOR_OFFSET,
        pages_offset,
        page_count * inventory.EXACT_PAGE_LEN,
    )
    put_locator(
        trailer,
        inventory.TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        auxiliary_offset,
        len(auxiliary),
    )
    put_u64(trailer, inventory.TRAILER_EXACT_ENTRY_COUNT_OFFSET, len(lists))
    put_u32(trailer, inventory.TRAILER_EXACT_PAGE_COUNT_OFFSET, page_count)
    put_u32(trailer, inventory.TRAILER_EXACT_RECORD_LEN_OFFSET, inventory.EXACT_RECORD_LEN)
    put_u32(trailer, inventory.TRAILER_EXACT_PAGE_LEN_OFFSET, inventory.EXACT_PAGE_LEN)
    put_u32(trailer, inventory.TRAILER_SERIES_COUNT_OFFSET, series_count)
    put_u32(trailer, inventory.TRAILER_SYMBOL_COUNT_OFFSET, symbol_count)
    put_u32(trailer, inventory.TRAILER_EXACT_DIRECTORY_CRC_OFFSET, directory_crc)
    put_u32(trailer, inventory.TRAILER_AUX_DIRECTORY_CRC_OFFSET, auxiliary_crc)
    put_u32(
        trailer,
        inventory.TRAILER_TERMINAL_MAGIC_OFFSET,
        index_format.terminal_magic,
    )
    rewrite_crc32c(trailer, inventory.TRAILER_CRC_OFFSET)
    return (
        bytes(header)
        + metric
        + postings
        + bytes(directory)
        + b"".join(pages)
        + auxiliary
        + bytes(trailer)
    )


def indexes_v8(
    lists: list[tuple[tuple[int, int], list[int]]],
    series_count: int,
    symbol_count: int,
) -> bytes:
    return indexes(lists, series_count, symbol_count, inventory.INDEX_VERSION)


def indexes_v9(
    lists: list[tuple[tuple[int, int], list[int]]],
    series_count: int,
    symbol_count: int,
    encoded_payloads: list[bytes] | None = None,
) -> bytes:
    return indexes(
        lists,
        series_count,
        symbol_count,
        inventory.INDEX_V9_VERSION,
        encoded_payloads,
    )


def write_corpus(
    root: Path,
    lists: list[tuple[tuple[int, int], list[int]]],
    *,
    version: int = inventory.INDEX_VERSION,
    encoded_payloads: list[bytes] | None = None,
) -> Path:
    corpus = root / "segments"
    segment = corpus / "seg-1-2-00000000000000000000000000"
    segment.mkdir(parents=True)
    series_count = 256
    symbols = [f"symbol-{index:02d}".encode() for index in range(8)]
    segment.joinpath("series.bin").write_bytes(series_v3(series_count))
    segment.joinpath("symbols.bin").write_bytes(symbols_v3(symbols))
    segment.joinpath("indexes.puffin").write_bytes(
        indexes(
            lists,
            series_count,
            len(symbols),
            version,
            encoded_payloads,
        )
    )
    return corpus


def rewrite_directory_and_trailer_crc(value: bytearray) -> None:
    trailer_offset = len(value) - inventory.INDEX_TRAILER_LEN
    trailer = bytearray(value[trailer_offset:])
    directory_offset = inventory._u64(
        trailer, inventory.TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET
    )
    directory_len = inventory._u64(
        trailer, inventory.TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET + 8
    )
    directory = bytearray(value[directory_offset : directory_offset + directory_len])
    directory_crc = rewrite_crc32c(directory, 56)
    value[directory_offset : directory_offset + directory_len] = directory
    put_u32(
        trailer,
        inventory.TRAILER_EXACT_DIRECTORY_CRC_OFFSET,
        directory_crc,
    )
    rewrite_crc32c(trailer, inventory.TRAILER_CRC_OFFSET)
    value[trailer_offset:] = trailer


def parse_constructed_v9(payload: bytes, refs: list[int], series_count: int) -> inventory.Totals:
    index = indexes_v9([((0, 1), refs)], series_count, 2, [payload])
    with tempfile.TemporaryDirectory() as temporary_directory:
        path = Path(temporary_directory) / "indexes.puffin"
        path.write_bytes(index)
        logical_digest = hashlib.sha256()
        totals, _, _, _ = inventory._parse_index(
            path,
            inventory.BoundRoot(series_count, 0, 0, ""),
            inventory.BoundRoot(2, 0, 0, ""),
            inventory._Crc32cSpans(),
            "seg-test",
            logical_digest,
        )
        return totals


class AdaptivePostingsInventoryTest(unittest.TestCase):
    def test_crc_and_uleb_boundaries(self) -> None:
        self.assertEqual(inventory._crc32c(b"123456789"), 0xE3069283)
        self.assertEqual(
            [
                inventory._uleb128_len(value)
                for value in (0, 127, 128, 16_383, 16_384, 0x1F_FFFF, 0x20_0000, 0xFFFF_FFFF)
            ],
            [1, 1, 2, 2, 3, 3, 4, 5],
        )

    def test_complete_inventory_is_reproducible_and_models_all_lists(self) -> None:
        lists = [((0, 1), [0]), ((0, 2), [1, 128, 255]), ((3, 4), [3, 130])]
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = write_corpus(Path(temporary_directory), lists)
            first = inventory.inventory_corpus(corpus)
            second = inventory.inventory_corpus(corpus)

        aggregate = first["aggregate"]
        self.assertEqual(aggregate["list_count"], 3)
        self.assertEqual(aggregate["ref_count"], 6)
        self.assertEqual(aggregate["raw_v8_bytes"], 36)
        self.assertEqual(aggregate["delta_candidate_bytes"], 18)
        self.assertEqual(aggregate["selected_v9_bytes"], 18)
        self.assertEqual(aggregate["codec_selection"]["delta_uleb128_lists"], 3)
        self.assertEqual(
            first["input"]["measurement_input_fingerprint"]["digest"],
            second["input"]["measurement_input_fingerprint"]["digest"],
        )
        self.assertEqual(first["segments"][0]["bound_roots"]["series"]["count"], 256)
        json.dumps(first)

    def test_v8_and_v9_decode_to_the_same_logical_fingerprint(self) -> None:
        lists = [((0, 1), [0]), ((0, 2), [1, 128, 255]), ((3, 4), [3, 130])]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            v8 = inventory.inventory_corpus(write_corpus(root / "v8", lists))
            v9 = inventory.inventory_corpus(
                write_corpus(root / "v9", lists, version=inventory.INDEX_V9_VERSION)
            )

        self.assertEqual(
            v8["input"]["decoded_exact_postings_logical_fingerprint"]["digest"],
            v9["input"]["decoded_exact_postings_logical_fingerprint"]["digest"],
        )
        self.assertNotEqual(
            v8["input"]["measurement_input_fingerprint"]["digest"],
            v9["input"]["measurement_input_fingerprint"]["digest"],
        )
        self.assertEqual(
            v8["input"]["integrity_checked_index_formats"], ["indexes.puffin v8"]
        )
        self.assertEqual(
            v9["input"]["integrity_checked_index_formats"], ["indexes.puffin v9"]
        )
        self.assertEqual(
            v8["aggregate"]["actual_codec_counts"],
            {"raw32_lists": 3, "delta_uleb128_lists": 0},
        )
        self.assertEqual(
            v9["aggregate"]["actual_codec_counts"],
            {"raw32_lists": 0, "delta_uleb128_lists": 3},
        )
        self.assertEqual(v8["aggregate"]["actual_encoded_postings_bytes"], 36)
        self.assertEqual(v9["aggregate"]["actual_encoded_postings_bytes"], 18)
        self.assertEqual(
            v9["aggregate"]["actual_encoded_postings_bytes"],
            v9["aggregate"]["selected_v9_bytes"],
        )

    def test_v9_accepts_canonical_raw_tie(self) -> None:
        large_ref = 1 << 21
        payload = bytes((0, 0, 0, 0)) + struct.pack("<I", large_ref)
        totals = parse_constructed_v9(payload, [large_ref], large_ref + 1)
        self.assertEqual(totals.actual_raw_lists, 1)
        self.assertEqual(totals.actual_encoded_postings_bytes, 8)

    def test_v9_rejects_payload_codec_and_varint_corruption(self) -> None:
        cases = [
            ("unknown codec", bytes((2, 0, 0, 0, 0)), [0], 256, "codec is unknown"),
            (
                "flags",
                bytes((1, 1, 0, 0, 0)),
                [0],
                256,
                "flags or reserved",
            ),
            (
                "reserved",
                bytes((1, 0, 1, 0, 0)),
                [0],
                256,
                "flags or reserved",
            ),
            (
                "overlong",
                bytes((1, 0, 0, 0, 0x80, 0)),
                [0],
                256,
                "not canonically encoded",
            ),
            (
                "truncated",
                bytes((1, 0, 0, 0, 0x80, 0x80)),
                [0, 1],
                256,
                "varint is truncated",
            ),
            (
                "overflow",
                bytes((1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0x10, 1)),
                [0, 1],
                256,
                "varint exceeds u32",
            ),
            (
                "addition overflow",
                bytes((1, 0, 0, 0))
                + encode_uleb128(0xFFFF_FFFE)
                + encode_uleb128(2),
                [0, 1],
                0xFFFF_FFFF,
                "delta addition overflows",
            ),
            (
                "zero gap",
                bytes((1, 0, 0, 0, 0, 0)),
                [0, 1],
                256,
                "delta gap is zero",
            ),
            (
                "trailing",
                bytes((1, 0, 0, 0, 0, 0)),
                [0],
                256,
                "trailing bytes",
            ),
            (
                "out of range",
                bytes((1, 0, 0, 0)) + encode_uleb128(256),
                [256],
                256,
                "bound series count",
            ),
            (
                "noncanonical raw",
                bytes((0, 0, 0, 0)) + struct.pack("<I", 0),
                [0],
                256,
                "RAW32 codec choice",
            ),
        ]
        for name, payload, refs, series_count, message in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(inventory.InventoryError, message):
                    parse_constructed_v9(payload, refs, series_count)

        large_ref = 1 << 21
        noncanonical_delta_tie = bytes((1, 0, 0, 0)) + encode_uleb128(large_ref)
        with self.assertRaisesRegex(inventory.InventoryError, "delta codec choice"):
            parse_constructed_v9(
                noncanonical_delta_tie,
                [large_ref],
                large_ref + 1,
            )

        duplicate_raw = bytes((0, 0, 0, 0)) + struct.pack(
            "<II", large_ref, large_ref
        )
        with self.assertRaisesRegex(inventory.InventoryError, "strictly ordered"):
            parse_constructed_v9(
                duplicate_raw,
                [large_ref, large_ref],
                large_ref + 1,
            )

    def test_v9_rejects_payload_crc_mismatch_before_codec_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = write_corpus(
                Path(temporary_directory),
                [((0, 1), [1, 2, 3])],
                version=inventory.INDEX_V9_VERSION,
            )
            index = next(corpus.glob("seg-*/indexes.puffin"))
            value = bytearray(index.read_bytes())
            value[inventory.INDEX_HEADER_LEN + 1] = 0xFF
            index.write_bytes(value)
            with self.assertRaisesRegex(inventory.InventoryError, "v9 exact postings CRC"):
                inventory.inventory_corpus(corpus)

    def test_v9_rejects_versioned_structure_substitution(self) -> None:
        lists = [((0, 1), [0])]
        original = indexes_v9(lists, 256, 8)
        mutations: list[tuple[str, bytearray, str]] = []

        header = bytearray(original)
        put_u32(header, 0, inventory.INDEX_TRAILER_MAGIC)
        mutations.append(("header", header, "canonical indexes.puffin v9 header"))

        trailer = bytearray(original)
        trailer_offset = len(trailer) - inventory.INDEX_TRAILER_LEN
        put_u32(
            trailer,
            trailer_offset + inventory.TRAILER_TERMINAL_MAGIC_OFFSET,
            inventory.INDEX_TERMINAL_MAGIC,
        )
        trailer_root = bytearray(trailer[trailer_offset:])
        rewrite_crc32c(trailer_root, inventory.TRAILER_CRC_OFFSET)
        trailer[trailer_offset:] = trailer_root
        mutations.append(("trailer", trailer, "v9 fixed trailer"))

        directory = bytearray(original)
        trailer_root = directory[-inventory.INDEX_TRAILER_LEN :]
        directory_offset = inventory._u64(
            trailer_root, inventory.TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET
        )
        put_u32(directory, directory_offset, inventory.EXACT_DIRECTORY_MAGIC)
        rewrite_directory_and_trailer_crc(directory)
        mutations.append(("directory", directory, "v9 exact-directory header"))

        page = bytearray(original)
        trailer_root = page[-inventory.INDEX_TRAILER_LEN :]
        page_offset = inventory._u64(
            trailer_root, inventory.TRAILER_EXACT_PAGES_LOCATOR_OFFSET
        )
        directory_offset = inventory._u64(
            trailer_root, inventory.TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET
        )
        put_u32(page, page_offset, inventory.EXACT_PAGE_MAGIC)
        page_crc = inventory._crc32c(
            page[page_offset : page_offset + inventory.EXACT_PAGE_LEN]
        )
        put_u32(
            page,
            directory_offset + inventory.EXACT_DIRECTORY_HEADER_LEN + 24,
            page_crc,
        )
        rewrite_directory_and_trailer_crc(page)
        mutations.append(("page", page, "v9 exact page 0 header"))

        for name, value, message in mutations:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary_directory:
                corpus = write_corpus(
                    Path(temporary_directory),
                    lists,
                    version=inventory.INDEX_V9_VERSION,
                )
                next(corpus.glob("seg-*/indexes.puffin")).write_bytes(value)
                with self.assertRaisesRegex(inventory.InventoryError, message):
                    inventory.inventory_corpus(corpus)

    def test_raw_wins_ties(self) -> None:
        totals = inventory.Totals()
        totals.add(ref_count=1, raw_bytes=8, delta_bytes=8)
        report = totals.to_json(index_bytes=100)
        self.assertEqual(report["codec_selection"], {"raw32_lists": 1, "delta_uleb128_lists": 0})
        self.assertEqual(report["selected_v9_bytes"], 8)

    def test_rejects_payload_crc_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = write_corpus(Path(temporary_directory), [((0, 1), [1, 2, 3])])
            index = next(corpus.glob("seg-*/indexes.puffin"))
            value = bytearray(index.read_bytes())
            value[inventory.INDEX_HEADER_LEN + 1 + 4] ^= 1
            index.write_bytes(value)
            with self.assertRaisesRegex(inventory.InventoryError, "postings CRC mismatch"):
                inventory.inventory_corpus(corpus)

    def test_rejects_ordered_payload_with_valid_crc_when_refs_decrease(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = write_corpus(Path(temporary_directory), [((0, 1), [2, 1])])
            with self.assertRaisesRegex(inventory.InventoryError, "strictly ordered"):
                inventory.inventory_corpus(corpus)

    def test_rejects_index_count_not_bound_to_series_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = write_corpus(root, [((0, 1), [1])])
            segment = next(corpus.glob("seg-*"))
            segment.joinpath("series.bin").write_bytes(series_v3(255))
            with self.assertRaisesRegex(inventory.InventoryError, "integrity-checked.*root"):
                inventory.inventory_corpus(corpus)


if __name__ == "__main__":
    unittest.main()
