#!/usr/bin/env python3

from __future__ import annotations

import struct
import tempfile
import unittest
from pathlib import Path

import storage_inventory


def symbols_v2(values: list[bytes]) -> bytes:
    strings = b"".join(values)
    offsets = [0]
    for value in values:
        offsets.append(offsets[-1] + len(value))
    return (
        struct.pack("<IHHI", storage_inventory.SYMBOLS_MAGIC, 2, 0, len(values))
        + struct.pack(f"<{len(offsets)}Q", *offsets)
        + strings
    )


def symbols_v3(values: list[bytes]) -> bytes:
    if not values:
        return struct.pack(
            "<IHHIIIIQQQQQQII",
            storage_inventory.SYMBOLS_MAGIC,
            3,
            0,
            80,
            48,
            0,
            0,
            80,
            0,
            80,
            0,
            80,
            80,
            0,
            0,
        )

    strings = b"".join(values)
    offsets = [0]
    for value in values:
        offsets.append(offsets[-1] + len(value))
    offsets_bytes = struct.pack(f"<{len(offsets)}I", *offsets)
    page_header = struct.pack(
        "<IHHIIIIII",
        storage_inventory.SYMBOLS_V3_PAGE_MAGIC,
        1,
        0,
        0,
        0,
        len(values),
        len(offsets_bytes),
        len(strings),
        0,
    )
    page = page_header + offsets_bytes + strings
    fences = values[0] + values[-1]
    pages_offset = 80 + 48 + len(fences)
    descriptor = struct.pack(
        "<IIQIIIIIIII",
        0,
        len(values),
        pages_offset,
        len(page),
        0,
        0,
        len(values[0]),
        len(values[0]),
        len(values[-1]),
        len(strings),
        0,
    )
    file_len = pages_offset + len(page)
    header = struct.pack(
        "<IHHIIIIQQQQQQII",
        storage_inventory.SYMBOLS_MAGIC,
        3,
        0,
        80,
        48,
        len(values),
        1,
        80,
        48,
        128,
        len(fences),
        pages_offset,
        file_len,
        0,
        0,
    )
    return header + descriptor + fences + page


class StorageInventoryTest(unittest.TestCase):
    def test_synthetic_v2_layout_components_equal_file(self) -> None:
        data = symbols_v2([b"alpha", b"omega"])
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "symbols.bin"
            path.write_bytes(data)
            layout = storage_inventory.parse_symbols(path)

        self.assertEqual(layout.version, 2)
        self.assertEqual(layout.symbol_count, 2)
        self.assertEqual(layout.page_count, 0)
        self.assertEqual(
            layout.components,
            {"header": 12, "offset_table": 24, "string_bytes": 10},
        )
        self.assertEqual(sum(layout.components.values()), len(data))

    def test_synthetic_v3_layout_components_equal_file(self) -> None:
        data = symbols_v3([b"alpha", b"omega"])
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "symbols.bin"
            path.write_bytes(data)
            layout = storage_inventory.parse_symbols(path)

        self.assertEqual(layout.version, 3)
        self.assertEqual(layout.symbol_count, 2)
        self.assertEqual(layout.page_count, 1)
        self.assertEqual(
            layout.components,
            {
                "root_header": 80,
                "page_descriptors": 48,
                "fence_bytes": 10,
                "page_headers": 32,
                "page_offset_tables": 12,
                "page_string_bytes": 10,
            },
        )
        self.assertEqual(sum(layout.components.values()), len(data))

    def test_corpus_aggregates_standard_artifacts_and_both_layouts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            v2_segment = corpus / "seg-v2"
            v3_segment = corpus / "seg-v3"
            v2_segment.mkdir()
            v3_segment.mkdir()
            v2_segment.joinpath("symbols.bin").write_bytes(symbols_v2([b"a"]))
            v3_segment.joinpath("symbols.bin").write_bytes(symbols_v3([b"b"]))
            v2_segment.joinpath("chunks.bin").write_bytes(b"123")
            v3_segment.joinpath("chunks.bin").write_bytes(b"45678")

            inventory = storage_inventory.inventory_corpus(corpus)

        self.assertEqual(inventory.artifacts["chunks.bin"], storage_inventory.ArtifactTotal(2, 8))
        self.assertEqual(inventory.artifacts["series.bin"], storage_inventory.ArtifactTotal(0, 0))
        self.assertEqual(set(inventory.symbol_layouts), {2, 3})
        self.assertEqual(
            sum(
                sum(layout.components.values())
                for layout in inventory.symbol_layouts.values()
            ),
            inventory.artifacts["symbols.bin"].bytes,
        )

    def test_rejects_trailing_v2_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "symbols.bin"
            path.write_bytes(symbols_v2([b"alpha"]) + b"trailing")
            with self.assertRaisesRegex(
                storage_inventory.InventoryError,
                "component lengths do not equal the file length",
            ):
                storage_inventory.parse_symbols(path)


if __name__ == "__main__":
    unittest.main()
