#!/usr/bin/env python3
"""Inventory Chronoxide segment artifacts and symbols.bin layout bytes."""

from __future__ import annotations

import argparse
import csv
import stat
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable


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

SYMBOLS_MAGIC = int.from_bytes(b"SYMB", "little")
SYMBOLS_V2_HEADER_LEN = 12
SYMBOLS_V3_HEADER_LEN = 80
SYMBOLS_V3_DESCRIPTOR_LEN = 48
SYMBOLS_V3_PAGE_HEADER_LEN = 32
SYMBOLS_V3_PAGE_MAGIC = int.from_bytes(b"SYPG", "little")

V2_COMPONENTS = ("header", "offset_table", "string_bytes")
V3_COMPONENTS = (
    "root_header",
    "page_descriptors",
    "fence_bytes",
    "page_headers",
    "page_offset_tables",
    "page_string_bytes",
)


class InventoryError(ValueError):
    pass


@dataclass(frozen=True)
class SymbolLayout:
    version: int
    symbol_count: int
    page_count: int
    components: dict[str, int]


@dataclass(frozen=True)
class ArtifactTotal:
    file_count: int
    bytes: int


@dataclass(frozen=True)
class SymbolLayoutTotal:
    file_count: int
    symbol_count: int
    page_count: int
    components: dict[str, int]


@dataclass(frozen=True)
class CorpusInventory:
    artifacts: dict[str, ArtifactTotal]
    symbol_layouts: dict[int, SymbolLayoutTotal]


def _error(path: Path, message: str) -> InventoryError:
    return InventoryError(f"{path}: {message}")


def _read_exact_at(source: BinaryIO, offset: int, length: int, path: Path, what: str) -> bytes:
    source.seek(offset)
    value = source.read(length)
    if len(value) != length:
        raise _error(path, f"truncated {what}: expected {length} bytes, got {len(value)}")
    return value


def _regular_file_size(path: Path) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise _error(path, "file disappeared while inventorying") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise _error(path, "expected a regular file (symbolic links are rejected)")
    return metadata.st_size


def _exists_no_follow(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _parse_symbols_v2(path: Path, source: BinaryIO, file_size: int, header: bytes) -> SymbolLayout:
    magic, version, flags, symbol_count = struct.unpack("<IHHI", header)
    if magic != SYMBOLS_MAGIC or version != 2:
        raise _error(path, "invalid symbols v2 header")
    if flags != 0:
        raise _error(path, "symbols v2 flags are non-zero")

    offset_count = symbol_count + 1
    offsets_len = offset_count * 8
    strings_start = SYMBOLS_V2_HEADER_LEN + offsets_len
    if strings_start > file_size:
        raise _error(path, "symbols v2 offset table exceeds the file")
    offset_bytes = _read_exact_at(
        source,
        SYMBOLS_V2_HEADER_LEN,
        offsets_len,
        path,
        "symbols v2 offset table",
    )
    offsets = [value[0] for value in struct.iter_unpack("<Q", offset_bytes)]
    if not offsets or offsets[0] != 0:
        raise _error(path, "symbols v2 first offset is not zero")
    if any(right < left for left, right in zip(offsets, offsets[1:])):
        raise _error(path, "symbols v2 offsets are not non-decreasing")
    strings_len = offsets[-1]
    if strings_start + strings_len != file_size:
        raise _error(path, "symbols v2 component lengths do not equal the file length")

    components = {
        "header": SYMBOLS_V2_HEADER_LEN,
        "offset_table": offsets_len,
        "string_bytes": strings_len,
    }
    if sum(components.values()) != file_size:
        raise _error(path, "symbols v2 inventory total does not equal the file length")
    return SymbolLayout(2, symbol_count, 0, components)


def _decode_fence(path: Path, fences: bytes, offset: int, length: int, name: str) -> bytes:
    end = offset + length
    if end > len(fences):
        raise _error(path, f"symbols v3 {name} fence exceeds the fence region")
    value = fences[offset:end]
    try:
        value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise _error(path, f"symbols v3 {name} fence is not UTF-8: {error}") from None
    return value


def _parse_symbols_v3(path: Path, source: BinaryIO, file_size: int, header: bytes) -> SymbolLayout:
    (
        magic,
        version,
        flags,
        header_len,
        descriptor_len,
        symbol_count,
        page_count,
        directory_offset,
        directory_len,
        fence_offset,
        fence_len,
        pages_offset,
        stored_file_len,
        _root_crc32c,
        reserved,
    ) = struct.unpack("<IHHIIIIQQQQQQII", header)
    if magic != SYMBOLS_MAGIC or version != 3:
        raise _error(path, "invalid symbols v3 header")
    if flags != 0 or reserved != 0:
        raise _error(path, "symbols v3 header flags or reserved field are non-zero")
    if header_len != SYMBOLS_V3_HEADER_LEN:
        raise _error(path, "symbols v3 header length is invalid")
    if descriptor_len != SYMBOLS_V3_DESCRIPTOR_LEN:
        raise _error(path, "symbols v3 descriptor length is invalid")
    if page_count > symbol_count or ((symbol_count == 0) != (page_count == 0)):
        raise _error(path, "symbols v3 symbol and page counts are inconsistent")
    if directory_offset != SYMBOLS_V3_HEADER_LEN:
        raise _error(path, "symbols v3 directory offset is invalid")
    if directory_len != page_count * SYMBOLS_V3_DESCRIPTOR_LEN:
        raise _error(path, "symbols v3 directory length is invalid")
    if fence_offset != directory_offset + directory_len:
        raise _error(path, "symbols v3 fence offset is invalid")
    if pages_offset != fence_offset + fence_len:
        raise _error(path, "symbols v3 pages offset is invalid")
    if stored_file_len != file_size:
        raise _error(path, "symbols v3 stored file length does not equal the file length")
    if pages_offset > file_size:
        raise _error(path, "symbols v3 root exceeds the file")

    directory = _read_exact_at(
        source,
        directory_offset,
        directory_len,
        path,
        "symbols v3 descriptor directory",
    )
    fences = _read_exact_at(source, fence_offset, fence_len, path, "symbols v3 fences")

    descriptors: list[tuple[int, int, int, int, int]] = []
    expected_symbol_id = 0
    expected_page_offset = pages_offset
    expected_fence_offset = 0
    previous_last_fence: bytes | None = None
    page_offset_table_bytes = 0
    page_string_bytes = 0

    for page_index in range(page_count):
        descriptor_start = page_index * SYMBOLS_V3_DESCRIPTOR_LEN
        descriptor = directory[
            descriptor_start : descriptor_start + SYMBOLS_V3_DESCRIPTOR_LEN
        ]
        (
            first_symbol_id,
            descriptor_symbol_count,
            page_offset,
            page_len,
            _page_crc32c,
            first_fence_offset,
            first_fence_len,
            last_fence_offset,
            last_fence_len,
            strings_len,
            descriptor_reserved,
        ) = struct.unpack("<IIQIIIIIIII", descriptor)
        if descriptor_symbol_count == 0:
            raise _error(path, f"symbols v3 page {page_index} has no symbols")
        if first_symbol_id != expected_symbol_id:
            raise _error(path, "symbols v3 symbol ID ranges are not contiguous")
        expected_symbol_id += descriptor_symbol_count
        if page_offset != expected_page_offset:
            raise _error(path, "symbols v3 page byte ranges are not contiguous")
        offsets_len = (descriptor_symbol_count + 1) * 4
        expected_page_len = SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len + strings_len
        if page_len != expected_page_len:
            raise _error(path, f"symbols v3 page {page_index} length is inconsistent")
        expected_page_offset += page_len
        if expected_page_offset > file_size:
            raise _error(path, f"symbols v3 page {page_index} exceeds the file")
        if descriptor_reserved != 0:
            raise _error(path, f"symbols v3 page {page_index} descriptor reserved field is non-zero")
        if first_fence_offset != expected_fence_offset:
            raise _error(path, "symbols v3 first fences are not canonically positioned")
        first_fence = _decode_fence(
            path, fences, first_fence_offset, first_fence_len, "first"
        )
        expected_fence_offset += first_fence_len
        if last_fence_offset != expected_fence_offset:
            raise _error(path, "symbols v3 last fences are not canonically positioned")
        last_fence = _decode_fence(path, fences, last_fence_offset, last_fence_len, "last")
        expected_fence_offset += last_fence_len
        if descriptor_symbol_count == 1 and first_fence != last_fence:
            raise _error(path, f"symbols v3 singleton page {page_index} fences differ")
        if descriptor_symbol_count == 1 and first_fence_len != strings_len:
            raise _error(path, f"symbols v3 singleton page {page_index} fence length is invalid")
        if descriptor_symbol_count > 1 and first_fence >= last_fence:
            raise _error(path, f"symbols v3 page {page_index} fences are not ordered")
        if descriptor_symbol_count > 1 and first_fence_len + last_fence_len > strings_len:
            raise _error(path, f"symbols v3 page {page_index} fences exceed its string bytes")
        if previous_last_fence is not None and previous_last_fence >= first_fence:
            raise _error(path, "symbols v3 page fences are not strictly ordered")
        previous_last_fence = last_fence
        page_offset_table_bytes += offsets_len
        page_string_bytes += strings_len
        descriptors.append(
            (first_symbol_id, descriptor_symbol_count, page_offset, page_len, strings_len)
        )

    if expected_symbol_id != symbol_count:
        raise _error(path, "symbols v3 descriptor symbol counts do not equal the header")
    if expected_page_offset != file_size:
        raise _error(path, "symbols v3 page lengths do not equal the file length")
    if expected_fence_offset != fence_len:
        raise _error(path, "symbols v3 fence locators do not consume the fence region")

    for page_index, descriptor in enumerate(descriptors):
        first_symbol_id, descriptor_symbol_count, page_offset, page_len, strings_len = descriptor
        page_header = _read_exact_at(
            source,
            page_offset,
            SYMBOLS_V3_PAGE_HEADER_LEN,
            path,
            f"symbols v3 page {page_index} header",
        )
        (
            page_magic,
            page_version,
            page_flags,
            stored_page_index,
            stored_first_symbol_id,
            stored_symbol_count,
            offsets_len,
            stored_strings_len,
            page_reserved,
        ) = struct.unpack("<IHHIIIIII", page_header)
        expected_offsets_len = (descriptor_symbol_count + 1) * 4
        if page_magic != SYMBOLS_V3_PAGE_MAGIC or page_version != 1:
            raise _error(path, f"symbols v3 page {page_index} header is invalid")
        if page_flags != 0 or page_reserved != 0:
            raise _error(path, f"symbols v3 page {page_index} flags or reserved field are non-zero")
        if (
            stored_page_index != page_index
            or stored_first_symbol_id != first_symbol_id
            or stored_symbol_count != descriptor_symbol_count
        ):
            raise _error(path, f"symbols v3 page {page_index} identity disagrees with its descriptor")
        if offsets_len != expected_offsets_len or stored_strings_len != strings_len:
            raise _error(path, f"symbols v3 page {page_index} component lengths disagree")
        if SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len + stored_strings_len != page_len:
            raise _error(path, f"symbols v3 page {page_index} components do not equal its length")
        offset_bytes = _read_exact_at(
            source,
            page_offset + SYMBOLS_V3_PAGE_HEADER_LEN,
            offsets_len,
            path,
            f"symbols v3 page {page_index} offset table",
        )
        offsets = [value[0] for value in struct.iter_unpack("<I", offset_bytes)]
        if not offsets or offsets[0] != 0 or offsets[-1] != strings_len:
            raise _error(path, f"symbols v3 page {page_index} endpoint offsets are invalid")
        if any(right < left for left, right in zip(offsets, offsets[1:])):
            raise _error(path, f"symbols v3 page {page_index} offsets are not non-decreasing")

    components = {
        "root_header": SYMBOLS_V3_HEADER_LEN,
        "page_descriptors": directory_len,
        "fence_bytes": fence_len,
        "page_headers": page_count * SYMBOLS_V3_PAGE_HEADER_LEN,
        "page_offset_tables": page_offset_table_bytes,
        "page_string_bytes": page_string_bytes,
    }
    if sum(components.values()) != file_size:
        raise _error(path, "symbols v3 inventory total does not equal the file length")
    return SymbolLayout(3, symbol_count, page_count, components)


def parse_symbols(path: Path) -> SymbolLayout:
    file_size = _regular_file_size(path)
    if file_size < SYMBOLS_V2_HEADER_LEN:
        raise _error(path, "symbols file is shorter than its minimum header")
    with path.open("rb") as source:
        prefix = _read_exact_at(source, 0, SYMBOLS_V2_HEADER_LEN, path, "symbols header")
        magic, version = struct.unpack_from("<IH", prefix)
        if magic != SYMBOLS_MAGIC:
            raise _error(path, "symbols magic mismatch")
        if version == 2:
            return _parse_symbols_v2(path, source, file_size, prefix)
        if version == 3:
            if file_size < SYMBOLS_V3_HEADER_LEN:
                raise _error(path, "symbols file is shorter than the v3 header")
            header = _read_exact_at(source, 0, SYMBOLS_V3_HEADER_LEN, path, "symbols v3 header")
            return _parse_symbols_v3(path, source, file_size, header)
        raise _error(path, f"unsupported symbols version {version}")


def _segment_directories(corpus: Path) -> list[Path]:
    try:
        corpus_metadata = corpus.lstat()
    except FileNotFoundError:
        raise _error(corpus, "corpus does not exist") from None
    if not stat.S_ISDIR(corpus_metadata.st_mode):
        raise _error(corpus, "corpus must be a directory and not a symbolic link")
    segments = []
    for path in corpus.iterdir():
        if not path.name.startswith("seg-"):
            continue
        metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode):
            raise _error(path, "segment path must be a directory and not a symbolic link")
        segments.append(path)
    return sorted(segments, key=lambda path: path.name)


def inventory_corpus(corpus: Path) -> CorpusInventory:
    artifact_files = defaultdict(int)
    artifact_bytes = defaultdict(int)
    layout_files = defaultdict(int)
    layout_symbols = defaultdict(int)
    layout_pages = defaultdict(int)
    layout_components: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    segments = _segment_directories(corpus)
    for segment in segments:
        for artifact in STANDARD_ARTIFACTS:
            path = segment / artifact
            if not _exists_no_follow(path):
                continue
            size = _regular_file_size(path)
            artifact_files[artifact] += 1
            artifact_bytes[artifact] += size

        symbols_path = segment / "symbols.bin"
        if not _exists_no_follow(symbols_path):
            raise _error(segment, "segment has no symbols.bin")
        layout = parse_symbols(symbols_path)
        layout_files[layout.version] += 1
        layout_symbols[layout.version] += layout.symbol_count
        layout_pages[layout.version] += layout.page_count
        for component, size in layout.components.items():
            layout_components[layout.version][component] += size

    artifacts = {
        artifact: ArtifactTotal(artifact_files[artifact], artifact_bytes[artifact])
        for artifact in STANDARD_ARTIFACTS
    }
    symbol_layouts = {
        version: SymbolLayoutTotal(
            layout_files[version],
            layout_symbols[version],
            layout_pages[version],
            dict(layout_components[version]),
        )
        for version in sorted(layout_files)
    }
    component_total = sum(
        sum(layout.components.values()) for layout in symbol_layouts.values()
    )
    if component_total != artifacts["symbols.bin"].bytes:
        raise _error(corpus, "symbols layout bytes do not equal aggregate symbols.bin bytes")
    if sum(layout.file_count for layout in symbol_layouts.values()) != artifacts["symbols.bin"].file_count:
        raise _error(corpus, "symbols layout file count does not equal symbols.bin file count")
    return CorpusInventory(artifacts, symbol_layouts)


def _write_tsv_exclusive(path: Path, header: Iterable[str], rows: Iterable[Iterable[object]]) -> None:
    try:
        with path.open("x", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, delimiter="\t", lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
    except FileExistsError:
        raise _error(path, "refusing to reuse an existing inventory output") from None


def write_inventory(inventory: CorpusInventory, artifacts_output: Path, symbols_output: Path) -> None:
    if artifacts_output == symbols_output:
        raise InventoryError("artifact and symbols inventory outputs must differ")
    if artifacts_output.exists() or symbols_output.exists():
        existing = artifacts_output if artifacts_output.exists() else symbols_output
        raise _error(existing, "refusing to reuse an existing inventory output")

    artifact_rows = (
        (artifact, inventory.artifacts[artifact].file_count, inventory.artifacts[artifact].bytes)
        for artifact in STANDARD_ARTIFACTS
    )
    symbol_rows = []
    for version, layout in inventory.symbol_layouts.items():
        component_order = V2_COMPONENTS if version == 2 else V3_COMPONENTS
        for component in component_order:
            symbol_rows.append(
                (
                    version,
                    component,
                    layout.file_count,
                    layout.symbol_count,
                    layout.page_count,
                    layout.components[component],
                )
            )
    _write_tsv_exclusive(
        artifacts_output,
        ("artifact", "file_count", "bytes"),
        artifact_rows,
    )
    _write_tsv_exclusive(
        symbols_output,
        ("symbols_version", "component", "file_count", "symbol_count", "page_count", "bytes"),
        symbol_rows,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--artifacts-output", required=True, type=Path)
    parser.add_argument("--symbols-output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        inventory = inventory_corpus(args.corpus)
        write_inventory(inventory, args.artifacts_output, args.symbols_output)
    except (InventoryError, OSError) as error:
        print(f"storage inventory: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
