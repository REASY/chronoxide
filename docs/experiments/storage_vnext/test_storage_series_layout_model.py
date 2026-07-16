#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import json
import struct
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

import storage_series_layout_model as model


def _put_locator(target: bytearray, offset: int, locator: tuple[int, int]) -> None:
    struct.pack_into("<QQ", target, offset, *locator)


def _create_index_v7(
    segment: Path, *, exact_entries: int = 0, auxiliary_entries: int = 0
) -> Path:
    exact_pages = model._page_count(
        exact_entries, model.INDEX_V7_EXACT_RECORDS_PER_PAGE
    )
    value = bytearray(
        model.INDEX_HEADER.pack(
            model.INDEX_MAGIC,
            model.INDEX_V7_VERSION,
            0,
            model.INDEX_HEADER_BYTES,
            0,
        )
    )

    metric_locator = (len(value), 1)
    value.extend(b"M")
    exact_postings_locator = (0, 0)
    if exact_entries:
        exact_postings_locator = (len(value), 1)
        value.extend(b"P")
    auxiliary_payloads_locator = (0, 0)
    if auxiliary_entries:
        auxiliary_payloads_locator = (len(value), auxiliary_entries)
        value.extend(index % 256 for index in range(auxiliary_entries))

    exact_directory = bytearray(model.INDEX_EXACT_DIRECTORY_HEADER_BYTES)
    struct.pack_into(
        "<IHHIIIIQIIQQII",
        exact_directory,
        0,
        model.INDEX_EXACT_DIRECTORY_MAGIC,
        model.INDEX_EXACT_DIRECTORY_VERSION,
        0,
        model.INDEX_EXACT_DIRECTORY_HEADER_BYTES,
        model.INDEX_EXACT_PAGE_DESCRIPTOR_BYTES,
        model.INDEX_EXACT_PAGE_BYTES,
        model.INDEX_V7_EXACT_RECORD_BYTES,
        exact_entries,
        exact_pages,
        model.INDEX_V7_EXACT_RECORDS_PER_PAGE,
        model.INDEX_EXACT_DIRECTORY_HEADER_BYTES,
        exact_pages * model.INDEX_EXACT_PAGE_DESCRIPTOR_BYTES,
        0,
        0,
    )
    decoded_entries = 0
    for _ in range(exact_pages):
        record_count = min(
            exact_entries - decoded_entries,
            model.INDEX_V7_EXACT_RECORDS_PER_PAGE,
        )
        exact_directory.extend(
            struct.pack(
                "<IIIIIIII",
                0,
                decoded_entries,
                0,
                decoded_entries + record_count - 1,
                record_count,
                0,
                0,
                0,
            )
        )
        decoded_entries += record_count
    struct.pack_into("<I", exact_directory, 56, model._crc32c(exact_directory))
    exact_directory_locator = (len(value), len(exact_directory))
    value.extend(exact_directory)

    exact_pages_locator = (0, 0)
    if exact_pages:
        exact_pages_locator = (
            len(value),
            exact_pages * model.INDEX_EXACT_PAGE_BYTES,
        )
        value.extend(bytes(exact_pages_locator[1]))

    auxiliary_directory = bytearray(model.INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES)
    struct.pack_into(
        "<IHHIIQQQI20s",
        auxiliary_directory,
        0,
        model.INDEX_AUXILIARY_DIRECTORY_MAGIC,
        model.INDEX_AUXILIARY_DIRECTORY_VERSION,
        0,
        model.INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES,
        model.INDEX_V7_AUXILIARY_RECORD_BYTES,
        auxiliary_entries,
        model.INDEX_AUXILIARY_DIRECTORY_HEADER_BYTES,
        auxiliary_entries * model.INDEX_V7_AUXILIARY_RECORD_BYTES,
        0,
        bytes(20),
    )
    for index in range(auxiliary_entries):
        auxiliary_directory.extend(
            struct.pack(
                "<HHIQQQQ",
                2,
                0,
                index,
                auxiliary_payloads_locator[0] + index,
                1,
                0,
                (1 << 64) - 1,
            )
        )
    struct.pack_into("<I", auxiliary_directory, 40, model._crc32c(auxiliary_directory))
    auxiliary_directory_locator = (len(value), len(auxiliary_directory))
    value.extend(auxiliary_directory)

    file_len = len(value) + model.INDEX_TRAILER_BYTES
    trailer = bytearray(model.INDEX_TRAILER_BYTES)
    struct.pack_into(
        "<IHHIIQ",
        trailer,
        0,
        model.INDEX_TRAILER_MAGIC,
        model.INDEX_V7_VERSION,
        0,
        model.INDEX_TRAILER_BYTES,
        0,
        file_len,
    )
    _put_locator(trailer, 24, (0, 0))
    _put_locator(trailer, 40, metric_locator)
    _put_locator(trailer, 56, exact_directory_locator)
    _put_locator(trailer, 72, exact_pages_locator)
    _put_locator(trailer, 88, exact_postings_locator)
    _put_locator(trailer, 104, auxiliary_directory_locator)
    _put_locator(trailer, 120, auxiliary_payloads_locator)
    struct.pack_into("<Q", trailer, 136, exact_entries)
    struct.pack_into("<I", trailer, 144, exact_pages)
    struct.pack_into("<I", trailer, 148, model.INDEX_V7_EXACT_RECORD_BYTES)
    struct.pack_into("<I", trailer, 152, model.INDEX_EXACT_PAGE_BYTES)
    struct.pack_into("<I", trailer, 156, auxiliary_entries)
    struct.pack_into(
        "<I", trailer, model.INDEX_TRAILER_TERMINAL_MAGIC_OFFSET, model.INDEX_V7_TERMINAL_MAGIC
    )
    struct.pack_into(
        "<I", trailer, model.INDEX_TRAILER_CRC_OFFSET, model._crc32c(trailer)
    )
    value.extend(trailer)
    path = segment / "indexes.puffin"
    path.write_bytes(value)
    return path


def _create_segment(
    corpus: Path,
    entries: list[tuple[int, int, int, int, int, int, int, int, int]],
    *,
    kind_mask: int = 1,
    meta_len: int = 0,
    final_directory_delta: int = 0,
    exact_entries: int = 0,
    auxiliary_entries: int = 0,
    segment_name: str = "seg-1000-2000-test",
) -> Path:
    segment = corpus / segment_name
    segment.mkdir()
    segment.joinpath("meta.json").write_text(
        json.dumps({"start_ms": 1000, "end_ms": 2000, "series": 1}),
        encoding="utf-8",
    )

    table_offset = model.SERIES_HEADER_BYTES
    keysets_offset = table_offset + model.SERIES_ROW_BYTES
    value_dicts_offset = keysets_offset + 24
    blocks_offset = value_dicts_offset + 8
    meta_offset = blocks_offset + 32
    chunk_entries_start = model.CHUNK_INDEX_HEADER_BYTES + 2 * 8
    chunk_range_len = len(entries) * model.CHUNK_ENTRY_BYTES
    header = model.SERIES_HEADER.pack(
        model.SERIES_MAGIC,
        model.SERIES_VERSION,
        0,
        1,
        1,
        0,
        0,
        table_offset,
        keysets_offset,
        value_dicts_offset,
        blocks_offset,
        meta_offset,
    )
    row = model.SERIES_ROW.pack(
        42,
        kind_mask,
        0,
        0,
        chunk_entries_start,
        chunk_range_len,
        0,
        0,
        0,
        meta_len,
    )
    # The model inventories the label sections but intentionally leaves their
    # deep validation to the production reader and existing inventory tests.
    segment.joinpath("series.bin").write_bytes(header + row + bytes(meta_offset - keysets_offset))

    index_header = model.CHUNK_INDEX_HEADER.pack(
        model.CHUNK_INDEX_MAGIC,
        model.CHUNK_INDEX_VERSION,
        0,
        1,
    )
    directory = struct.pack(
        "<QQ",
        chunk_entries_start,
        chunk_entries_start + chunk_range_len + final_directory_delta,
    )
    encoded_entries = b"".join(model.CHUNK_ENTRY.pack(*entry) for entry in entries)
    segment.joinpath("chunk_index.bin").write_bytes(index_header + directory + encoded_entries)
    chunk_file_sizes = {
        file_id: max(
            (entry[5] + entry[6] for entry in entries if entry[0] == file_id),
            default=0,
        )
        for file_id in (0, 1)
    }
    segment.joinpath("chunks.bin").write_bytes(bytes(chunk_file_sizes[0]))
    if chunk_file_sizes[1] != 0:
        segment.joinpath("ooo_chunks.bin").write_bytes(bytes(chunk_file_sizes[1]))
    _create_index_v7(
        segment,
        exact_entries=exact_entries,
        auxiliary_entries=auxiliary_entries,
    )
    return segment


def _entry(
    *,
    file_id: int = 0,
    kind: int = 0,
    flags: int = 0,
    min_ms: int = 1100,
    max_ms: int = 1200,
    offset: int = 14,
    length: int = 100,
    scalar_offset: int = 0,
    scalar_len: int = 0,
) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (
        file_id,
        kind,
        flags,
        min_ms,
        max_ms,
        offset,
        length,
        scalar_offset,
        scalar_len,
    )


def _rewrite_trailer(path: Path, mutate: Callable[[bytearray], None]) -> None:
    value = bytearray(path.read_bytes())
    trailer_offset = len(value) - model.INDEX_TRAILER_BYTES
    trailer = value[trailer_offset:]
    mutate(trailer)
    struct.pack_into("<I", trailer, model.INDEX_TRAILER_CRC_OFFSET, 0)
    struct.pack_into(
        "<I", trailer, model.INDEX_TRAILER_CRC_OFFSET, model._crc32c(trailer)
    )
    value[trailer_offset:] = trailer
    path.write_bytes(value)


class StorageSeriesLayoutModelTest(unittest.TestCase):
    def test_crc32c_matches_standard_check_value(self) -> None:
        self.assertEqual(model._crc32c(b"123456789"), 0xE3069283)

    def test_v7_to_v8_exact_page_boundaries_are_per_segment(self) -> None:
        for exact_entries, expected_v7_pages, expected_v8_pages in (
            (0, 0, 0),
            (1, 1, 1),
            (341, 1, 1),
            (342, 1, 2),
        ):
            with (
                self.subTest(exact_entries=exact_entries),
                tempfile.TemporaryDirectory() as temporary_directory,
            ):
                corpus = Path(temporary_directory)
                _create_segment(corpus, [_entry()], exact_entries=exact_entries)
                report = model.model_corpus(corpus)

            observed = report["index_layout"]["observed_v7"]
            projected = report["index_layout"]["projected_v8"]
            self.assertEqual(observed["exact_entry_count"], exact_entries)
            self.assertEqual(observed["exact_page_count"], expected_v7_pages)
            self.assertEqual(projected["exact_page_count"], expected_v8_pages)
            self.assertEqual(
                report["segments"][0]["v8_exact_page_count"], expected_v8_pages
            )
            self.assertEqual(
                report["index_layout"]["v8_delta"]["exact_page_count"],
                expected_v8_pages - expected_v7_pages,
            )

    def test_v8_accounting_includes_auxiliary_records_and_metadata_reduction(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [_entry()],
                exact_entries=342,
                auxiliary_entries=2,
            )
            report = model.model_corpus(corpus)

        index = report["index_layout"]
        self.assertEqual(index["observed_v7"]["exact_record_bytes_logical"], 342 * 40)
        self.assertEqual(index["projected_v8"]["exact_record_bytes_logical"], 342 * 48)
        self.assertEqual(index["v8_delta"]["exact_page_bytes"], 16_384)
        self.assertEqual(index["v8_delta"]["exact_page_descriptor_bytes"], 32)
        self.assertEqual(index["v8_delta"]["auxiliary_record_bytes"], 16)
        self.assertEqual(index["v8_delta"]["indexes_puffin_bytes"], 16_432)
        selected = report["models"]["selected_40_byte_paged"]
        self.assertEqual(
            selected["modeled_savings_bytes"],
            selected["series_chunk_layout_savings_bytes"]
            - selected["index_v8_overhead_bytes"],
        )
        self.assertEqual(
            selected["projected_metadata_bytes"],
            report["current_layout"]["metadata_bytes"]
            - selected["modeled_savings_bytes"],
        )

    def test_v8_page_counts_sum_per_segment_ceilings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [_entry()],
                exact_entries=1,
                segment_name="seg-1000-2000-a",
            )
            _create_segment(
                corpus,
                [_entry()],
                exact_entries=1,
                segment_name="seg-1000-2000-b",
            )
            report = model.model_corpus(corpus)

        self.assertEqual(report["index_layout"]["observed_v7"]["exact_entry_count"], 2)
        self.assertEqual(report["index_layout"]["observed_v7"]["exact_page_count"], 2)
        self.assertEqual(report["index_layout"]["projected_v8"]["exact_page_count"], 2)
        self.assertEqual(
            [segment["v8_exact_page_count"] for segment in report["segments"]],
            [1, 1],
        )

    def test_rejects_index_v7_header_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            segment = _create_segment(corpus, [_entry()])
            path = segment / "indexes.puffin"
            value = bytearray(path.read_bytes())
            struct.pack_into("<H", value, 4, 6)
            path.write_bytes(value)
            with self.assertRaisesRegex(model.ModelError, "expected indexes.puffin v7"):
                model.model_corpus(corpus)

    def test_rejects_index_v7_page_count_disagreement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            segment = _create_segment(corpus, [_entry()], exact_entries=1)
            path = segment / "indexes.puffin"
            _rewrite_trailer(path, lambda trailer: struct.pack_into("<I", trailer, 144, 2))
            with self.assertRaisesRegex(model.ModelError, "exact page count is inconsistent"):
                model.model_corpus(corpus)

    def test_rejects_index_v7_record_width_disagreement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            segment = _create_segment(corpus, [_entry()], exact_entries=1)
            path = segment / "indexes.puffin"
            _rewrite_trailer(path, lambda trailer: struct.pack_into("<I", trailer, 148, 48))
            with self.assertRaisesRegex(model.ModelError, "exact record length is invalid"):
                model.model_corpus(corpus)

    def test_rejects_index_v7_overlapping_root_locator(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            segment = _create_segment(corpus, [_entry()], exact_entries=1)
            path = segment / "indexes.puffin"

            def overlap_exact_directory(trailer: bytearray) -> None:
                exact_len = struct.unpack_from("<Q", trailer, 64)[0]
                _put_locator(trailer, 56, (model.INDEX_HEADER_BYTES, exact_len))

            _rewrite_trailer(path, overlap_exact_directory)
            with self.assertRaisesRegex(model.ModelError, "overlap or are out of physical order"):
                model.model_corpus(corpus)

    def test_rejects_index_v7_exact_directory_crc_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            segment = _create_segment(corpus, [_entry()], exact_entries=1)
            path = segment / "indexes.puffin"
            value = bytearray(path.read_bytes())
            trailer = value[-model.INDEX_TRAILER_BYTES :]
            directory_offset = struct.unpack_from("<Q", trailer, 56)[0]
            value[directory_offset + model.INDEX_EXACT_DIRECTORY_HEADER_BYTES + 24] ^= 1
            path.write_bytes(value)
            with self.assertRaisesRegex(model.ModelError, "exact directory CRC mismatch"):
                model.model_corpus(corpus)

    def test_single_chunk_is_inline_and_saves_exact_hot_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry()])
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["series_count"], 1)
        self.assertEqual(report["observed"]["chunk_count"], 1)
        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 1)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 1)
        self.assertEqual(report["observed"]["conservative_overflow_series"], 0)
        self.assertEqual(report["observed"]["paged_overflow_series"], 0)
        self.assertEqual(report["current_layout"]["hot_routing_index_bytes"], 96)
        conservative = report["models"]["conservative_56_byte"]
        self.assertEqual(conservative["modeled_hot_routing_index_bytes"], 56)
        self.assertEqual(conservative["modeled_savings_bytes"], 40)
        self.assertEqual(
            conservative["projected_series_and_chunk_index_bytes"],
            report["current_layout"]["series_and_chunk_index_bytes"] - 40,
        )
        paged = report["models"]["selected_40_byte_paged"]
        self.assertEqual(paged["hot_page_count"], 1)
        self.assertEqual(paged["cold_page_count"], 1)
        self.assertEqual(paged["total_page_count"], 2)
        self.assertEqual(paged["series_header_bytes"], 176)
        self.assertEqual(paged["hot_page_descriptor_bytes"], 16)
        self.assertEqual(paged["cold_page_descriptor_bytes"], 16)
        self.assertEqual(paged["page_descriptor_bytes"], 32)
        self.assertEqual(paged["hot_offset_alignment_bytes"], 3888)
        self.assertEqual(paged["hot_page_bytes"], 16384)
        self.assertEqual(paged["hot_page_zero_padding_bytes"], 16320)
        self.assertEqual(paged["cold_label_bytes"], 64)
        self.assertEqual(paged["cold_final_pages_bytes"], 64)
        self.assertEqual(paged["chunk_index_root_bytes"], 64)

    def test_multi_chunk_series_uses_general_overflow_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [
                    _entry(offset=14, length=100),
                    _entry(min_ms=1300, max_ms=1400, offset=114, length=100),
                ],
            )
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["chunk_shape"]["multi_chunk_series"], 1)
        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["conservative_overflow_series"], 1)
        self.assertEqual(report["observed"]["paged_overflow_series"], 1)
        self.assertEqual(report["observed"]["paged_overflow_chunks"], 2)
        conservative = report["models"]["conservative_56_byte"]
        self.assertEqual(conservative["fixed_series_record_bytes"], 56)
        self.assertEqual(conservative["overflow_descriptor_bytes"], 80)
        self.assertEqual(conservative["modeled_savings_bytes"], 0)
        paged = report["models"]["selected_40_byte_paged"]
        self.assertEqual(paged["overflow_blob_header_bytes"], 32)
        self.assertEqual(paged["overflow_chunk_entry_bytes"], 88)
        self.assertEqual(paged["projected_chunk_index_bytes"], 64 + 32 + 88)

    def test_zero_chunk_series_fails_selected_schema_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [])
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["chunk_shape"]["zero_chunk_series"], 1)
        self.assertFalse(
            report["screening_gate"]["selected_40_byte_paged"]["checks"][
                "common_case_field_fits"
            ]
        )

    def test_noncanonical_scalar_lane_is_counted_as_overflow(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [_entry(kind=2, length=120, scalar_offset=44, scalar_len=20)],
                kind_mask=1 << 2,
            )
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["field_fit_failures"]["scalar_lane_shape"], 1)
        self.assertFalse(
            report["screening_gate"]["selected_40_byte_paged"]["checks"][
                "common_case_field_fits"
            ]
        )

    def test_typed_scalar_lane_shorter_than_header_is_counted_as_overflow(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [
                    _entry(
                        kind=2,
                        length=model.CHUNK_HEADER_BYTES + 15,
                        scalar_offset=model.CHUNK_HEADER_BYTES,
                        scalar_len=15,
                    )
                ],
                kind_mask=1 << 2,
            )
            report = model.model_corpus(corpus)

        self.assertEqual(
            report["observed"]["conservative_inline_eligible_series"], 0
        )
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(
            report["observed"]["field_fit_failures"]["scalar_lane_shape"], 1
        )

    def test_typed_scalar_lane_at_header_boundary_is_inline_eligible(self) -> None:
        for kind in (2, 3, 4):
            with (
                self.subTest(kind=kind),
                tempfile.TemporaryDirectory() as temporary_directory,
            ):
                corpus = Path(temporary_directory)
                _create_segment(
                    corpus,
                    [
                        _entry(
                            kind=kind,
                            length=model.CHUNK_HEADER_BYTES + 16,
                            scalar_offset=model.CHUNK_HEADER_BYTES,
                            scalar_len=16,
                        )
                    ],
                    kind_mask=1 << kind,
                )
                report = model.model_corpus(corpus)

            self.assertEqual(
                report["observed"]["conservative_inline_eligible_series"], 1
            )
            self.assertEqual(report["observed"]["paged_inline_eligible_series"], 1)
            self.assertEqual(
                report["observed"]["field_fit_failures"]["scalar_lane_shape"], 0
            )

    def test_float_chunk_with_scalar_lane_is_counted_as_overflow(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [
                    _entry(
                        kind=0,
                        length=model.CHUNK_HEADER_BYTES + 16,
                        scalar_offset=model.CHUNK_HEADER_BYTES,
                        scalar_len=16,
                    )
                ],
                kind_mask=1 << 0,
            )
            report = model.model_corpus(corpus)

        self.assertEqual(
            report["observed"]["conservative_inline_eligible_series"], 0
        )
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(
            report["observed"]["field_fit_failures"]["scalar_lane_shape"], 1
        )

    def test_selected_page_boundaries_include_fixed_page_padding(self) -> None:
        full = model.paged_series_layout_components(409, 0)
        spill = model.paged_series_layout_components(410, 0)
        self.assertEqual(full["hot_page_count"], 1)
        self.assertEqual(full["cold_page_count"], 0)
        self.assertEqual(full["total_page_count"], 1)
        self.assertEqual(full["cold_final_page_bytes"], 0)
        self.assertEqual(full["page_padding_bytes"], 0)
        self.assertEqual(full["alignment_bytes"], 3904)
        self.assertEqual(spill["hot_page_count"], 2)
        self.assertEqual(spill["cold_page_count"], 0)
        self.assertEqual(spill["page_bytes"], 2 * 16384)
        self.assertEqual(spill["page_padding_bytes"], 16320)
        self.assertEqual(spill["alignment_bytes"], 3888)

    def test_selected_cold_pages_add_descriptors_without_padding_final_page(self) -> None:
        cold_bytes = model.PAGED_COLD_PAGE_BYTES + 7
        layout = model.paged_series_layout_components(1, cold_bytes)

        self.assertEqual(layout["hot_page_count"], 1)
        self.assertEqual(layout["cold_page_count"], 2)
        self.assertEqual(layout["total_page_count"], 3)
        self.assertEqual(layout["hot_descriptor_bytes"], 16)
        self.assertEqual(layout["cold_descriptor_bytes"], 32)
        self.assertEqual(layout["descriptor_bytes"], 48)
        self.assertEqual(layout["alignment_bytes"], 3872)
        self.assertEqual(layout["cold_page_bytes"], cold_bytes)
        self.assertEqual(layout["cold_final_page_bytes"], 7)

    def test_21_bit_scalar_length_uses_selected_overflow_only(self) -> None:
        scalar_len = 1 << 21
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(
                corpus,
                [
                    _entry(
                        kind=2,
                        length=model.CHUNK_HEADER_BYTES + scalar_len,
                        scalar_offset=model.CHUNK_HEADER_BYTES,
                        scalar_len=scalar_len,
                    )
                ],
                kind_mask=1 << 2,
            )
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 1)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["field_fit_failures"]["scalar_lane_len_21bit"], 1)

    def test_ooo_file_id_fits_one_bit_and_chunk_flags_are_not_indexed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry(file_id=1, flags=(1 << 16) - 1)])
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 1)
        self.assertEqual(report["observed"]["field_fit_failures"]["file_id_bit"], 0)
        self.assertEqual(report["observed"]["chunk_flag_counts"], {"65535": 1})

    def test_inline_requires_exact_single_kind_mask(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry()], kind_mask=0b00101)
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["field_fit_failures"]["series_kind_mismatch"], 1)

    def test_inline_requires_chunk_header_length(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry(length=model.CHUNK_HEADER_BYTES - 1)])
            report = model.model_corpus(corpus)

        self.assertEqual(report["observed"]["conservative_inline_eligible_series"], 0)
        self.assertEqual(report["observed"]["paged_inline_eligible_series"], 0)
        self.assertEqual(
            report["observed"]["field_fit_failures"]["chunk_length_at_least_header"],
            1,
        )

    def test_rejects_unimplemented_series_v2_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry()], meta_len=1)
            with self.assertRaisesRegex(
                model.ModelError, "series v2 metadata fields must be zero"
            ):
                model.model_corpus(corpus)

    def test_rejects_directory_that_does_not_end_at_eof(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory)
            _create_segment(corpus, [_entry()], final_directory_delta=40)
            with self.assertRaisesRegex(model.ModelError, "truncated chunk-index entry block"):
                model.model_corpus(corpus)

    def test_cli_writes_machine_readable_json_exclusively(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            _create_segment(corpus, [_entry()])
            output = root / "model.json"
            self.assertEqual(
                model.main(["--corpus", str(corpus), "--output", str(output)]),
                0,
            )
            decoded = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(decoded["model_version"], 4)
            self.assertEqual(decoded["observed"]["paged_inline_eligible_series"], 1)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    model.main(["--corpus", str(corpus), "--output", str(output)]),
                    2,
                )


if __name__ == "__main__":
    unittest.main()
