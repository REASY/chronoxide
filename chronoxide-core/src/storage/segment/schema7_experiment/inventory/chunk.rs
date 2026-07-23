use std::collections::BTreeMap;
use std::io;

use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkSamples, DecodedChunkLayout};

use super::super::super::invalid_segment_data;
use super::super::helpers::{
    checked_add, chunk_encoding_from_inventory_id, chunk_encoding_id, chunk_encoding_name,
    chunk_kind_from_inventory_id, chunk_kind_id, chunk_kind_name, chunk_payload_layout_name,
};
use super::super::{ExperimentalChunkEncodingInventory, ExperimentalChunkInventory};
use super::common::PowerOfTwoHistogramAccumulator;
use super::float::{FloatCodecCandidatesAccumulator, observe_float_codec_candidates};
use super::timestamp::TimestampCodecCandidatesAccumulator;

#[derive(Default)]
struct ChunkEncodingInventoryAccumulator {
    chunks: u64,
    points: u64,
    indexed_bytes: u64,
    common_header_bytes: u64,
    scalar_lane_bytes: u64,
    payload_bytes: u64,
    timestamp_base_bytes: u64,
    timestamp_delta_bytes: u64,
    value_bytes: u64,
    point_count_histogram: PowerOfTwoHistogramAccumulator,
    cadence_ms_histogram: PowerOfTwoHistogramAccumulator,
}

impl ChunkEncodingInventoryAccumulator {
    fn observe(
        &mut self,
        layout: &DecodedChunkLayout,
        indexed_bytes: u32,
        expected_min_time_ms: u64,
        expected_max_time_ms: u64,
        timestamps: impl IntoIterator<Item = u64>,
    ) -> io::Result<()> {
        let indexed_components = layout
            .common_header_bytes
            .checked_add(layout.scalar_lane_bytes)
            .and_then(|bytes| bytes.checked_add(layout.payload_bytes))
            .ok_or_else(|| invalid_segment_data("indexed chunk byte components overflow"))?;
        if indexed_components != indexed_bytes {
            return Err(invalid_segment_data(
                "indexed chunk byte components do not equal its locator length",
            ));
        }
        let payload_components = layout
            .timestamp_base_bytes
            .checked_add(layout.timestamp_delta_bytes)
            .and_then(|bytes| bytes.checked_add(layout.value_bytes))
            .ok_or_else(|| invalid_segment_data("chunk payload byte components overflow"))?;
        if payload_components != layout.payload_bytes {
            return Err(invalid_segment_data(
                "chunk payload byte components do not equal its payload length",
            ));
        }

        let mut observed_points = 0u64;
        let mut first_timestamp = None;
        let mut previous_timestamp = None;
        for timestamp_ms in timestamps {
            first_timestamp.get_or_insert(timestamp_ms);
            if let Some(previous) = previous_timestamp {
                let cadence_ms = timestamp_ms.checked_sub(previous).ok_or_else(|| {
                    invalid_segment_data("decoded chunk timestamps are not ordered")
                })?;
                self.cadence_ms_histogram.observe(cadence_ms)?;
            }
            previous_timestamp = Some(timestamp_ms);
            observed_points = observed_points
                .checked_add(1)
                .ok_or_else(|| invalid_segment_data("decoded point count overflows"))?;
        }
        if observed_points != u64::from(layout.num_points) {
            return Err(invalid_segment_data(
                "decoded point count disagrees with the chunk header",
            ));
        }
        if first_timestamp != Some(expected_min_time_ms)
            || previous_timestamp != Some(expected_max_time_ms)
        {
            return Err(invalid_segment_data(
                "decoded timestamp range disagrees with the chunk header",
            ));
        }

        checked_add(&mut self.chunks, 1, "chunk inventory count")?;
        checked_add(&mut self.points, observed_points, "chunk inventory points")?;
        checked_add(
            &mut self.indexed_bytes,
            u64::from(indexed_bytes),
            "chunk inventory indexed bytes",
        )?;
        checked_add(
            &mut self.common_header_bytes,
            u64::from(layout.common_header_bytes),
            "chunk inventory common-header bytes",
        )?;
        checked_add(
            &mut self.scalar_lane_bytes,
            u64::from(layout.scalar_lane_bytes),
            "chunk inventory scalar-lane bytes",
        )?;
        checked_add(
            &mut self.payload_bytes,
            u64::from(layout.payload_bytes),
            "chunk inventory payload bytes",
        )?;
        checked_add(
            &mut self.timestamp_base_bytes,
            u64::from(layout.timestamp_base_bytes),
            "chunk inventory timestamp-base bytes",
        )?;
        checked_add(
            &mut self.timestamp_delta_bytes,
            u64::from(layout.timestamp_delta_bytes),
            "chunk inventory timestamp-delta bytes",
        )?;
        checked_add(
            &mut self.value_bytes,
            u64::from(layout.value_bytes),
            "chunk inventory value bytes",
        )?;
        self.point_count_histogram
            .observe(u64::from(layout.num_points))?;
        Ok(())
    }

    fn finish(
        self,
        kind: ChunkKind,
        encoding: ChunkEncoding,
    ) -> ExperimentalChunkEncodingInventory {
        ExperimentalChunkEncodingInventory {
            kind: chunk_kind_name(kind).to_owned(),
            encoding: chunk_encoding_name(encoding).to_owned(),
            payload_layout: chunk_payload_layout_name(kind, encoding).to_owned(),
            chunks: self.chunks,
            points: self.points,
            indexed_bytes: self.indexed_bytes,
            common_header_bytes: self.common_header_bytes,
            scalar_lane_bytes: self.scalar_lane_bytes,
            payload_bytes: self.payload_bytes,
            timestamp_base_bytes: self.timestamp_base_bytes,
            timestamp_delta_bytes: self.timestamp_delta_bytes,
            value_bytes: self.value_bytes,
            point_count_histogram: self.point_count_histogram.finish(),
            cadence_ms_histogram: self.cadence_ms_histogram.finish(),
        }
    }
}

#[derive(Default)]
pub(in super::super) struct ChunkInventoryAccumulator {
    by_kind_encoding: BTreeMap<(u8, u8), ChunkEncodingInventoryAccumulator>,
    pub(in super::super) float_candidates: FloatCodecCandidatesAccumulator,
    timestamp_candidates: TimestampCodecCandidatesAccumulator,
}

impl ChunkInventoryAccumulator {
    pub(in super::super) fn observe(
        &mut self,
        layout: &DecodedChunkLayout,
        indexed_bytes: u32,
        min_time_ms: u64,
        max_time_ms: u64,
        samples: &ChunkSamples,
    ) -> io::Result<()> {
        let current_offset_uleb_bytes = layout
            .timestamp_base_bytes
            .checked_add(layout.timestamp_delta_bytes)
            .map(u64::from)
            .ok_or_else(|| invalid_segment_data("native timestamp byte count overflows"))?;
        let key = (
            chunk_kind_id(layout.kind),
            chunk_encoding_id(layout.encoding),
        );
        let inventory = self.by_kind_encoding.entry(key).or_default();
        match samples {
            ChunkSamples::Float(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
                observe_float_codec_candidates(
                    &mut self.float_candidates,
                    layout,
                    indexed_bytes,
                    values,
                )?;
            }
            ChunkSamples::Int64(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::Histogram(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::ExponentialHistogram(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::Summary(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
        }
        Ok(())
    }

    pub(in super::super) fn finish(self) -> ExperimentalChunkInventory {
        let by_kind_encoding = self
            .by_kind_encoding
            .into_iter()
            .map(|((kind, encoding), inventory)| {
                inventory.finish(
                    chunk_kind_from_inventory_id(kind),
                    chunk_encoding_from_inventory_id(encoding),
                )
            })
            .collect();
        ExperimentalChunkInventory {
            layout: "sealed_chunk_v1".to_owned(),
            by_kind_encoding,
            raw_f64_vs_gorilla: self.float_candidates.finish(),
            timestamp_candidates: self.timestamp_candidates.finish(),
        }
    }
}
