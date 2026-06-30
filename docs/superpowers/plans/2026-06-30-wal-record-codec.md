# WAL Record Codec Plan

## Goal

Add the first Milestone 4 durability slice: a checksummed WAL record codec and append/read file API that future checkpoint, replay, and manifest work can build on.

## Scope

- [x] Encode and decode records matching `storage_spec.md` §7.1: `magic u32`, `version u16`, `type u16`, `len u64`, `payload`, trailing `crc32c u32`.
- [x] Expose WAL record types for `OTLP_BATCH`, `CHECKPOINT`, and `SEGMENT_SEALED`.
- [x] Return clean EOF as `Ok(None)` and torn/corrupt records as `InvalidData` or `UnexpectedEof`.
- [x] Add append/read wrappers for WAL files that return append offsets for future LSN use.
- [x] Cover codec behavior with unit tests and file append/replay behavior with integration tests.

## Out Of Scope

- Checkpoint payload schema and `checkpoint.meta`.
- WAL replay into `HeadBuffer`.
- Manifest authority, segment footer validation, and WAL truncation.
