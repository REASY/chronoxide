# Specifications index

This directory separates current contracts from time-bounded plans and retained
historical records. A dated document is not current authority merely because it
is stored here.

## Current authority

- [Storage contract](storage.md): persisted format, ingestion, replay, and query
  semantics.
- [Clock contract](clock.md): event-time storage and ingest/capture-time policy.
- [Crate boundaries](crate-boundaries.md): current workspace ownership and API
  migration reference.
- [PromQL coverage](../../promql-coverage.md): supported surface and known gaps.

## Active plans

[active/](active/) contains work that has an open implementation or measurement
decision. Each plan must name its current status and its evidence when a phase
finishes. Completed plans move to `archive/`.

## Historical records

[archive/](archive/) retains completed or superseded designs, implementation
plans, and benchmark protocols for provenance. They are not normative and must
not be used to infer current behavior without checking the current contracts
and code.

## Reviews

Non-normative strategy and design reviews live in [../../reviews/](../../reviews/).
They may inform future work, but do not define storage or query behavior.
