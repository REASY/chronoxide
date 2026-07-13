# Prometheus HTTP Query API Design

Status: implemented by the `chronoxide-api` crate.

## Objective

Expose Chronoxide's sealed-segment PromQL evaluator through the Prometheus HTTP
query protocol so the same HTTP client and query schedule can compare
Chronoxide, Prometheus, and GreptimeDB. The API is an adapter over
`chronoxide-core`; it must not duplicate or alter PromQL semantics.

## Scope

The initial server provides:

- `GET` and form-encoded `POST` `/api/v1/query`
- `GET` and form-encoded `POST` `/api/v1/query_range`
- `GET /-/healthy` and `GET /-/ready`
- Prometheus success and error envelopes for float scalar, vector, and matrix
  results

The server reads sealed segments only. If a manifest is present, only
manifest-published segments are opened. Otherwise the existing directory scan
behavior is used. Active-head reads, remote read/write, metadata endpoints,
and native-histogram wire samples are outside this initial scope. Histogram
expressions whose Chronoxide result is a float vector or matrix are supported.

## Request contract

`query` is required. Instant `time`, and range `start` and `end`, accept either
Unix seconds (including fractional seconds) or RFC3339. `step` accepts Unix
seconds or a Prometheus-style duration composed from `ms`, `s`, `m`, `h`, `d`,
`w`, and `y`. Range bounds are inclusive and are converted exactly once to
integer milliseconds before calling the core evaluator.

An omitted instant `time` uses the server wall clock. Range `start`, `end`, and
`step` are required. Invalid parameters are returned as HTTP 400 Prometheus
`bad_data` errors. Unsupported PromQL, query-limit failures, and storage errors
are returned as HTTP 422 Prometheus `execution` errors. Panics and blocking
worker failures are returned as HTTP 500 `internal` errors.

## Execution and isolation

One immutable `SegmentStoreReader` is opened at startup and shared by the
server. Every request creates a new `SegmentStoreQuerySession`. Session-local
label, projection, and range-scalar caches therefore cannot leak across HTTP
requests. Chunk-read configuration, the experimental cross-segment flow,
range-scalar cache budget, and production query limits are applied to every
session.

Query execution runs on Tokio's blocking pool. A semaphore bounds concurrent
queries before blocking tasks are admitted. The permit is retained until core
execution completes. This avoids blocking async workers and prevents an
unbounded blocking-query backlog. The initial API intentionally has no
misleading timeout: dropping an HTTP future cannot cancel synchronous core
execution.

## Response and observability

Float values use Prometheus strings: finite values use Rust's shortest
round-trippable representation, and non-finite values are `NaN`, `+Inf`, and
`-Inf`. Timestamps are JSON numbers in Unix seconds with millisecond precision.
Labels are emitted as a JSON object.

Successful and evaluator-error responses include:

- `Server-Timing` entries for semaphore queueing, core PromQL execution, and
  JSON serialization
- `X-Chronoxide-Query-Duration-Ns` for core execution only
- `X-Chronoxide-Query-Stats` containing the serialized core `QueryStats`

Client-side wall latency remains the apples-to-apples primary metric. The
headers separate API overhead from evaluator and storage work without changing
the Prometheus response body.

## Correctness gates

Focused tests must cover GET/POST parity, instant scalar/vector and range
matrix encoding, timestamp and step parsing, special float values, health
routes, Prometheus error envelopes, timing headers, manifest inventory
selection, and HTTP-versus-direct-core semantic equivalence on a written
segment corpus.
