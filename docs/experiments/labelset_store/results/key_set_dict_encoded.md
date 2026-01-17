# Ingestion Statistics

## General Stats

| Metric | Value |
|---|---|
| Total Messages | 11376766 |
| Total OTLP Metric Records | 81825901 |
| Total Unique Metrics (`__name__`) | 19953 |
| Total Series (unique label sets) | 79005309 |
| Total Datapoints | 413593326 |
| Total Processing Time | 885.87251567s |
| Total Intern Time | 749.351061955s |
| Skipped Non-Scalar | 0 |

## Partition Watermarks

Based on Kafka record timestamps (`timestamp_ms`) seen per `(topic, partition)`.

| Metric | Value |
|---|---|
| Tracked Messages | 11376766 |
| Tracked Datapoints | 413593326 |
| Missing Timestamp Messages | 0 |
| Missing Timestamp Datapoints | 0 |
| Overall Min TS | 2026-01-12T07:59:42.356Z |
| Overall Max TS | 2026-01-12T11:29:39.835Z |
| Overall Window | 03:29:57.479 (12597479ms) |
| Tracked Msg/s (event time) | 903.10 |
| Tracked DP/s (event time) | 32831.44 |

| Topic | Partition | Messages | Datapoints | Min TS | Max TS | Window | Msg/s | DP/s |
|---|---:|---:|---:|---|---|---|---:|---:|
| otlp_metrics | 0 | 436310 | 16454747 | 2026-01-12T08:08:25.354Z | 2026-01-12T11:25:52.613Z | 03:17:27.259 | 36.83 | 1388.91 |
| otlp_metrics | 1 | 436445 | 16345533 | 2026-01-12T08:10:00.324Z | 2026-01-12T11:21:34.835Z | 03:11:34.511 | 37.97 | 1422.03 |
| otlp_metrics | 2 | 513753 | 17206419 | 2026-01-12T08:02:50.415Z | 2026-01-12T11:17:57.711Z | 03:15:07.296 | 43.88 | 1469.72 |
| otlp_metrics | 3 | 388885 | 15300959 | 2026-01-12T08:08:59.550Z | 2026-01-12T11:19:07.712Z | 03:10:08.162 | 34.09 | 1341.23 |
| otlp_metrics | 4 | 423242 | 13747492 | 2026-01-12T08:07:44.703Z | 2026-01-12T11:17:28.664Z | 03:09:43.961 | 37.18 | 1207.62 |
| otlp_metrics | 5 | 383141 | 14264584 | 2026-01-12T08:13:51.459Z | 2026-01-12T11:12:29.104Z | 02:58:37.645 | 35.75 | 1330.94 |
| otlp_metrics | 6 | 420022 | 17379564 | 2026-01-12T08:09:15.191Z | 2026-01-12T11:15:00.681Z | 03:05:45.490 | 37.69 | 1559.34 |
| otlp_metrics | 7 | 367549 | 12858996 | 2026-01-12T08:05:03.273Z | 2026-01-12T11:24:56.663Z | 03:19:53.390 | 30.65 | 1072.17 |
| otlp_metrics | 8 | 400693 | 15813811 | 2026-01-12T08:12:13.299Z | 2026-01-12T11:16:12.716Z | 03:03:59.417 | 36.30 | 1432.49 |
| otlp_metrics | 9 | 611504 | 18256979 | 2026-01-12T08:02:27.337Z | 2026-01-12T11:16:34.506Z | 03:14:07.169 | 52.50 | 1567.50 |
| otlp_metrics | 10 | 781231 | 25292295 | 2026-01-12T08:04:12.848Z | 2026-01-12T11:26:35.999Z | 03:22:23.151 | 64.34 | 2082.84 |
| otlp_metrics | 11 | 548388 | 21178357 | 2026-01-12T08:11:13.678Z | 2026-01-12T11:23:00.738Z | 03:11:47.060 | 47.66 | 1840.47 |
| otlp_metrics | 12 | 431966 | 13897053 | 2026-01-12T08:07:01.825Z | 2026-01-12T11:25:43.348Z | 03:18:41.523 | 36.23 | 1165.71 |
| otlp_metrics | 13 | 512588 | 15935249 | 2026-01-12T08:04:02.359Z | 2026-01-12T11:22:41.753Z | 03:18:39.394 | 43.00 | 1336.92 |
| otlp_metrics | 14 | 355092 | 12685332 | 2026-01-12T07:59:42.356Z | 2026-01-12T11:23:24.862Z | 03:23:42.506 | 29.05 | 1037.87 |
| otlp_metrics | 15 | 653843 | 29700692 | 2026-01-12T08:01:37.261Z | 2026-01-12T11:21:21.417Z | 03:19:44.156 | 54.56 | 2478.33 |
| otlp_metrics | 16 | 493127 | 14514911 | 2026-01-12T08:02:50.444Z | 2026-01-12T11:20:12.431Z | 03:17:21.987 | 41.64 | 1225.72 |
| otlp_metrics | 17 | 435208 | 16577939 | 2026-01-12T08:02:32.693Z | 2026-01-12T11:21:27.991Z | 03:18:55.298 | 36.46 | 1388.98 |
| otlp_metrics | 18 | 607988 | 27001615 | 2026-01-12T08:10:22.831Z | 2026-01-12T11:29:39.835Z | 03:19:17.004 | 50.85 | 2258.23 |
| otlp_metrics | 19 | 495813 | 18307881 | 2026-01-12T08:09:00.872Z | 2026-01-12T11:27:18.894Z | 03:18:18.022 | 41.67 | 1538.73 |
| otlp_metrics | 20 | 497773 | 16106015 | 2026-01-12T08:07:10.210Z | 2026-01-12T11:16:06.476Z | 03:08:56.266 | 43.91 | 1420.75 |
| otlp_metrics | 21 | 439565 | 17097780 | 2026-01-12T08:02:27.544Z | 2026-01-12T11:19:33.768Z | 03:17:06.224 | 37.17 | 1445.75 |
| otlp_metrics | 22 | 368843 | 13592851 | 2026-01-12T08:10:42.823Z | 2026-01-12T11:16:26.595Z | 03:05:43.772 | 33.10 | 1219.77 |
| otlp_metrics | 23 | 373797 | 14076272 | 2026-01-12T08:01:31.960Z | 2026-01-12T11:22:25.903Z | 03:20:53.943 | 31.01 | 1167.77 |

### Latency Statistics

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Message Total | 11376766 | 77.866µs | 327.766µs | 410ns | 538.671167ms | 8.707µs | 15.302µs | 653.083µs | 1.448757ms |
| DP Total | 11376766 | 1.938µs | 36.058µs | 352ns | 107.734233ms | 1.952µs | 2.258µs | 2.919µs | 3.961µs |
| DP Intern | 11376766 | 1.651µs | 36.055µs | 277ns | 107.732661ms | 1.677µs | 1.937µs | 2.49µs | 3.366µs |
| DP Build | 11376766 | 286ns | 196ns | 52ns | 107.05µs | 273ns | 327ns | 431ns | 612ns |
| DPs per Msg | 11376766 | 36.35 | 114.40 | 1 | 3998 | 5 | 7 | 473 | 500 |

### Label Tag Statistics

Computed by scanning the `LabelSetStore`: `series_scanned=79005309` out of `series_total=79005309`.

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Labels per Series | 79005309 | 23.41 | 8.52 | 3 | 64 | 23 | 28 | 38 | 47 |
| Avg Key Length/Series (B/label) | 79005309 | 13.33 | 2.66 | 6 | 32 | 13 | 14 | 19 | 23 |
| Avg Value Length/Series (B/label) | 79005309 | 13.22 | 3.84 | 5 | 76 | 12 | 15 | 20 | 22 |
| Key Len Max/Series (B) | 79005309 | 28.98 | 7.31 | 12 | 71 | 29 | 29 | 44 | 63 |
| Value Len Max/Series (B) | 79005309 | 64.29 | 51.77 | 11 | 2048 | 46 | 66 | 191 | 222 |
| Key Total Bytes/Series | 79005309 | 337.90 | 174.49 | 24 | 1827 | 329 | 378 | 684 | 919 |
| Value Total Bytes/Series | 79005309 | 337.65 | 196.73 | 32 | 2692 | 293 | 367 | 712 | 845 |
| LabelSet Total Bytes/Series | 79005309 | 675.55 | 361.26 | 56 | 4016 | 611 | 751 | 1364 | 1757 |

## Store Statistics

| Metric | Value |
|---|---|
| Store Kind | KeySetDictEncoded |
| Symbol Table | ArenaSymbolTable |
| Series Count | 79005309 |
| Allocated Bytes | 14621358208 |
| Used Bytes | 9538832603 |
| Allocated Bytes/Series | 185.07 |
| Used Bytes/Series | 120.74 |
| Symbols | 2621843 |

## Buffer Statistics

| Metric | Value |
|---|---|
| type | chronoxide_core::labels::interners::KeySetLabelSetStoreBufferStats |
| by_hash_len | 79005309 |
| by_hash_cap | 117440512 |
| by_hash_collisions_len | 0 |
| by_hash_collisions_cap | 0 |
| series_len | 79005309 |
| series_cap | 134217728 |
| per_keyset_rows_len | 5707 |
| per_keyset_rows_cap | 8192 |
| per_keyset_values_len | 1849755097 |
| per_keyset_values_cap | 2731829775 |
| value_dicts_len | 1792 |
| value_dicts_cap | 1792 |
| sum_per_key_cardinality | 3101759 |
| global_distinct_values | 2620274 |
| keysets_len | 5707 |
| keysets_cap | 8192 |
| keyset_to_id_len | 5707 |
| keyset_to_id_cap | 7168 |

## Symbol Table Statistics

| Metric | Value |
|---|---|
| kind | arena |
| symbols | 2621843 |
| hash_to_id_len | 2621843 |
| hash_to_id_cap | 3670016 |
| hash_collisions_len | 0 |
| hash_collisions_cap | 0 |
| arena_len | 147606309 |
| arena_cap | 176160768 |
| id_to_loc_len | 2621843 |
| id_to_loc_cap | 4194304 |

## Report Generation Timing

| Metric | Value |
|---|---|
| Report Build Time (no file I/O) | 131.627146622s |
| Accounted Time | 131.627123362s |
| Unaccounted Time | 23.26µs |
| Store Stats Snapshot Time | 19.95µs |
| General Stats Build Time | 1.43µs |
| Partition Watermarks Build Time | 19.69µs |
| Latency Stats Total Time | 246.52µs |
| Latency Stats Markdown Build Time | 246.4µs |
| Latency Stats Markdown Append Time | 50ns |
| Label Tag Stats Total Time | 5.113676376s |
| Label Tag Stats Compute Time | 5.113301416s |
| Label Tag Stats Markdown Build Time | 374.85µs |
| Label Tag Stats Markdown Append Time | 110ns |
| Per-Key Stats Total Time | 126.513148936s |
| Per-Key Stats Build Time | 126.513148936s |
| Store Stats Section Build Time | 2.15µs |
| Buffer Stats Section Build Time | 6.77µs |
| Symbol Table Stats Section Build Time | 1.54µs |
