# Ingestion Statistics

## General Stats

| Metric | Value |
|---|---|
| Total Messages | 11376766 |
| Total OTLP Metric Records | 84143299 |
| Total Unique Metrics (`__name__`) | 20042 |
| Total Series (unique label sets) | 75294581 |
| Total Datapoints | 427040038 |
| Total Processing Time | 654.976297526s |
| Total Intern Time | 521.40523257s |
| Skipped Non-Scalar | 0 |

## Partition Watermarks

Based on Kafka record timestamps (`timestamp_ms`) seen per `(topic, partition)`.

| Metric | Value |
|---|---|
| Tracked Messages | 11376766 |
| Tracked Datapoints | 427040038 |
| Missing Timestamp Messages | 0 |
| Missing Timestamp Datapoints | 0 |
| Overall Min TS | 2026-01-18T05:10:09.342Z |
| Overall Max TS | 2026-01-18T06:04:28.223Z |
| Overall Window | 00:54:18.881 (3258881ms) |
| Tracked Msg/s (event time) | 3491.00 |
| Tracked DP/s (event time) | 131038.86 |

| Topic | Partition | Messages | Datapoints | Min TS | Max TS | Window | Msg/s | DP/s |
|---|---:|---:|---:|---|---|---|---:|---:|
| otlp_metrics | 0 | 414731 | 15865946 | 2026-01-18T05:10:09.342Z | 2026-01-18T05:52:23.754Z | 00:42:14.412 | 163.64 | 6260.21 |
| otlp_metrics | 1 | 453874 | 15832622 | 2026-01-18T05:14:07.854Z | 2026-01-18T05:58:04.979Z | 00:43:57.125 | 172.11 | 6003.74 |
| otlp_metrics | 2 | 572808 | 22000035 | 2026-01-18T05:13:38.047Z | 2026-01-18T05:59:53.510Z | 00:46:15.463 | 206.38 | 7926.62 |
| otlp_metrics | 3 | 305959 | 11886251 | 2026-01-18T05:20:33.210Z | 2026-01-18T05:57:40.000Z | 00:37:06.790 | 137.40 | 5337.84 |
| otlp_metrics | 4 | 399259 | 13915299 | 2026-01-18T05:17:57.240Z | 2026-01-18T05:55:19.694Z | 00:37:22.454 | 178.05 | 6205.39 |
| otlp_metrics | 5 | 326595 | 11743503 | 2026-01-18T05:15:59.448Z | 2026-01-18T05:53:14.588Z | 00:37:15.140 | 146.12 | 5254.03 |
| otlp_metrics | 6 | 435339 | 15750696 | 2026-01-18T05:15:50.733Z | 2026-01-18T05:56:21.671Z | 00:40:30.938 | 179.08 | 6479.27 |
| otlp_metrics | 7 | 319551 | 11279102 | 2026-01-18T05:20:16.194Z | 2026-01-18T06:00:01.045Z | 00:39:44.851 | 133.99 | 4729.48 |
| otlp_metrics | 8 | 322954 | 13628759 | 2026-01-18T05:21:38.775Z | 2026-01-18T05:59:47.046Z | 00:38:08.271 | 141.13 | 5955.92 |
| otlp_metrics | 9 | 626514 | 19131539 | 2026-01-18T05:20:37.180Z | 2026-01-18T06:01:10.070Z | 00:40:32.890 | 257.52 | 7863.71 |
| otlp_metrics | 10 | 953887 | 31568874 | 2026-01-18T05:19:12.034Z | 2026-01-18T05:58:17.072Z | 00:39:05.038 | 406.77 | 13461.99 |
| otlp_metrics | 11 | 554023 | 23138590 | 2026-01-18T05:18:13.368Z | 2026-01-18T05:58:29.575Z | 00:40:16.207 | 229.29 | 9576.41 |
| otlp_metrics | 12 | 422926 | 13764406 | 2026-01-18T05:20:10.730Z | 2026-01-18T05:56:08.812Z | 00:35:58.082 | 195.97 | 6378.07 |
| otlp_metrics | 13 | 472863 | 15637175 | 2026-01-18T05:18:03.922Z | 2026-01-18T05:57:47.836Z | 00:39:43.914 | 198.36 | 6559.45 |
| otlp_metrics | 14 | 297225 | 11428170 | 2026-01-18T05:17:13.187Z | 2026-01-18T05:57:29.780Z | 00:40:16.593 | 122.99 | 4729.04 |
| otlp_metrics | 15 | 800901 | 34195992 | 2026-01-18T05:18:27.317Z | 2026-01-18T06:04:15.337Z | 00:45:48.020 | 291.45 | 12443.87 |
| otlp_metrics | 16 | 493787 | 14329553 | 2026-01-18T05:22:25.558Z | 2026-01-18T06:00:51.652Z | 00:38:26.094 | 214.12 | 6213.78 |
| otlp_metrics | 17 | 348176 | 13627446 | 2026-01-18T05:16:46.692Z | 2026-01-18T05:58:49.415Z | 00:42:02.723 | 138.02 | 5401.88 |
| otlp_metrics | 18 | 779599 | 36398680 | 2026-01-18T05:24:21.465Z | 2026-01-18T05:57:11.612Z | 00:32:50.147 | 395.71 | 18475.11 |
| otlp_metrics | 19 | 578939 | 21631575 | 2026-01-18T05:18:00.994Z | 2026-01-18T05:58:21.759Z | 00:40:20.765 | 239.16 | 8935.84 |
| otlp_metrics | 20 | 438519 | 17816163 | 2026-01-18T05:20:57.108Z | 2026-01-18T06:04:28.223Z | 00:43:31.115 | 167.94 | 6823.20 |
| otlp_metrics | 21 | 408536 | 17675024 | 2026-01-18T05:16:41.395Z | 2026-01-18T05:56:44.097Z | 00:40:02.702 | 170.03 | 7356.31 |
| otlp_metrics | 22 | 306025 | 13089190 | 2026-01-18T05:19:57.897Z | 2026-01-18T05:58:24.220Z | 00:38:26.323 | 132.69 | 5675.35 |
| otlp_metrics | 23 | 343776 | 11705448 | 2026-01-18T05:17:58.565Z | 2026-01-18T06:01:10.194Z | 00:43:11.629 | 132.65 | 4516.64 |

### Latency Statistics

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Message Total | 11376766 | 57.571µs | 253.754µs | 400ns | 471.93244ms | 6.399µs | 11.238µs | 484.575µs | 1.031024ms |
| DP Total | 11376766 | 1.384µs | 7.007µs | 255ns | 23.515182ms | 1.407µs | 1.623µs | 2.068µs | 2.764µs |
| DP Intern | 11376766 | 1.106µs | 7.002µs | 177ns | 23.51452ms | 1.133µs | 1.307µs | 1.674µs | 2.218µs |
| DP Build | 11376766 | 278ns | 133ns | 52ns | 31.91µs | 268ns | 320ns | 414ns | 574ns |
| DPs per Msg | 11376766 | 37.54 | 116.36 | 1 | 3998 | 5 | 7 | 491 | 500 |

### Label Tag Statistics

Computed by scanning the `LabelSetStore`: `series_scanned=75294581` out of `series_total=75294581`.

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Labels per Series | 75294581 | 23.05 | 8.40 | 3 | 64 | 23 | 27 | 38 | 46 |
| Avg Key Length/Series (B/label) | 75294581 | 13.12 | 2.42 | 6 | 32 | 13 | 14 | 18 | 22 |
| Avg Value Length/Series (B/label) | 75294581 | 13.02 | 3.73 | 5 | 68 | 12 | 15 | 20 | 22 |
| Key Len Max/Series (B) | 75294581 | 28.52 | 6.47 | 12 | 71 | 29 | 29 | 43 | 52 |
| Value Len Max/Series (B) | 75294581 | 60.09 | 48.17 | 11 | 1814 | 43 | 62 | 191 | 223 |
| Key Total Bytes/Series | 75294581 | 326.20 | 161.39 | 24 | 1827 | 328 | 360 | 632 | 918 |
| Value Total Bytes/Series | 75294581 | 326.35 | 187.98 | 32 | 2468 | 290 | 343 | 695 | 840 |
| LabelSet Total Bytes/Series | 75294581 | 652.55 | 339.19 | 56 | 4014 | 603 | 704 | 1307 | 1745 |

## Store Statistics

| Metric | Value |
|---|---|
| Store Kind | FlatInterned |
| Symbol Table | ArenaSymbolTable |
| Series Count | 75294581 |
| Allocated Bytes | 17569939704 |
| Used Bytes | 15852702341 |
| Allocated Bytes/Series | 233.35 |
| Used Bytes/Series | 210.54 |
| Symbols | 2100662 |

## Buffer Statistics

| Metric | Value |
|---|---|
| type | chronoxide_core::labels::interners::FlatInternedLabelSetStoreBufferStats |
| by_hash_len | 75294581 |
| by_hash_cap | 117440512 |
| by_hash_collisions_len | 0 |
| by_hash_collisions_cap | 0 |
| series_len | 75294581 |
| series_cap | 134217728 |
| key_values_len | 1735751582 |
| key_values_cap | 1744830464 |

## Symbol Table Statistics

| Metric | Value |
|---|---|
| kind | arena |
| symbols | 2100662 |
| hash_to_id_len | 2100662 |
| hash_to_id_cap | 3670016 |
| hash_collisions_len | 0 |
| hash_collisions_cap | 0 |
| arena_len | 113404929 |
| arena_cap | 159383552 |
| id_to_loc_len | 2100662 |
| id_to_loc_cap | 4194304 |

## Report Generation Timing

| Metric | Value |
|---|---|
| Report Build Time (no file I/O) | 74.414485313s |
| Accounted Time | 74.414465503s |
| Unaccounted Time | 19.81µs |
| Store Stats Snapshot Time | 1.01µs |
| General Stats Build Time | 1.49µs |
| Partition Watermarks Build Time | 18.631µs |
| Latency Stats Total Time | 252.301µs |
| Latency Stats Markdown Build Time | 252.211µs |
| Latency Stats Markdown Append Time | 40ns |
| Label Tag Stats Total Time | 2.55157897s |
| Label Tag Stats Compute Time | 2.551255058s |
| Label Tag Stats Markdown Build Time | 323.872µs |
| Label Tag Stats Markdown Append Time | 40ns |
| Per-Key Stats Total Time | 71.862606001s |
| Per-Key Stats Build Time | 71.862606001s |
| Store Stats Section Build Time | 2.07µs |
| Buffer Stats Section Build Time | 3.24µs |
| Symbol Table Stats Section Build Time | 1.79µs |
| Packed KeySet Stats Build Time | 0ns |
| Bit-Packed KeySet Stats Build Time | 0ns |
