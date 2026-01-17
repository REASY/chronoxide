# Ingestion Statistics

## General Stats

| Metric | Value |
|---|---|
| Total Messages | 400000 |
| Total OTLP Metric Records | 2988593 |
| Total Unique Metrics (`__name__`) | 13840 |
| Total Series (unique label sets) | 13801586 |
| Total Datapoints | 14548242 |
| Total Processing Time | 29.643892284s |
| Total Intern Time | 24.480683756s |
| Skipped Non-Scalar | 0 |

## Partition Watermarks

Based on Kafka record timestamps (`timestamp_ms`) seen per `(topic, partition)`.

| Metric | Value |
|---|---|
| Tracked Messages | 400000 |
| Tracked Datapoints | 14548242 |
| Missing Timestamp Messages | 0 |
| Missing Timestamp Datapoints | 0 |
| Overall Min TS | 2026-01-12T07:59:42.356Z |
| Overall Max TS | 2026-01-12T08:14:03.258Z |
| Overall Window | 00:14:20.902 (860902ms) |
| Tracked Msg/s (event time) | 464.63 |
| Tracked DP/s (event time) | 16898.84 |

| Topic | Partition | Messages | Datapoints | Min TS | Max TS | Window | Msg/s | DP/s |
|---|---:|---:|---:|---|---|---|---:|---:|
| otlp_metrics | 0 | 18512 | 605068 | 2026-01-12T08:08:25.354Z | 2026-01-12T08:08:37.701Z | 00:00:12.347 | 1499.31 | 49005.26 |
| otlp_metrics | 1 | 13306 | 511983 | 2026-01-12T08:10:00.324Z | 2026-01-12T08:10:12.606Z | 00:00:12.282 | 1083.37 | 41685.64 |
| otlp_metrics | 2 | 21974 | 791337 | 2026-01-12T08:02:50.415Z | 2026-01-12T08:03:05.569Z | 00:00:15.154 | 1450.05 | 52219.68 |
| otlp_metrics | 3 | 20073 | 551905 | 2026-01-12T08:08:59.550Z | 2026-01-12T08:09:14.181Z | 00:00:14.631 | 1371.95 | 37721.62 |
| otlp_metrics | 4 | 14875 | 503496 | 2026-01-12T08:07:44.703Z | 2026-01-12T08:07:59.254Z | 00:00:14.551 | 1022.27 | 34602.16 |
| otlp_metrics | 5 | 16178 | 845977 | 2026-01-12T08:13:51.459Z | 2026-01-12T08:14:03.258Z | 00:00:11.799 | 1371.13 | 71699.04 |
| otlp_metrics | 6 | 8322 | 375716 | 2026-01-12T08:09:15.191Z | 2026-01-12T08:09:24.787Z | 00:00:09.596 | 867.24 | 39153.40 |
| otlp_metrics | 7 | 6429 | 221141 | 2026-01-12T08:05:03.273Z | 2026-01-12T08:05:09.324Z | 00:00:06.051 | 1062.47 | 36546.19 |
| otlp_metrics | 8 | 19646 | 656380 | 2026-01-12T08:12:13.299Z | 2026-01-12T08:12:27.916Z | 00:00:14.617 | 1344.05 | 44905.25 |
| otlp_metrics | 9 | 19426 | 527496 | 2026-01-12T08:02:27.337Z | 2026-01-12T08:02:38.874Z | 00:00:11.537 | 1683.80 | 45722.11 |
| otlp_metrics | 10 | 32294 | 1028632 | 2026-01-12T08:04:12.848Z | 2026-01-12T08:04:34.552Z | 00:00:21.704 | 1487.93 | 47393.66 |
| otlp_metrics | 11 | 12788 | 511924 | 2026-01-12T08:11:13.678Z | 2026-01-12T08:11:25.490Z | 00:00:11.812 | 1082.63 | 43339.32 |
| otlp_metrics | 12 | 13668 | 604553 | 2026-01-12T08:07:01.825Z | 2026-01-12T08:07:12.932Z | 00:00:11.107 | 1230.58 | 54429.91 |
| otlp_metrics | 13 | 8338 | 373564 | 2026-01-12T08:04:02.359Z | 2026-01-12T08:04:09.314Z | 00:00:06.955 | 1198.85 | 53711.57 |
| otlp_metrics | 14 | 10275 | 216716 | 2026-01-12T07:59:42.356Z | 2026-01-12T07:59:49.803Z | 00:00:07.447 | 1379.75 | 29101.11 |
| otlp_metrics | 15 | 32751 | 1505441 | 2026-01-12T08:01:37.261Z | 2026-01-12T08:02:01.932Z | 00:00:24.671 | 1327.51 | 61020.67 |
| otlp_metrics | 16 | 13894 | 421639 | 2026-01-12T08:02:50.444Z | 2026-01-12T08:03:00.697Z | 00:00:10.253 | 1355.12 | 41123.48 |
| otlp_metrics | 17 | 16711 | 634918 | 2026-01-12T08:02:32.693Z | 2026-01-12T08:02:42.802Z | 00:00:10.109 | 1653.08 | 62807.20 |
| otlp_metrics | 18 | 29897 | 1279540 | 2026-01-12T08:10:22.831Z | 2026-01-12T08:10:43.223Z | 00:00:20.392 | 1466.11 | 62747.16 |
| otlp_metrics | 19 | 22742 | 872912 | 2026-01-12T08:09:00.872Z | 2026-01-12T08:09:16.754Z | 00:00:15.882 | 1431.94 | 54962.35 |
| otlp_metrics | 20 | 15712 | 548793 | 2026-01-12T08:07:10.210Z | 2026-01-12T08:07:16.358Z | 00:00:06.148 | 2555.63 | 89263.66 |
| otlp_metrics | 21 | 13685 | 376273 | 2026-01-12T08:02:27.544Z | 2026-01-12T08:02:36.138Z | 00:00:08.594 | 1592.39 | 43783.22 |
| otlp_metrics | 22 | 10315 | 237043 | 2026-01-12T08:10:42.823Z | 2026-01-12T08:10:48.416Z | 00:00:05.593 | 1844.27 | 42382.08 |
| otlp_metrics | 23 | 8189 | 345795 | 2026-01-12T08:01:31.960Z | 2026-01-12T08:01:36.554Z | 00:00:04.594 | 1782.54 | 75271.01 |

### Latency Statistics

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Message Total | 400000 | 74.109µs | 284.651µs | 530ns | 62.890659ms | 8.296µs | 14.315µs | 600.329µs | 1.404421ms |
| DP Total | 400000 | 1.807µs | 2.009µs | 365ns | 1.097036ms | 1.823µs | 2.1µs | 2.733µs | 3.69µs |
| DP Intern | 400000 | 1.498µs | 1.974µs | 292ns | 1.096606ms | 1.521µs | 1.753µs | 2.28µs | 3.011µs |
| DP Build | 400000 | 309ns | 212ns | 57ns | 18.099µs | 296ns | 350ns | 461ns | 638ns |
| DPs per Msg | 400000 | 36.37 | 114.66 | 1 | 3998 | 5 | 7 | 474 | 500 |

### Label Tag Statistics

Computed by scanning the `LabelSetStore`: `series_scanned=13801586` out of `series_total=13801586`.

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| Labels per Series | 13801586 | 21.91 | 7.73 | 3 | 64 | 21 | 26 | 34 | 45 |
| Avg Key Length/Series (B/label) | 13801586 | 12.95 | 2.09 | 6 | 31 | 13 | 14 | 15 | 20 |
| Avg Value Length/Series (B/label) | 13801586 | 12.71 | 3.56 | 5 | 46 | 12 | 15 | 19 | 22 |
| Key Len Max/Series (B) | 13801586 | 28.36 | 5.87 | 12 | 67 | 29 | 29 | 43 | 49 |
| Value Len Max/Series (B) | 13801586 | 53.63 | 40.00 | 11 | 624 | 40 | 55 | 141 | 200 |
| Key Total Bytes/Series | 13801586 | 304.03 | 140.33 | 24 | 1740 | 291 | 360 | 554 | 817 |
| Value Total Bytes/Series | 13801586 | 300.27 | 165.83 | 32 | 2330 | 264 | 326 | 664 | 780 |
| LabelSet Total Bytes/Series | 13801586 | 604.30 | 296.06 | 56 | 4016 | 566 | 681 | 1226 | 1571 |

## Store Statistics

| Metric | Value |
|---|---|
| Store Kind | KeySetDictEncoded |
| Symbol Table | ArenaSymbolTable |
| Series Count | 13801586 |
| Allocated Bytes | 2299344920 |
| Used Bytes | 1582566681 |
| Allocated Bytes/Series | 166.60 |
| Used Bytes/Series | 114.67 |
| Symbols | 563571 |

## Buffer Statistics

| Metric | Value |
|---|---|
| type | chronoxide_core::labels::interners::KeySetLabelSetStoreBufferStats |
| by_hash_len | 13801586 |
| by_hash_cap | 14680064 |
| by_hash_collisions_len | 0 |
| by_hash_collisions_cap | 0 |
| series_len | 13801586 |
| series_cap | 16777216 |
| per_keyset_rows_len | 4341 |
| per_keyset_rows_cap | 8192 |
| per_keyset_values_len | 302333349 |
| per_keyset_values_cap | 454725779 |
| value_dicts_len | 1561 |
| value_dicts_cap | 1792 |
| total_cardinality | 690414 |
| keysets_len | 4341 |
| keysets_cap | 8192 |
| keyset_to_id_len | 4341 |
| keyset_to_id_cap | 7168 |

## Symbol Table Statistics

| Metric | Value |
|---|---|
| kind | arena |
| symbols | 563571 |
| hash_to_id_len | 563571 |
| hash_to_id_cap | 917504 |
| hash_collisions_len | 0 |
| hash_collisions_cap | 0 |
| arena_len | 20411739 |
| arena_cap | 22020096 |
| id_to_loc_len | 563571 |
| id_to_loc_cap | 1048576 |

## Report Generation Timing

| Metric | Value |
|---|---|
| Report Build Time (no file I/O) | 20.851125657s |
| Accounted Time | 20.851098726s |
| Unaccounted Time | 26.931µs |
| Store Stats Snapshot Time | 19.14µs |
| General Stats Build Time | 1.83µs |
| Partition Watermarks Build Time | 18.18µs |
| Latency Stats Total Time | 299.565µs |
| Latency Stats Markdown Build Time | 299.445µs |
| Latency Stats Markdown Append Time | 50ns |
| Label Tag Stats Total Time | 796.409169ms |
| Label Tag Stats Compute Time | 796.061153ms |
| Label Tag Stats Markdown Build Time | 347.976µs |
| Label Tag Stats Markdown Append Time | 40ns |
| Per-Key Stats Total Time | 20.054340612s |
| Per-Key Stats Build Time | 20.054340612s |
| Store Stats Section Build Time | 2.12µs |
| Buffer Stats Section Build Time | 6.82µs |
| Symbol Table Stats Section Build Time | 1.29µs |
