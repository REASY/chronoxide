use super::*;

impl OtlpLabelSetProcessor {
    pub(super) fn write_markdown_report(&mut self) {
        let report_start = Instant::now();
        let ingestion = self.labelset_stats.snapshot();
        let store_stats_start = Instant::now();
        let store_stats = self.labelsets.stats();
        let store_stats_time = store_stats_start.elapsed();
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let filename = format!("ingestion_stats_{}.md", timestamp);
        let path = PathBuf::from(filename);
        let mut md = String::new();
        md.push_str("# Ingestion Statistics\n\n");

        let general_stats_start = Instant::now();
        md.push_str("## General Stats\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!(
            "| Total Messages | {} |\n",
            ingestion.totals.messages
        ));
        md.push_str(&format!(
            "| Total OTLP Metric Records | {} |\n",
            ingestion.totals.metrics
        ));
        md.push_str(&format!(
            "| Total Unique Metrics (`__name__`) | {} |\n",
            ingestion.totals.unique_metrics
        ));
        md.push_str(&format!(
            "| Total Series (unique label sets) | {} |\n",
            store_stats.series
        ));
        md.push_str(&format!(
            "| Observed OTLP Datapoints | {} |\n",
            ingestion.totals.observed_datapoints
        ));
        md.push_str(&format!(
            "| Accepted Datapoints | {} |\n",
            ingestion.totals.datapoints
        ));
        md.push_str(&format!(
            "| Total Processing Time | {:?} |\n",
            ingestion.totals.processing_time
        ));
        md.push_str(&format!(
            "| Total Intern Time | {:?} |\n",
            ingestion.totals.intern_time
        ));
        md.push_str(&format!(
            "| Skipped Non-Scalar | {} |\n",
            ingestion.totals.skipped_non_scalar_values
        ));
        md.push_str(&format!(
            "| Recorded Samples | {} |\n",
            ingestion.totals.datapoint_storage.recorded_samples
        ));
        md.push_str(&format!(
            "| Missing Number Value | {} |\n",
            ingestion.totals.datapoint_storage.missing_number_values
        ));
        md.push('\n');

        md.push_str(&datapoint_policy_counts_markdown(
            &ingestion.totals.datapoint_policy,
            &ingestion.window.datapoint_policy,
        ));
        md.push_str(&datapoint_storage_counts_markdown(
            &ingestion.totals.datapoint_storage,
            &ingestion.window.datapoint_storage,
            &ingestion.totals.datapoint_policy,
            &ingestion.window.datapoint_policy,
        ));
        md.push_str(&event_time_skew_markdown(&ingestion.totals.event_time_skew));
        let general_stats_time = general_stats_start.elapsed();

        let data_type_counts_start = Instant::now();
        md.push_str(&data_type_counts_markdown(
            &ingestion.totals.metric_types,
            &ingestion.totals.observed_datapoint_types,
            &ingestion.totals.datapoint_types,
        ));
        let data_type_counts_time = data_type_counts_start.elapsed();

        let partition_watermarks_start = Instant::now();
        if !ingestion.partition_watermarks.is_empty() {
            let mut rows = ingestion.partition_watermarks.clone();
            rows.sort_by(|((topic_a, part_a), _), ((topic_b, part_b), _)| {
                topic_a.cmp(topic_b).then_with(|| part_a.cmp(part_b))
            });

            let mut overall_min: Option<DateTime<Utc>> = None;
            let mut overall_max: Option<DateTime<Utc>> = None;
            let mut tracked_messages: u64 = 0;
            let mut tracked_datapoints: u64 = 0;

            for (_, wm) in &rows {
                overall_min = Some(overall_min.map_or(wm.min_ts, |cur| cur.min(wm.min_ts)));
                overall_max = Some(overall_max.map_or(wm.max_ts, |cur| cur.max(wm.max_ts)));
                tracked_messages = tracked_messages.saturating_add(wm.messages);
                tracked_datapoints = tracked_datapoints.saturating_add(wm.datapoints);
            }

            md.push_str("## Partition Watermarks\n\n");
            md.push_str(
                "Based on Kafka record timestamps (`timestamp_ms`) seen per `(topic, partition)`.\n\n",
            );
            md.push_str("| Metric | Value |\n|---|---|\n");
            md.push_str(&format!("| Tracked Messages | {} |\n", tracked_messages));
            md.push_str(&format!(
                "| Tracked Datapoints | {} |\n",
                tracked_datapoints
            ));
            md.push_str(&format!(
                "| Missing Timestamp Messages | {} |\n",
                ingestion.totals.messages.saturating_sub(tracked_messages)
            ));
            md.push_str(&format!(
                "| Missing Timestamp Datapoints | {} |\n",
                ingestion
                    .totals
                    .datapoints
                    .saturating_sub(tracked_datapoints)
            ));

            if let (Some(min_ts), Some(max_ts)) = (overall_min, overall_max) {
                let window_ms = (max_ts - min_ts).num_milliseconds();
                md.push_str(&format!(
                    "| Overall Min TS | {} |\n",
                    min_ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                md.push_str(&format!(
                    "| Overall Max TS | {} |\n",
                    max_ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                md.push_str(&format!(
                    "| Overall Window | {} ({}ms) |\n",
                    format_window_ms(window_ms),
                    window_ms
                ));
                if window_ms > 0 {
                    let window_s = window_ms as f64 / 1000.0;
                    md.push_str(&format!(
                        "| Tracked Msg/s (event time) | {:.2} |\n",
                        tracked_messages as f64 / window_s
                    ));
                    md.push_str(&format!(
                        "| Tracked DP/s (event time) | {:.2} |\n",
                        tracked_datapoints as f64 / window_s
                    ));
                }
            }
            md.push('\n');

            md.push_str("| Topic | Partition | Messages | Datapoints | Min TS | Max TS | Window | Msg/s | DP/s |\n");
            md.push_str("|---|---:|---:|---:|---|---|---|---:|---:|\n");
            for ((topic, partition), wm) in rows {
                let window_ms = wm.window_ms();
                let (msg_s, dp_s) = if window_ms > 0 {
                    let window_s = window_ms as f64 / 1000.0;
                    (
                        format!("{:.2}", wm.messages as f64 / window_s),
                        format!("{:.2}", wm.datapoints as f64 / window_s),
                    )
                } else {
                    ("n/a".to_string(), "n/a".to_string())
                };

                md.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    topic,
                    partition,
                    wm.messages,
                    wm.datapoints,
                    wm.min_ts
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    wm.max_ts
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    format_window_ms(window_ms),
                    msg_s,
                    dp_s
                ));
            }
            md.push('\n');
        }
        let partition_watermarks_time = partition_watermarks_start.elapsed();

        let latency_stats_start = Instant::now();
        let latency_md_build_start = Instant::now();
        let latency_md = self.labelset_stats.latency_samples().to_markdown();
        let latency_md_build_time = latency_md_build_start.elapsed();
        let latency_md_append_start = Instant::now();
        md.push_str(&latency_md);
        let latency_md_append_time = latency_md_append_start.elapsed();
        let latency_stats_time = latency_stats_start.elapsed();

        let head_stats_start = Instant::now();
        if !self.partition_heads.is_empty() {
            let mut partitions: Vec<_> = self.partition_heads.iter().collect();
            partitions.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut wrote_section = false;

            for (partition, state) in partitions {
                let dists = state.stats.distributions();
                let mut dist_rows = Vec::new();
                if let Some(dist) = dists.call_latency {
                    dist_rows.push(dist.to_markdown_row("head_call_latency"));
                }
                if let Some(dist) = dists.batch_sizes {
                    dist_rows.push(dist.to_markdown_row("batch_sizes"));
                }
                if let Some(dist) = dists.series_sample_counts {
                    dist_rows.push(dist.to_markdown_row("series_sample_counts"));
                }
                if let Some(dist) = dists.blocks_per_series {
                    dist_rows.push(dist.to_markdown_row("blocks_per_series"));
                }
                if let Some(dist) = dists.samples_per_block {
                    dist_rows.push(dist.to_markdown_row("samples_per_block"));
                }

                let density = state.stats.series_density();
                let table = state.stats.series_table_summary();
                if dist_rows.is_empty() && density.is_none() && table.is_none() {
                    continue;
                }

                if !wrote_section {
                    md.push_str("## Head Buffer Stats (by partition)\n\n");
                    wrote_section = true;
                }

                md.push_str(&format!("### Partition {}\n\n", partition));

                if !dist_rows.is_empty() {
                    md.push_str("#### Distributions\n\n");
                    md.push_str(
                        "| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n",
                    );
                    md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
                    for row in dist_rows {
                        md.push_str(&row);
                    }
                    md.push('\n');
                }

                if let Some(density) = density {
                    md.push_str("#### Series Density\n\n");
                    md.push_str("| Metric | Value |\n|---|---|\n");
                    md.push_str(&format!(
                        "| series_single_sample_count | {} |\n",
                        density.series_single_sample_count
                    ));
                    md.push_str(&format!(
                        "| series_single_sample_ratio | {:.3} |\n",
                        density.series_single_sample_ratio
                    ));
                    md.push_str(&format!(
                        "| series_multi_sample_count | {} |\n",
                        density.series_multi_sample_count
                    ));
                    md.push('\n');
                }

                if let Some(table) = table {
                    md.push_str("#### Series Table Structure\n\n");
                    md.push_str("| Metric | Value |\n|---|---:|\n");
                    md.push_str(&format!("| windows | {} |\n", table.windows));
                    md.push_str(&format!(
                        "| adaptive_windows | {} |\n",
                        table.adaptive_windows
                    ));
                    md.push_str(&format!("| series_total | {} |\n", table.series_total));
                    md.push_str(&format!(
                        "| direct_pages_total | {} |\n",
                        table.direct_pages_total
                    ));
                    md.push_str(&format!(
                        "| direct_series_total | {} |\n",
                        table.direct_series_total
                    ));
                    md.push_str(&format!(
                        "| direct_series_ratio | {:.6} |\n",
                        table.direct_series_ratio
                    ));
                    md.push_str(&format!(
                        "| sparse_pages_total | {} |\n",
                        table.sparse_pages_total
                    ));
                    md.push_str(&format!(
                        "| sparse_series_total | {} |\n",
                        table.sparse_series_total
                    ));
                    md.push_str(&format!(
                        "| refs_above_paged_limit_total | {} |\n",
                        table.refs_above_paged_limit_total
                    ));
                    md.push_str(&format!(
                        "| max_page_directory_len | {} |\n",
                        table.max_page_directory_len
                    ));
                    md.push_str(&format!(
                        "| max_page_directory_capacity | {} |\n",
                        table.max_page_directory_capacity
                    ));
                    md.push_str(&format!(
                        "| max_sparse_capacity | {} |\n",
                        table.max_sparse_capacity
                    ));
                    md.push_str(&format!(
                        "| max_sparse_slot_capacity | {} |\n",
                        table.max_sparse_slot_capacity
                    ));
                    md.push_str(&format!(
                        "| max_direct_slot_index_bytes | {} |\n",
                        table.max_direct_slot_index_bytes
                    ));
                    md.push_str(&format!(
                        "| max_direct_reverse_slot_capacity | {} |\n",
                        table.max_direct_reverse_slot_capacity
                    ));
                    md.push_str(&format!(
                        "| max_direct_value_capacity | {} |\n",
                        table.max_direct_value_capacity
                    ));
                    md.push('\n');
                }
            }
        }
        let head_stats_time = head_stats_start.elapsed();

        let label_tag_stats_compute_start = Instant::now();
        let label_tag_stats = match &self.labelsets {
            LabelSetInterner::Naive(store) => label_tag_stats_from_store(store, None),
            LabelSetInterner::FlatInterned(store) => label_tag_stats_from_store(store, None),
            LabelSetInterner::KeySetDictEncoded(store) => label_tag_stats_from_store(store, None),
        };
        let label_tag_stats_compute_time = label_tag_stats_compute_start.elapsed();
        let label_tag_stats_markdown_start = Instant::now();
        let label_tag_stats_md = label_tag_stats.to_markdown();
        let label_tag_stats_markdown_time = label_tag_stats_markdown_start.elapsed();
        let label_tag_stats_append_start = Instant::now();
        md.push_str(&label_tag_stats_md);
        let label_tag_stats_append_time = label_tag_stats_append_start.elapsed();

        let store_section_start = Instant::now();
        md.push_str("## Store Statistics\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!("| Store Kind | {} |\n", self.labelsets.kind()));
        let symbol_table_name = if store_stats.symbols.is_some() {
            std::any::type_name::<DefaultSymbolTable>()
                .split("::")
                .last()
                .unwrap_or("Unknown")
        } else {
            "None"
        };
        md.push_str(&format!("| Symbol Table | {} |\n", symbol_table_name));
        md.push_str(&format!("| Series Count | {} |\n", store_stats.series));
        md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            store_stats.alloc_bytes
        ));
        md.push_str(&format!("| Used Bytes | {} |\n", store_stats.used_bytes));

        let series = store_stats.series.max(1);
        let alloc_bits_per_series = store_stats.alloc_bytes as f64 / series as f64;
        let used_bits_per_series = store_stats.used_bytes as f64 / series as f64;
        md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            alloc_bits_per_series
        ));
        md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            used_bits_per_series
        ));

        if let Some(s) = store_stats.symbols {
            md.push_str(&format!("| Symbols | {} |\n", s));
        }
        md.push('\n');
        let store_section_time = store_section_start.elapsed();

        let buffer_stats_start = Instant::now();
        if let Some(bs) = &store_stats.buffer_stats {
            md.push_str("## Buffer Statistics\n\n");
            md.push_str("| Metric | Value |\n|---|---|\n");
            for part in bs.split_whitespace() {
                if let Some((k, v)) = part.split_once('=') {
                    md.push_str(&format!("| {} | {} |\n", k, v));
                }
            }
            md.push('\n');
        }
        let buffer_stats_time = buffer_stats_start.elapsed();

        let symbol_table_stats_start = Instant::now();
        if let Some(sts) = &store_stats.symbol_table_stats {
            md.push_str("## Symbol Table Statistics\n\n");
            md.push_str("| Metric | Value |\n|---|---|\n");
            for part in sts.split_whitespace() {
                if let Some((k, v)) = part.split_once('=') {
                    md.push_str(&format!("| {} | {} |\n", k, v));
                }
            }
            md.push('\n');
        }
        let symbol_table_stats_time = symbol_table_stats_start.elapsed();

        let per_key_stats_build_start = Instant::now();
        let per_key_stats_md = match &self.labelsets {
            LabelSetInterner::Naive(store) => per_key_value_stats_markdown_from_store(store, None),
            LabelSetInterner::FlatInterned(store) => {
                per_key_value_stats_markdown_from_store(store, None)
            }
            LabelSetInterner::KeySetDictEncoded(store) => {
                per_key_value_stats_markdown_from_store(store, None)
            }
        };

        let per_key_stats_build_time = per_key_stats_build_start.elapsed();

        let packed_stats_start = Instant::now();
        let mut bit_packed_stats_time = Duration::ZERO;
        let mut packed_stats_time = Duration::ZERO;
        let mut packed_stats_md = String::new();
        if let LabelSetInterner::KeySetDictEncoded(store) = &mut self.labelsets {
            packed_stats_md = Self::get_packed_stats(store);
            packed_stats_time = packed_stats_start.elapsed();

            let bit_packed_start = Instant::now();
            packed_stats_md += &Self::get_bit_packed_stats(store);
            bit_packed_stats_time = bit_packed_start.elapsed();
        }
        if !packed_stats_md.is_empty() {
            md.push_str(&packed_stats_md);
        }

        let label_tag_stats_total_time = label_tag_stats_compute_time
            .saturating_add(label_tag_stats_markdown_time)
            .saturating_add(label_tag_stats_append_time);
        // Use just build time, append time is super small
        let per_key_stats_total_time = per_key_stats_build_time;

        let report_build_time = report_start.elapsed();

        let accounted_time = store_stats_time
            .saturating_add(general_stats_time)
            .saturating_add(data_type_counts_time)
            .saturating_add(partition_watermarks_time)
            .saturating_add(latency_stats_time)
            .saturating_add(head_stats_time)
            .saturating_add(label_tag_stats_total_time)
            .saturating_add(per_key_stats_total_time)
            .saturating_add(store_section_time)
            .saturating_add(buffer_stats_time)
            .saturating_add(symbol_table_stats_time)
            .saturating_add(bit_packed_stats_time);
        let unaccounted_time = report_build_time.saturating_sub(accounted_time);

        md.push_str("## Report Generation Timing\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!(
            "| Report Build Time (no file I/O) | {:?} |\n",
            report_build_time
        ));
        md.push_str(&format!("| Accounted Time | {:?} |\n", accounted_time));
        md.push_str(&format!("| Unaccounted Time | {:?} |\n", unaccounted_time));
        md.push_str(&format!(
            "| Store Stats Snapshot Time | {:?} |\n",
            store_stats_time
        ));
        md.push_str(&format!(
            "| General Stats Build Time | {:?} |\n",
            general_stats_time
        ));
        md.push_str(&format!(
            "| Data Type Counts Build Time | {:?} |\n",
            data_type_counts_time
        ));
        md.push_str(&format!(
            "| Partition Watermarks Build Time | {:?} |\n",
            partition_watermarks_time
        ));
        md.push_str(&format!(
            "| Latency Stats Total Time | {:?} |\n",
            latency_stats_time
        ));
        md.push_str(&format!(
            "| Head Buffer Stats Build Time | {:?} |\n",
            head_stats_time
        ));
        md.push_str(&format!(
            "| Latency Stats Markdown Build Time | {:?} |\n",
            latency_md_build_time
        ));
        md.push_str(&format!(
            "| Latency Stats Markdown Append Time | {:?} |\n",
            latency_md_append_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Total Time | {:?} |\n",
            label_tag_stats_total_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Compute Time | {:?} |\n",
            label_tag_stats_compute_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Markdown Build Time | {:?} |\n",
            label_tag_stats_markdown_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Markdown Append Time | {:?} |\n",
            label_tag_stats_append_time
        ));
        md.push_str(&format!(
            "| Per-Key Stats Total Time | {:?} |\n",
            per_key_stats_total_time
        ));
        md.push_str(&format!(
            "| Per-Key Stats Build Time | {:?} |\n",
            per_key_stats_build_time
        ));
        md.push_str(&format!(
            "| Store Stats Section Build Time | {:?} |\n",
            store_section_time
        ));
        md.push_str(&format!(
            "| Buffer Stats Section Build Time | {:?} |\n",
            buffer_stats_time
        ));
        md.push_str(&format!(
            "| Symbol Table Stats Section Build Time | {:?} |\n",
            symbol_table_stats_time
        ));
        md.push_str(&format!(
            "| Packed KeySet Stats Build Time | {:?} |\n",
            packed_stats_time
        ));
        md.push_str(&format!(
            "| Bit-Packed KeySet Stats Build Time | {:?} |\n",
            bit_packed_stats_time
        ));
        md.push('\n');

        md.push_str(&per_key_stats_md);
        md.push('\n');

        if let Ok(mut file) = File::create(&path) {
            if let Err(e) = file.write_all(md.as_bytes()) {
                let report_total_time = report_start.elapsed();
                error!(
                    "Failed to write markdown report to {:?}: {} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                    path, e, report_total_time, report_build_time, per_key_stats_build_time
                );
            } else {
                let report_total_time = report_start.elapsed();
                info!(
                    "Markdown report written to {:?} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                    path, report_total_time, report_build_time, per_key_stats_build_time
                );
            }
        } else {
            let report_total_time = report_start.elapsed();
            error!(
                "Failed to create markdown report file at {:?} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                path, report_total_time, report_build_time, per_key_stats_build_time
            );
        }
    }

    fn get_bit_packed_stats(store: &KeySetDictEncodedLabelSetStore) -> String {
        let mut packed_stats_md: String = String::new();

        let bit_packed = store.seal_bit_packed();

        packed_stats_md.push_str("## Bit-Packed KeySet Store Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        packed_stats_md.push_str("| Store Kind | BitPackedKeySetDictEncoded |\n");
        packed_stats_md.push_str(&format!("| Series Count | {} |\n", bit_packed.len()));
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            bit_packed.estimate_size_bytes()
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes | {} |\n",
            bit_packed.estimate_used_bytes()
        ));
        let series = bit_packed.len().max(1) as f64;
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            bit_packed.estimate_size_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            bit_packed.estimate_used_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!("| Symbols | {} |\n", bit_packed.symbols().len()));
        packed_stats_md.push_str(&format!("| KeySets | {} |\n", bit_packed.keysets().len()));
        packed_stats_md.push('\n');

        let bit_packed_buffer_stats = bit_packed.buffer_stats();
        packed_stats_md.push_str("### Bit-Packed Buffer Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        for part in bit_packed_buffer_stats.to_string().split_whitespace() {
            if let Some((k, v)) = part.split_once('=') {
                packed_stats_md.push_str(&format!("| {} | {} |\n", k, v));
            }
        }
        packed_stats_md.push('\n');
        packed_stats_md
    }

    fn get_packed_stats(store: &KeySetDictEncodedLabelSetStore) -> String {
        let mut packed_stats_md: String = String::new();

        let packed = store.seal_fixed_width();

        packed_stats_md.push_str("## Packed KeySet Store Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        packed_stats_md.push_str("| Store Kind | PackedKeySetDictEncoded |\n");
        packed_stats_md.push_str(&format!("| Series Count | {} |\n", packed.len()));
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            packed.estimate_size_bytes()
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes | {} |\n",
            packed.estimate_used_bytes()
        ));
        let series = packed.len().max(1) as f64;
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            packed.estimate_size_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            packed.estimate_used_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!("| Symbols | {} |\n", packed.symbols().len()));
        packed_stats_md.push_str(&format!("| KeySets | {} |\n", packed.keysets().len()));
        packed_stats_md.push('\n');

        let packed_buffer_stats = packed.buffer_stats();
        packed_stats_md.push_str("### Packed Buffer Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        for part in packed_buffer_stats.to_string().split_whitespace() {
            if let Some((k, v)) = part.split_once('=') {
                packed_stats_md.push_str(&format!("| {} | {} |\n", k, v));
            }
        }
        packed_stats_md.push('\n');
        packed_stats_md
    }
}
