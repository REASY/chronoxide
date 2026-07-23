# Cold value reverse-map attribution

**Status:** rejected as a memory candidate before implementation. Fresh
allocation-site attribution shows that replacing the cold-plan
`BTreeMap<value_symbol, code>` reverse dictionaries cannot materially lower
either the process-wide maximum or the later Series-stage crest.

## Decision

Do not build a binary-search or compact-hash comparator for memory reduction.

During the largest segment, the complete inner and outer reverse maps retained
6,260,528 bytes (5.970 MiB) across 52,822 allocations. They were fully live at
the affected Series-stage maximum of 2,836,394,141 bytes, so even complete
elimination has an upper bound of only 0.221% of that crest. Removing the
target allocations from the raw event timeline shifts the remaining maximum
later and lowers it by only 6,187,768 bytes (5.901 MiB).

The process-wide requested-live maximum occurred at 74.345 seconds. The
largest-segment reverse maps did not begin until 80.762 seconds, so this
candidate cannot affect the process-wide maximum.

## Attribution

The analysis used the frozen packed-row trace and exact source selectors:

```text
TRACE=heaptrack-candidate/output/heaptrack.trace.zst

heaptrack_site_timeline TRACE value_code_maps cold_v2.rs 500
heaptrack_site_timeline TRACE value_code_maps cold_v2.rs 504
```

Line 500 selects inner dictionary-node allocations at `codes.insert(...)`;
line 504 selects outer key-to-dictionary nodes. A second raw-event parser
combines both sites and removes their live bytes from the declared
80.752-85.236-second packed-buffer window without conflating them with
suppressed process totals.

| Largest-segment site | Calls | Allocated/live bytes | Lifetime |
| --- | ---: | ---: | ---: |
| Inner value-to-code nodes | 52,562 | 6,173,776 B | 80.762-82.191 s |
| Outer key-to-map nodes | 260 | 86,752 B | 80.762-82.191 s |
| Combined | 52,822 | 6,260,528 B | 80.762-82.191 s |

Across all four segments, the two sites made 53,255 allocations and requested
6,314,680 bytes. That is only 0.022% of the replay's 240,159,535 allocation
calls.

The raw parser reports a 2,836,468,973-byte process maximum inside the packed
window at 82.181 seconds, with all 6,260,528 target bytes live. Subtracting
the maps from every event moves the adjusted maximum to 85.236 seconds at
2,830,281,205 bytes. Official Heaptrack Massif values remain the authority for
process comparisons; raw events are used here only for exact site attribution
and the counterfactual window.

## Why no comparator

The already-sorted value dictionaries make binary search the least invasive
alternative. It would preserve each code as the value's sorted ordinal and
could reuse one small key-to-slice directory. It would nevertheless change
the lookup executed for all 88,864,686 logical codes in the largest segment
to remove at most about 6.2 MB.

The workload spans both larger dictionaries, where contiguous binary search
may outperform tree traversal, and cardinalities one through four. A CPU
improvement is therefore plausible but unproved and is not a memory
justification. Revisit only if a fresh quiet-host CPU profile identifies
reverse-map lookup itself as material.

A future CPU comparator must preserve:

- strictly increasing dictionary keys and values;
- `code == sorted dictionary ordinal`;
- missing-key and missing-value errors;
- partial-row rollback and width-boundary checks;
- byte-identical cold sections and complete corpus output; and
- footer validation plus independent readback equivalence.

## Evidence

The frozen evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/packed-cold-rows-memory-20260723T192245Z-tdUWIy`

Machine-readable results, parser source, parser binary, and the filtered
Heaptrack report are retained under `metadata/analysis/`:

- `value-code-maps-attribution.json`;
- `value-code-maps-inner-nodes.json`;
- `value-code-maps-outer-nodes.json`;
- `value-code-maps-combined-window.json`;
- `value-code-maps-heaptrack-print.txt`;
- `heaptrack_site_timeline.cpp`; and
- `heaptrack_site_window.cpp`.

`attribution-sha256sums.txt` covers all 16 new attribution artifacts and
verifies cleanly; its SHA-256 is
`00d5ccc9e0ed440beba28fd9f588ee109267b14e6426b24622ffeebe5a3fadcb`.

No generated segment corpus or query binary was recreated for this
attribution.
