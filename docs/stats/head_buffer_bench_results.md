# Head buffer benchmark results (capture-driven)

Run configuration:
- Command: `cargo bench -p chronoxide-core --bench head_buffer`
- Env: `HEAD_BENCH_CAPTURE=chronoxide-otlp_all_11M.capture`
  `HEAD_BENCH_MAX_MESSAGES=20000`
  `HEAD_BENCH_MAX_SAMPLES=500000`
  `HEAD_BENCH_SERIES=128`
- Head window: 1h
- Blocks per bench: 16
- Read benches: 1 series, samples per iter = `block_size * 16`
- Write benches: 128 series, samples per iter = `block_size * 16` total
- Arena page: 4 MiB

## Findings

- Read path: Gorilla adds ~5.7x to 7.2x decode cost vs raw floats, while ZigZag adds ~1.1x to 1.8x vs raw ints.
- Write path: Gorilla adds ~1.7x to 3.7x encode cost vs raw floats, while ZigZag is near parity for block sizes >= 1024.
- Payload savings: Gorilla shrinks float payload by ~13% to 20%; ZigZag shrinks int payload by ~59% to 70%.
- HeadWindow overhead is stable (metadata): ~984 bytes for single-series read windows and ~11,264 bytes for 128-series write windows.
- Arena slack dominates total window slack (~3.4 to 4.1 MiB) due to the fixed 4 MiB arena page.

## Read performance (ns/sample, median time)

| type | encoding | block_size | raw ns/sample | enc ns/sample | enc/raw | raw median time (us) | enc median time (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| float | gorilla | 256 | 5.63 | 34.91 | 6.20x | 17.023 | 101.96 |
| float | gorilla | 1024 | 5.00 | 28.50 | 5.70x | 76.247 | 434.20 |
| float | gorilla | 4096 | 5.05 | 36.36 | 7.20x | 317.62 | 1751.9 |
| int | delta_zigzag | 256 | 4.99 | 8.85 | 1.77x | 16.467 | 19.774 |
| int | delta_zigzag | 1024 | 4.19 | 4.80 | 1.15x | 61.444 | 73.854 |
| int | delta_zigzag | 4096 | 4.82 | 5.67 | 1.18x | 283.51 | 302.29 |

## Read HeadWindow size (series=1)

| type | encoding | block_size | est bytes raw | est bytes enc | payload bytes raw | payload bytes enc | payload savings |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| float | gorilla | 256 | 45837 | 37882 | 44853 | 36898 | 17.7% |
| float | gorilla | 1024 | 190970 | 164510 | 189986 | 163526 | 13.9% |
| float | gorilla | 4096 | 779987 | 680319 | 779003 | 679335 | 12.8% |
| int | delta_zigzag | 256 | 46003 | 17353 | 45019 | 16369 | 63.6% |
| int | delta_zigzag | 1024 | 189981 | 75322 | 188997 | 74338 | 60.7% |
| int | delta_zigzag | 4096 | 779943 | 321222 | 778959 | 320238 | 58.9% |

## Write performance (ns/sample, median time)

| type | encoding | block_size | raw ns/sample | enc ns/sample | enc/raw | raw median time (us) | enc median time (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| float | gorilla | 256 | 37.46 | 67.74 | 1.81x | 135.22 | 230.80 |
| float | gorilla | 1024 | 31.02 | 54.23 | 1.75x | 389.94 | 832.07 |
| float | gorilla | 4096 | 21.09 | 78.80 | 3.74x | 1237.7 | 2963.1 |
| int | delta_zigzag | 256 | 35.34 | 55.86 | 1.58x | 125.45 | 118.81 |
| int | delta_zigzag | 1024 | 27.59 | 27.52 | 1.00x | 429.97 | 411.64 |
| int | delta_zigzag | 4096 | 21.61 | 22.20 | 1.03x | 1386.9 | 1374.3 |

## Write HeadWindow size (series=128)

| type | encoding | block_size | est bytes raw | est bytes enc | payload bytes raw | payload bytes enc | payload savings |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| float | gorilla | 256 | 52295 | 44020 | 41031 | 32756 | 20.2% |
| float | gorilla | 1024 | 182921 | 151343 | 171657 | 140079 | 18.4% |
| float | gorilla | 4096 | 723914 | 608066 | 712650 | 596802 | 16.3% |
| int | delta_zigzag | 256 | 52364 | 23820 | 41100 | 12556 | 69.5% |
| int | delta_zigzag | 1024 | 182305 | 67745 | 171041 | 56481 | 67.0% |
| int | delta_zigzag | 4096 | 723742 | 265118 | 712478 | 253854 | 64.4% |
