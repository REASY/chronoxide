| Version                          | Raw total MiB | Raw payload MiB | Raw overhead MiB | Gorilla total MiB | Gorilla payload MiB | Gorilla overhead MiB |
|----------------------------------|--------------:|----------------:|-----------------:|------------------:|--------------------:|---------------------:|
| Original (per‑series Vecs)       |        612.59 |          303.61 |           308.98 |            521.41 |              141.08 |               380.33 |
| Arena + boxed builder            |        550.79 |          303.61 |           247.18 |            388.27 |              141.08 |               247.18 |
| Arena + boxed builder + SmallVec |        530.20 |          303.61 |           226.60 |            367.68 |              141.08 |               226.60 |