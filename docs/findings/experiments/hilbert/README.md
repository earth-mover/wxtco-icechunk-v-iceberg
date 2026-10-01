# Experiment: Hilbert row order for the wide table (2026-09-20)

Question (Ryan): does a spatial sort inside each `(init_time, lead_hours)` partition fix Q2 without a
tile predicate in the query? Scratch table `mogreps.surface_hilbert` (Arraylake org `wxtco`), 2 cycles
(2026/09/15 T0000Z and T0600Z), same schema, partitions and row-group size as `mogreps.surface`; rows
written cell by cell along a Hilbert curve over the 960 x 1280 grid, member innermost
(`WXTCO_TABLE_SORT=hilbert`, commit 2f01ec0). Queries are unchanged: pruning comes from Parquet
row-group and Iceberg file statistics on `lat` and `lon`.

These CSVs are NOT read by `wxtco tco`. Instance: ETL c7i.16xlarge (1.01 h), bench m7i.4xlarge (2.53 h).

## Results (seconds; first run of a process is cold)

| Measurement | Cold | Warm |
|---|---|---|
| ETL per cycle, Hilbert (`etl.csv`) | 1681, 1677 | baseline 1495 |
| Storage per cycle (`storage.csv`) | 182.8 GB | baseline 194.3 GB |
| Q2 Hilbert, DuckDB file cache on (`table-hilbert-q2-v2.csv`) | 497 | 14.5, 14.5 |
| Q2 Hilbert, DuckDB file cache OFF (`table-hilbert-q2-nocache.csv`) | 482 | 476, 503 |
| Q2 baseline, yesterday, one cold run | 2209 | not measured (est. ~2000: scan-bound, > cache) |
| Q2 Icechunk, chunk cache 33 GB (`virtual-q2-cached.csv`) | 24.8 | 16.1-17.5 |
| Q2 Icechunk, chunk cache 0 (`virtual-q2-nocache.csv`) | 17.8 | 16.8, 16.5 |
| Q1 Hilbert, cheap grid discovery (`table-hilbert-q1-v2.csv`) | 10.0 | 0.59, 0.56 |
| Q1 baseline, cheap grid discovery (`table-q1-v2-control.csv`) | 120.7 | 6.45, 4.44 |
| Q1 baseline, old grid scan (`table-q1-control.csv`) | 258 | 3.5-3.8 |
| Q1 Icechunk, cached (`virtual-q1-cached.csv`) | 1.25 | 0.77-1.08 |
| Q3 Hilbert (`table-hilbert-q3.csv`) | 57.9 | 30.9, 31.7 |
| Q3 baseline same day (`table-q3-control.csv`) | 137 | 85.5, 83.9 |
| Q3 Icechunk, cached (`virtual-q3-cached.csv`) | 42.0 | 34.7, 32.0 |

## Reading the numbers

- **Layout effect on Q2 is real but 4.5x, not 170x.** From S3 the Hilbert table answers Q2 in ~480 s
  against 2209 s. The 14 s warm runs are DuckDB's external file cache (on by default since 1.3)
  serving the same byte ranges from RAM; disabling it (`SET enable_external_file_cache=false`)
  gives 476-503 s. Icechunk answers the same query from S3 in 16-18 s.
- **480 s for ~1,400 footers and a few thousand column chunks is request-latency bound**, not
  bytes: DuckDB's Iceberg scan keeps too few S3 requests in flight. That is the next table-side lever,
  not row order.
- **Q1 gains nothing from the layout.** Q3 cannot benefit from row order, and its warm gap
  (85 vs 31 s over 16 samples = 3.3 s per query) equals the Q1 gap (3.6 vs 0.5 s). Both are the
  per-query cost of walking 19,152 file entries instead of 1,368. Any full-size Hilbert table would
  pay it too.
- **Cold starts.** The old grid discovery scanned one column over a whole cycle (3.8 B rows) on the
  first Q1/Q2 of a process; commit daa18d7 reads one `(init_time, lead_hours=0, member=0)` slice
  instead. Q1 cold on the study table fell from 258 s to 121 s (now planning over 19k files) and on the
  scratch table from 174 s to 10 s. Q2 cold barely moved (506 to 497 s) because it is the S3 read.
- **Icechunk's chunk cache does nothing for this repo.** With `CachingConfig.num_bytes_chunks` at
  33 GB (half of RAM; commit b178d62) Q2 is 16-17.5 s; with it at 0, 16.5-17.8 s. Hypothesis: the
  earlier bypass hypothesis was wrong: Icechunk caches all chunk types; the queries are decode- and compute-bound at this size (corrected 2026-09-21); the rest of the sentence (the
  proxy blocks icechunk.io). The warm Icechunk numbers are therefore honest S3 reads.
- **ETL cost of the sort: +12% instance time, -6% Parquet bytes.** One gather per column through a
  cached permutation; memory peaked at ~99 GB with 5 concurrent leads on 128 GB.

## Caveat

Two cycles, not 28. Per-partition work is identical to the study table; only the metadata walk differs,
and the controls above quantify it. A full-scale Hilbert re-ingest would take ~13 h of c7i.16xlarge (~$40).
