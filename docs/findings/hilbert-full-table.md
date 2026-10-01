# Full Hilbert table and cache-honest table benchmarks (2026-09-21)

`mogreps.surface_hilbert`: all 28 study cycles, rows written cell-by-cell along a Hilbert curve with member
innermost (decision: Ryan, 2026-09-20). Verified 4,788 partitions x 4 files, 105,902,899,200 rows,
**5,122 GB** (member-sorted `mogreps.surface`: 5,441 GB). Both tables are kept.

## Query times from S3 (m7i.4xlarge, DuckDB external file cache OFF; first run of each process is cold)

| Query | table, member sort (`table_nocache`) | table, Hilbert (`table_hilbert`) | virtual Icechunk | download |
|---|---|---|---|---|
| Q1 point series | 177 cold, 116-122 | 75 cold, 22-23 | 1.1 | 11 |
| Q2 UK box + CRPS, 2 cycles | 2,209 (one run) | 584, 567, 544 | 16-18 | 54 |
| Q3 dataloader, 16 samples | 595, 559, 544 | 603, 594, 585 | 32-39 | 56 |

CSVs: `docs/findings/bench/table_hilbert-q*.csv`, `table_nocache-q*.csv` (Q2 reuses the 2026-09-19 cold run).

## What changed in the picture

- **Every earlier warm table number was DuckDB's file cache, not S3.** With the cache off the member-sorted
  table's Q1 is 118 s, not 3.6 s, and its Q3 is 560 s, not 85 s. `docs/findings/bench/table-q*.csv` (method
  `table`) keeps the cache-assisted runs for the record; `table_nocache` is the like-for-like row. Icechunk's
  numbers were S3 reads throughout (its chunk cache is off by default and, when enabled, did not change them).
- **Hilbert order helps where row-group pruning applies:** Q1 5x (one row group per lead instead of one per
  member), Q2 4x. Q3 reads whole partitions and is unchanged. Cold starts are unchanged: ~50-75 s of REST
  manifest fetch and planning over 19,152 files.
- **The table method's query cost is request-latency bound.** Q2 moves ~150 MB of needed data in 550 s
  because DuckDB's Iceberg scan keeps few S3 requests in flight per file (1,368 footers + ~5,000 column
  chunks). Download moves 22 GB in 54 s on the same node.

## Monthly cost, D010 workload, with the NetCDF copy (`wxtco tco --storage-variant with_netcdf`)

| method | storage | ETL | query | total USD |
|---|---|---|---|---|
| virtual | 149.79 | 36.56 | 75.20 | 261.54 |
| table (cache-assisted, for the record) | 274.50 | 142.32 | 366.43 | 783.25 |
| download | 149.36 | 0 | 737.91 | 887.27 |
| table_hilbert (S3) | 267.18 | 159.84 | 1,555.12 | 1,982.14 |
| table_nocache (S3) | 274.50 | 142.32 | 8,077.80 | 8,494.62 |
| fuse | 149.36 | 0 | 13,047.80 | 13,197.10 |

Q1 at 10,000/day dominates: 22 s x 10,000/day of m7i.4xlarge is ~$1,480/month for the Hilbert table against
$75 for Icechunk. A resident engine with a warm file cache recovers the 3.6 s figure only while the working set
stays in RAM; the D010 workload of 10,000 distinct points per day does not.

## ETL

Two c7i.16xlarge nodes, 5 lead groups each, 10 concurrent committers: per-cycle wall 1,700-1,917 s against
1,681/1,677 s isolated. ~3% from commit-conflict retries (~1.2 per lead after `commit.retry.num-retries=30`),
5-10% from metadata growth as the snapshot list reached 4,788 entries (per-lead in-process time 45.7 -> 50.5 s).
Three leads were lost to two pre-fix 409 conflicts and one Arraylake REST 502; the idempotent sweep re-ingested
them. `docs/findings/etl/table_hilbert.csv` carries the isolated 1,681/1,677 s. Instance time: 6.18 + 7.26 h
ingest, 1.90 h bench, ~$40.
