# Canonical results (decision 016)

One benchmark pass for the six reported methods, run 2026-09-30 01:37-03:30 UTC on one m7i.4xlarge
(16 vCPU, 64 GB, 6.25 Gbps = 781 MB/s baseline) in us-east-1, from commit 22a9f29 (recorded in every
row). The bucket was idle for 30 minutes before the start (CloudWatch `BytesDownloaded` 0). Every read
comes from S3: Icechunk chunk cache and DuckDB file cache off, FUSE page cache evicted after each query.
Wire bytes are the node's received bytes per run (`/proc/net/dev`). CSVs: `docs/findings/bench/`.
Parameters: Q1 point (51.5 N, 0.1 W) on 2026/09/15 T0000Z, one variable, 171 leads x 18 members;
Q2 UK box, two variables, cycle T0000Z plus the T0600Z proxy; Q3 the batched stream at B=8 over 32
samples (4 cycles of 2026/09/15 x 8 leads, 4 variables, full grid; 11.3 GB decoded), priced per
16-sample job. Runs: Q1 x5, Q2 x3, Q3 x2; FUSE once per query.

## Query performance (median seconds; wire bytes per run)

| Method | Q1 s | Q1 wire | Q2 s | Q2 wire | Q3 s per 16 samples | Q3 decoded GB/s | Q3 wire per 32 samples | Q3 wire vs minimum |
|---|---|---|---|---|---|---|---|---|
| native_ts | 0.11 | 2 MB | 2.3 | 38 MB | 52.1 | 0.11 | 44.7 GB | 22.0x |
| native_dl | 0.40 | 211 MB | 6.6 | 1.48 GB | 6.0 | 0.95 | 2.03 GB | 1.0x |
| virtual | 0.99 | 83 MB | 18.0 | 1.30 GB | 40.7 | 0.14 | 4.79 GB | 2.4x |
| download | 11.9 | 6.11 GB | 60.0 | 22.8 GB | 38.3 | 0.15 | 4.65 GB | 2.3x |
| table_hilbert | 27.6 | 315 MB | 694 | 1.46 GB | 184 | 0.031 | 4.72 GB | 2.3x |
| fuse | 268 | 8.46 GB | 1,250 | 39.1 GB | 249 | 0.023 | 20.5 GB | 10.1x |

"Minimum" is the native_dl stream, which reads exactly the 64 inner chunks per sample. Wire rates at
the median: download 380-520 MB/s and native_ts Q3 430 MB/s are the only runs near the line rate;
virtual moves 60-84 MB/s, FUSE 31-41 MB/s, table_hilbert 2-13 MB/s. Those three are request-latency
bound, which is also why they vary most between days (below).

## Monthly cost (`wxtco tco`, decision-010 workload, USD)

Compute is on-demand seconds; requests are S3 GET/HEAD at $0.0004 and LIST at $0.005 per 1,000
(`prices.toml`), per query from `docs/findings/requests.csv` (next section).

| Method | Storage (with NetCDF copy) | ETL | Query compute | S3 requests | Total | Total, own bytes only | Total at 10x queries |
|---|---|---|---|---|---|---|---|
| native_ts | 233 | 143 | 8 | 1 | **385** | **236** | **461** |
| native_dl | 237 | 129 | 27 | 36 | 430 | 280 | 998 |
| virtual | 150 | 37 | 67 | 301 | 555 | 405 | 3,871 |
| download | 149 | 0 | 801 | 38 | 989 | 839 | 8,541 |
| table_hilbert | 267 | 160 | 1,875 | 668 | 2,970 | 2,820 | 25,853 |
| fuse | 149 | 0 | 18,013 | 2,305 | 20,468 | 20,319 | 203,338 |

Q1 (10,000 a day) is over 90% of every method's query compute and request bill. With requests counted,
native_ts is the cheapest method at the decision-010 workload, with or without the NetCDF copy.
Break-even against virtual, from the fixed (storage + ETL) and per-workload (compute + requests)
parts: native_ts at 0.53x the query workload, native_dl at 0.59x, and one repo carrying both layouts
(the NetCDF copy once, Q1/Q2 on ts and Q3 on dl: $602 at 1x) at 1.1x. Without request costs the same
break-evens were 3.2x, 4.5x and 6.8x.

## S3 requests per query

Measured on 2026-09-30 in a separate pass (m7i.4xlarge `i-080fa34bf034b9833`, commit 15420e6, caches off): every query phase started and ended on a whole minute, so CloudWatch's
per-minute `GetRequests`, `HeadRequests` and `ListRequests` for the bucket belong to one phase. Each
query ran once and then N times; per query = (N-run count - 1-run count) / (N - 1), which removes
the one-time repo or table open (a long-lived service opens once). An idle phase gave the background
rate (about 9.5 HEAD a minute, no GETs), which is subtracted. FUSE and the Iceberg Q2/Q3 phases were
already clean in the canonical run and are taken from its windows. Q3 is per 16-sample job.

| Method | Q1 GET | Q1 LIST | Q2 GET | Q3 GET | Q1 requests, $/month |
|---|---|---|---|---|---|
| native_ts | 3 | 0.2 | 22 | 1,811 | 0.4 |
| native_dl | 298 | 0 | 1,233 | 77 | 36 |
| download | 169 | 12 | 613 | 58 | 38 |
| virtual | 2,478 | 0 | 49,055 | 97,423 | 297 |
| table_hilbert | 5,554 | 0 | 19,840 | 22,631 | 667 |
| fuse | 16,650 | 189 | 20,505 | 3,500 | 2,281 |

- **virtual** reads the NetCDF files' own HDF5 chunks: a point series touches about 14 small chunks in
  each of the 171 lead files, so a Q1 is ~2,500 GETs. At 300,000 Q1 a month that is $297, more than
  its storage and ETL together. This is the cost of not re-chunking.
- **native_ts** reads three chunks per Q1 (one per 57-lead chunk); **native_dl** one inner chunk per lead
  (171 shards, plus shard indexes).
- **download** is one GET per file plus a 12-page LIST of the cycle per query.
- **table_hilbert** issues ~5,500 range reads per Q1 after pruning, and opening the table costs another
  ~7,350 GETs (once per process).
- **FUSE** adds Mountpoint's metadata traffic (LIST and HEAD) to many small HDF5 reads.
- ETL request costs are not counted (ingest runs overlapped on the bucket); they are PUT-heavy and
  small next to the ETL compute, e.g. one native_dl cycle writes 171 shards.

## Iceberg: why Hilbert sort, and the lesson

The member-sorted table (rows ordered init, lead, member, lat, lon) puts one grid cell's 18 members
far apart in every row group, so Parquet min/max statistics cannot prune a point or box query: Q1
reads every row group of the cycle's 171 lead partitions. Hilbert row order within each (init, lead)
partition keeps nearby cells together, so the statistics prune to a few row groups. Measured the same
night on the same node (2026-09-21, S3 reads): member sort Q1 116-122 s, Hilbert 22-23 s, 5.2x;
Hilbert also stores 6% less (5,122 vs 5,441 GB). Two lessons for anyone putting gridded forecasts in
a table:

1. **Row order is the index.** A lat/lon table without a space-filling-curve sort is a full scan for
   every spatial query; with Hilbert order the same engine and files are 5x faster, at no extra
   ETL beyond the sort (1,679 vs 1,495 s per cycle).
2. **Measure with the cache off.** The first table numbers (Q1 4.6 s) came from DuckDB's external file
   cache, which kept the Parquet bytes in RAM between runs; from S3 the same query took 119 s. Every
   table number in the report is from S3.

Even sorted, the table stays request-latency bound (2-13 MB/s on the wire): each query plans over
~19,000 data files and issues many small range reads, so it is the most expensive non-FUSE method.

## Differences from the earlier runs

| Method and query | earlier (date, bucket state) | canonical | change |
|---|---|---|---|
| native_ts, native_dl, virtual Q1-Q3 | 2026-09-19 to 09-23 | same within 5-10% | none worth noting |
| download Q1 / Q2 | 11.0 / 54 s (09-20, bucket shared with an ingest) | 11.9 / 60 s | +8-10% |
| download Q3 | 58 s per 16 samples, one-sample loop | 38 s, one transfer per batch | batch path added |
| table_hilbert Q1 / Q2 | 22.9 / 567 s (09-21) | 27.6 / 694 s | +20-23% |
| fuse Q1 / Q2 | 194 / 1,016 s (09-20, shared bucket) | 268 / 1,250 s | +23-38% |

Neither the table read code nor DuckDB changed between the two table runs, and CloudWatch for the
earlier Hilbert Q2 runs shows about the same bytes per run (~0.15 GB/min over ~9.5 min, ~1.4 GB), so the slowdowns of the latency-bound methods are day-to-day S3 and catalog latency. The
bandwidth-bound methods moved less than 10%. The report uses the canonical numbers only.

## Not in the headline set (decision 016)

Cache-assisted Iceberg, `download_crt` (within 2-4% of obstore) and the member-sorted table; their CSVs
and storage and ETL rows are in `docs/findings/experiments/`. Side experiments reported separately:
the Q3 read-pattern study (`q3-bulk-fetch.md`, `q3-stream-sweep.md`) and the unsharded Anemoi layout
with pcodec and Blosc LZ4 (end of `q3-stream-sweep.md`).
