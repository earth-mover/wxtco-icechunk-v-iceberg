# Stage A summary

Final 2026-09-19 (session 003). Every number below is measured; commands and raw rows are cited.

Study window: 28 MOGREPS-G cycles, 2026/09/12 T0000Z to 2026/09/18 T1800Z, 81 surface diagnostics,
all leads, 18 members, 960 x 1280 grid. Source copy: `s3://em-tco-mogreps/netcdf/`, 322,588 files,
6.49 TB, verified complete. Both methods were built from the copy. Prices: `prices.toml`
(us-east-1 list, 2026-09). Commands and raw rows: `docs/findings/{storage.csv,etl/,bench/}`.

## Storage

| method | method's own bytes | with the NetCDF copy | objects |
|---|---|---|---|
| table (Iceberg/Parquet, zstd) | 5,440.6 GB | 11,934.6 GB | 19,152 data files (4 per lead) |
| virtual (Icechunk references) | 18.6 GB | 6,512.6 GB | 6,785 |

The table is 84% of the NetCDF size and stands alone. The virtual repo is 0.3% of it but cannot
exist without the copy, so the honest storage comparison is 5.4 TB vs 6.5 TB, both about $125-150
per month at S3 Standard. Table nulls (16.8% of value cells, from the six lead schedules) cost
nothing measurable: a null column chunk is one RLE run. Virtual repo size is 663 MB per cycle
because shards pack 4 cycles and superseded versions await garbage collection; Joe measured 376 MB
per cycle with per-cycle shards plus GC.

Source: `aws s3 ls --summarize` on each prefix from the instance role; Iceberg `file_size_in_bytes`
summed over live data files (`/opt/wxtco/verify_table.py`).

## ETL

Clean single-cycle timings on an otherwise idle c7i.16xlarge (64 vCPU, 123 GB), cycle 2026/09/15/T0000Z:

| method | seconds per cycle | notes |
|---|---|---|
| virtual | 384 | 60 worker processes, ~11,521 HDF5 files parsed, one commit |
| table | 1,495 | 171 leads, 5 one-lead processes at a time, 40.0 s per lead |

Table ingest is 3.9x the instance time. It is bound by single-process Parquet encoding of a 7 GB
Arrow table per lead and by memory: one process reaches 22-45 GB, so the 123 GB node runs at most
five leads concurrently, and a long-lived process leaks across leads (chunking to one lead per
process was the fix). The overnight bulk run shared one node between both methods and is not used
for costing; its numbers are in `docs/findings/etl/contaminated/`.

## Query performance

m7i.4xlarge (16 vCPU), alone, 5 runs each, cycle 2026/09/15/T0000Z, London grid cell
(51.47, -0.14), UK box (54 x 37 cells), Q3 = 16 (cycle, lead) samples x 4 variables x 18 members.

| query | table (DuckDB over Iceberg) | virtual (xarray over Icechunk) |
|---|---|---|
| Q1 point series, 171 leads x 18 members | 250 s cold, then 3.9-4.6 s (median 4.56) | 1.7 s cold, then 0.9-1.2 s (median 1.11) |
| Q2 UK box, all leads, 2 vars, mean/spread/CRPS | 2,209 s (one run; 37 min) | 21.9 s cold, then 17.6-18.2 s (median 18.1) |
| Q3 dataloader, 16 samples x 4 vars | 150 s cold, then 93.7-94.6 s (median 94.3) | 49.5 s cold, then 37.5-39.1 s (median 38.8) |

Observations:

- Virtual Q1 dropped from 21-32 s to about 1 s with two changes Ryan proposed: open without dask
  (`chunks=None`) and `zarr.config.set(async.concurrency=256, threading.max_workers=16)`. A point
  series touches about 3,000 small HDF5 chunks; fetch concurrency is the whole game.
- Instance size barely matters for the virtual reads: on a c7i.16xlarge the medians were 1.02, 17.0
  and 35.7 s (5-8% better with 4x the cores). The path is bound by S3 request latency.
- Table cold starts are planning over 19,152 data files through the REST catalog; a service that
  keeps a connection open amortizes it, one that opens per query pays it every time.
- Table Q2 is the layout's worst case: `lat`/`lon` are not partition columns, so the scan touches
  the coordinate columns of every file of two cycles (1,368 files) and proceeds at about 1 M rows/s.
  Spatial bucketing or a z-order sort would change this by orders of magnitude; that is a stage B
  layout question, not a DuckDB limitation.
- Operational finding: Arraylake's Iceberg REST catalog vends S3 credentials that expire after
  about 30 minutes and DuckDB does not refresh them, so a long scan fails with `ExpiredToken`. The
  table backend now reads data files with the ambient AWS credential chain and uses the catalog for
  metadata only.
- Bytes read are not measured in stage A (`bytes_read` is empty); request counts would come from
  S3 CloudWatch request metrics on the bucket.

## Cost at 1x and 10x workload

`wxtco tco` and `wxtco tco --storage-variant with_netcdf`, monthly USD, D010 workload (Q1 10,000/day,
Q2 4/day, Q3 1/day, ETL 4 cycles/day), serialized per-second instance model (c7i.16xlarge for ETL,
m7i.4xlarge for queries, S3 Standard for storage, no request charges):

| method | storage (own bytes) | storage (with copy) | ETL | queries | total (own bytes) | total (with copy) |
|---|---|---|---|---|---|---|
| table | 125 | 275 | 142 | 366 | 634 | 783 |
| virtual | 0.4 | 150 | 37 | 75 | 112 | 262 |

At 10x query workload (`--multiplier 10`) the query terms become 3,664 (table) and 752 (virtual),
for totals of 3,932 / 4,081 (table) and 789 / 938 (virtual). Query cost splits as: table Q1 $307,
Q2 $59, Q3 $0.6; virtual Q1 $75, Q2 $0.5, Q3 $0.3. Q1's 10,000/day weight makes the point query
the whole story at this workload; the table's 37-minute Q2 costs only $59/month at 4/day.

Interpretation: on the method's own bytes the virtual approach is 5.7x cheaper per month; when it
must also carry the 6.5 TB NetCDF copy the gap narrows to 3.0x, and storage becomes 57% of its bill.
The table's bill is 58% queries, 22% ETL, 20% storage.

## What this implies for the native chunking (input to decision 011)

- The virtual layout already wins Q1 and Q3 against the wide table, and the remaining Q1 cost is
  request count: 3,000 chunk reads for one point series. A native repo that rechunks to put all
  leads of a point in one chunk (for example (member 18, lead 171, lat 32, lon 32)) would turn Q1
  into a few dozen requests and should land well under 0.5 s.
- Q3 (whole fields for a few leads) is served adequately by the source (1, 128, 128) chunks; a
  native layout that enlarges the spatial footprint per chunk helps Q3 and hurts Q1, so the
  decision is a Q1/Q3 trade, with Q1's 10,000/day weight pointing at lead-major chunks.
- The six lead-schedule groups are a Zarr constraint (one coordinate per group) that the table
  avoids with nulls. A native repo can keep the groups, or pad every variable to the 171-lead axis
  and let missing chunks encode absence at the cost of 78% fill reads on the 3-hour statistics.
- Table storage is competitive (84% of NetCDF), and the table's ETL and Q2 costs are layout choices
  (row-group sort, spatial partitioning) rather than inherent; a fair stage B would revisit them
  alongside the native chunking.

## Addendum 2026-09-21: cache-honest table numbers, Hilbert table, new methods

The table figures above (Q1 3.6-4.6 s, Q3 85-94 s warm) were served largely from DuckDB's external file
cache, which is on by default; Icechunk's figures were S3 reads throughout. With the cache disabled the
member-sorted table reads Q1 in 116-122 s and Q3 in 544-595 s from S3. Details, the full Hilbert table
(5,122 GB, Q1 22 s, Q2 550-584 s, Q3 585-603 s from S3) and the like-for-like cost table are in
`docs/findings/hilbert-full-table.md`. The no-ETL methods (Download, FUSE) are in
`docs/findings/netcdf-direct-methods.md`; the native Zarr layout trials in `docs/findings/notes.md`
(2026-09-20) and decision 011. Monthly totals with the copy, D010 workload: virtual 262, table
(cache-assisted) 783, download 887, table_hilbert 1,982, table_nocache 8,495, fuse 13,197 USD.
