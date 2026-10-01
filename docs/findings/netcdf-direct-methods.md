# NetCDF-direct methods: Download and FUSE (2026-09-20)

Decision 015. No ETL by construction: queries read the copied NetCDF files. Benchmarks on
m7i.4xlarge in us-east-1, same parameters as the other methods (`docs/findings/bench/*.csv`),
code at f2e7b11 (`src/wxtco/queries/netcdf_backend.py`). Every run reads S3: Download deletes its
temp dir after each query; FUSE evicts the files it read from the page cache after each query.

## Query times (seconds; runs in order, first is cold)

| Query | download (obstore) | download_crt (S3 Transfer Manager) | fuse (Mountpoint) | virtual (Icechunk) | table (member sort) |
|---|---|---|---|---|---|
| Q1 point series | 11.2, 11.0, 10.7 | 11.4, 11.2, 11.0 | 208, 194, 191 | 1.1 | 3.6 warm |
| Q2 regional + CRPS | 54.5, 54.2 | 55.8, 55.1 | 996, 1037 | 16-18 | 2209 |
| Q3 dataloader, 16 samples | 58.0, 57.7, 56.1 | 50.4, 51.3, 51.4 | 219, 212, 207 | 32-39 | 85 |

Bytes moved per query (Download, `bytes_read`): Q1 5.99 GB (171 files of ~35 MB), Q2 22.3 GB
(684 files), Q3 2.26 GB (64 files). Download throughput 410-545 MB/s (3.3-4.4 Gbps) on an
instance rated 6.25 Gbps baseline: bandwidth-bound. obstore and the CRT transfer manager are within
noise of each other; CRT is ~12% faster on the many-small-batches Q3.

FUSE is per-file-overhead-bound, not bandwidth-bound: ~1.1 s per file on Q1 and Q2 (open, HDF5
metadata reads at scattered offsets, each seek restarting Mountpoint's prefetch), ~5 MB/s on Q3's
whole-file reads. Reading only the needed HDF5 chunks (verified on the fixture: 113x fewer bytes than
whole-file reads) does not help when the per-file cost dominates. Mount: bucket root, read-only,
no data cache, `--metadata-ttl indefinite`, `--max-threads 64` (`docs/infra/fuse-research.md`).

## Monthly cost, D010 workload, with the NetCDF copy (`wxtco tco --storage-variant with_netcdf`)

| method | storage | ETL | query | total |
|---|---|---|---|---|
| virtual | 149.79 | 36.56 | 75.20 | 261.54 |
| download_crt | 149.36 | 0 | 754.31 | 903.67 |
| download | 149.36 | 0 | 737.91 | 887.27 |
| table (member sort) | 274.50 | 142.32 | 366.43 | 783.25 |
| fuse | 149.36 | 0 | 13,047.80 | 13,197.10 |

Q1 at 10,000/day dominates every no-ETL method: 11 s x 10,000/day of m7i.4xlarge is ~$740/month;
FUSE's 197 s makes it ~$13,000/month. The model does not yet charge S3 GET requests on queries:
Download Q1 is 171 GETs (CRT: ~342 ranged GETs), so 10,000/day adds ~$20-40/month; FUSE's per-open
HEAD+LIST under a minimal metadata TTL would add ~$640/month (avoided by `indefinite`).

## Caveats

- One instance size. Download scales with network bandwidth; a c7i.16xlarge (25 Gbps) would cut
  Download times ~4x and its query cost proportionally less (the instance costs 3.5x more per hour).
- Each query re-lists the cycle (~14,000 keys, ~15 LIST pages). The listing time is available as
  `NetcdfFilesBackend.list_seconds()` but is not a bench column; keys are derivable from the file
  naming scheme, so a production system would skip it.
- The table column is the member-sorted study table; the Hilbert table is being re-ingested at full
  size and will be benchmarked with DuckDB's file cache disabled.
