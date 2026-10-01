# Native Zarr repos: query benchmarks

## Full repos (2026-09-23)

Both repos hold all 28 study cycles and all 81 variables after the xarray rewrite (`mogreps-g-native-ts`:
chunk 1,18,57,16,16, shard 1,18,171,320,320; `mogreps-g-native-dl`: one 7.2 GB shard per sample, inner chunk
1,1,1,18,240,320 in Morton order, Q3 variables first). Benchmarked from S3 on an m7i.4xlarge with the Icechunk
chunk cache at 0 (`WXTCO_CHUNK_CACHE_BYTES=0`), same parameters as every other method (cycle 2026/09/15/T0000Z,
Q3 over T0000Z + T0600Z). Run 0 is the cold run; the process is warm after it (metadata, not chunks).

| Query | native_ts | native_dl | virtual (Icechunk, S3) | table_hilbert (S3) | download |
|---|---|---|---|---|---|
| Q1 point series (5 runs) | 0.45 cold, 0.08-0.17 | 0.90 cold, 0.35-0.41 | 1.1 | 22 | 11 |
| Q2 UK box + CRPS, 2 cycles (3 runs) | 2.8, 2.4, 2.0 | 7.0, 6.5, 6.4 | 16-18 | 544-584 | 54 |
| Q3 dataloader, 16 samples (3 runs) | 172, 170, 167 | 17.9 cold, 14.6, 14.0 | 32-39 | 585-603 | 56 |

The full repos reproduce the trial within noise: the ts Q3 over-read is unchanged (64x, ~10.5 s per sample), and
the dl shard with 81 variables reads the same 64 inner chunks per sample as the 4-variable trial shard did, so
packing the whole variable set into one shard costs Q3 nothing.

### Ingest and storage (two c7i.16xlarge, one cycle per commit, pcodec level 8)

| | native_ts | native_dl |
|---|---|---|
| Cycles / units per cycle | 28 / 81 (one per variable) | 28 / 171 (one per lead) |
| Seconds per cycle (mean) | 1,512 | 1,361 |
| Read / write / commit per cycle (medians, worker-seconds) | 4,422 / 1,466 / 6.9 | 4,430 / 3,507 / 1.2 |
| Repo size | 3,646 GB | 3,828 GB |
| Compression vs 34,311 GB uncompressed | 9.4x | 9.0x |
| Node runtime | 11.9 h | 10.7 h |

Neither run skipped a unit or dropped a lead. The ts writer is bounded by S3 reads of the source NetCDF (each
variable's 171 files); the dl writer spends 2.4x more in `write` because every lead re-encodes 81 variables into
one 7.2 GB shard. Per-cycle rows: `docs/findings/etl/native_{ts,dl}.csv`; sizes: `docs/findings/storage.csv`.

### Monthly cost, D010 workload (`wxtco tco`)

| method | storage (with copy) | ETL | query | total, with copy | total, own bytes | 10x queries, with copy |
|---|---|---|---|---|---|---|
| virtual | 150 | 37 | 75 | 262 | 112 | 938 |
| native_ts | 233 | 143 | 9 | 386 | 236 | 466 |
| native_dl | 237 | 129 | 27 | 393 | 244 | 632 |
| table (cache-assisted) | 275 | 142 | 366 | 783 | 634 | 4,081 |
| download | 149 | 0 | 738 | 887 | 738 | 7,528 |
| table_hilbert (S3) | 267 | 160 | 1,555 | 1,982 | 1,833 | 15,978 |

The trial's projection held: at 1x the virtual repo wins on ETL (3.5-3.9x cheaper) and storage (18.6 GB against
3.6-3.8 TB); each native layout wins its own queries by 8-13x (Q1 and Q2 on ts, Q3 on dl) but not the other
group's. Against virtual, a single native layout breaks even at ~2.9x (ts) or ~3.7x (dl) the D010 query
workload. A repo carrying both layouts pays both storage and ETL bills (~$630/month at 1x with the copy, ~$950 at
10x, versus $262 / $938) and only breaks even at ~10x, which revises the trial's 3-4x estimate that used the
4-variable dl repo. CSVs: `docs/findings/bench/native_*.csv` (full repos; the trial CSVs were replaced).

## Trial repos (2026-09-21)


Trial repos on Arraylake (3 cycles each, 2026/09/12 T0000Z-T1200Z), m7i.4xlarge, Icechunk chunk cache at 0
(`WXTCO_CHUNK_CACHE_BYTES=0`) so every run reads S3. `native_ts` has the trial chunking (1,18,64,17,16);
`native_dl` holds only the four Q3 variables (one 354 MB shard per sample). The reviewed code changes both
(ts chunk 1,18,57,16,16; dl all 81 variables in one 7.2 GB shard per sample, Q3 variables first; Morton sub-chunk
order after Ryan's 2026-09-22 decision); byte-range reads inside the shard make the dl Q3 read the same 64 inner chunks either way,
so these are close to what the full repos will show. Query cycles are 2026/09/12 (the trial's), not 2026/09/15.

| Query | native_ts | native_dl | virtual (Icechunk, S3) | table_hilbert (S3) | download |
|---|---|---|---|---|---|
| Q1 point series | 0.27 cold, 0.07-0.09 | 0.77 cold, 0.35-0.45 | 1.1 | 22 | 11 |
| Q2 UK box + CRPS, 2 cycles | 3.7 cold, 2.3, 1.8 | 6.7, 6.5, 6.3 | 16-18 | 544-584 | 54 |
| Q3 dataloader, 16 samples | 185, 182, 176 | 15.4 cold, 13.0, 12.7 | 32-39 | 585-603 | 56 |

Each layout wins the queries it was shaped for and loses the other, as designed: `ts` reads Q1 in three inner
chunks and Q2 in ~144, but pays a 64x over-read on Q3 (11 s per sample); `dl` reads a Q3 sample as 64 inner
chunks of one shard in 0.8 s and is still 3x faster than the virtual repo on Q1 and Q2 because pcodec chunks
of 18 members x 240 x 320 need far fewer requests than 40 KB HDF5 chunks.

## Monthly cost, D010 workload, with the NetCDF copy (`wxtco tco --storage-variant with_netcdf`)

| method | storage | ETL | query | total USD | 10x queries |
|---|---|---|---|---|---|
| native_dl (4 vars; storage/ETL will rise to ~native_ts at 81 vars) | 156 | 7 | 30 | 194 | 464 |
| virtual | 150 | 37 | 75 | 262 | 938 |
| native_ts | 237 | 128 | 7 | 372 | 435 |
| table (cache-assisted) | 275 | 142 | 366 | 783 | 4,081 |
| download | 149 | 0 | 738 | 887 | 7,528 |
| table_hilbert (S3) | 267 | 160 | 1,555 | 1,982 | 15,978 |

At 1x the virtual repo is still cheapest of the honest methods because its ETL is 3.5x cheaper and it stores
almost nothing; at 10x the native layouts win because Q1 costs 0.08-0.4 s instead of 1.1 s. A two-group native
repo (ts + dl) would carry both storage and ETL bills: ~$600/month at 1x with the copy, ~$700 at 10x, versus
$262 / $938 for virtual. The crossover is around 3-4x the D010 query workload. CSVs: `docs/findings/bench/native_*.csv`.
