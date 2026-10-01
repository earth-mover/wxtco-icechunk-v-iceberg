# Q3 dataloader: one bulk fetch instead of a synchronous loop (2026-09-23)

`q3` loops over 16 (cycle, lead) samples and calls `lead_fields` once per sample, which loads the
four variables one after another. `q3_bulk` (commit 2b88a56) makes one lazy xarray selection over
all cycles, leads and variables and loads it once, so zarr and Icechunk fetch every inner chunk
concurrently (`async.concurrency` 256). Same node (m7i.4xlarge, 6.25 Gbps baseline = 781 MB/s),
same parameters (cycle 2026/09/15 T0000Z + T0600Z, leads 0-120 h, four Q3 variables), Icechunk
chunk cache off. Every run now records the node's received bytes (`net_rx_bytes` in the bench CSV),
so wire throughput and read amplification are measured, not estimated. Decoded bytes per run are
5.66 GB (16 samples x 4 variables x 18 members x 960 x 1280 x float32).

| Method, pattern | seconds | wire GB per run | wire MB/s | decoded MB/s | wire / decoded |
|---|---|---|---|---|---|
| native_dl, loop (`q3`) | 22.1, 17.2 | 1.42 | 64-83 | 256-330 | 0.25 |
| native_dl, bulk (`q3_bulk`) | 6.1, 5.7, 5.8 | 1.03 | 170-180 | 933-985 | 0.18 |
| native_ts, loop (`q3`) | 181.6, 181.5 | 140.6 | 775 | 31 | 24.8 |
| native_ts, bulk (`q3_bulk`) | 60.0, 61.5, 60.1 | 22.2 | 361-370 | 92-94 | 3.9 |

Read amplification against the smallest transfer that serves the query (the dl bulk read, 1.03 GB,
which fetches exactly the 64 inner chunks per sample): dl loop 1.4x, ts bulk 21.5x, ts loop 137x.
The pcodec ratio for these four variables is 5.5x (5.66 GB decoded from 1.03 GB), below the repo
mean of 9.0x.

## What the numbers say

- **The dl layout was latency-bound, not bandwidth-bound.** The loop moved 1.42 GB in 17-22 s, a
  tenth of the line rate. One bulk fetch cuts it to 5.8 s: 3x faster, and 980 MB/s decoded. The
  wire rate is still only 178 MB/s, so the remaining time is decode and copy (pcodec on 16 vCPUs,
  then the transpose and float32 copy in `batch_fields`), not the network.
- **The ts loop is pinned at the network ceiling** (775 MB/s of 781 MB/s), moving 25x the decoded
  bytes: every sample re-reads the 57-lead chunks and the (member, lead) chunking cannot slice a
  lead out. The bulk fetch reads each chunk once for all eight leads, so the wire bytes fall 6.3x
  (140.6 to 22.2 GB) and the time 3x (181 to 60 s). The remaining 22 GB is the three lead-chunks
  (0-56, 57-113, 114-170 h) that the eight leads touch, at 5.5x compression.
- **Order of magnitude for the cost model**: bulk Q3 on dl is 5.8 s against 14 s in the current
  `native_dl` row; on ts 60 s against 170 s. Both are better than the download method (56 s) and
  the virtual repo (32-39 s) by the dl margin only.

## Two zarr behaviours that shaped the result

1. **Orthogonal selection with two or more array-indexed axes falls back to coordinate indexing per
   shard.** `OrthogonalIndexer.__iter__` (zarr 3.4.0, `n_array_dims > 1 or self.drop_axes`) hands
   each shard an `np.ix_` tuple, which the sharding codec's `get_indexer` classifies as a
   `CoordinateIndexer` and broadcasts to the selection size. The loop never hits this: `lead_fields`
   reads scalar init, scalar lead and one variable at a time, a basic indexer. The first `q3_bulk`
   version (ff811cc) selected inits, leads and variables as three lists in one xarray selection:
   on the 7.2 GB dl shards that was 64 GB of RSS in 60 s and an OOM kill;
   on ts it fitted (57 GB) but ran CPU-bound at 141 MB/s wire, 157 s, no faster than the loop.
   Fix in 2b88a56: `positions()` turns consecutive labels into slices, so only `lead` stays an
   array. After the fix the same runs peak at ~3 GB. Worth an upstream issue: a sharded array read
   with two list indexers should not need memory proportional to the selection times six.
2. **`Dataset.load()` without dask loads variables one after another.** For ts (one array per
   variable) the bulk fetch is four sequential zarr reads. `Dataset.load_async()` exists in xarray
   2026.7 and is the natural next step (Ryan's option 2), together with an async prefetching
   loader for the true dataloader pattern.

Raw CSVs (including the OOM-era array-indexed ts runs, `native_ts-q3_bulk.csv`, and the loop runs
with wire bytes): `docs/findings/experiments/q3-bulk/`. Cost-model rows for the bulk variant:
`docs/findings/bench/native_{ts,dl}-q3_bulk.csv` (query `q3_bulk`; `wxtco tco` ignores it until we
decide which Q3 pattern the cost model should price).

## Async patterns (2026-09-23, same node type, chunk cache off, wire bytes measured)

Two async variants were tried on top of a per-sample `lead_fields_async` (xarray `load_async`, which
loads a sample's four variables concurrently): `q3_async` awaits one sample at a time, `q3_gather`
puts all 16 samples in flight at once. Both are retired (Ryan, 2026-09-23): async is a form of
preloading with extra complexity, and the batched stream below covers the same ground with one
code path. The runs stay as evidence. Decoded bytes per run are 5.66 GB.

| Method | loop `q3` | bulk `q3_bulk` | async iter | gather | wire GB per run |
|---|---|---|---|---|---|
| native_dl | 17-22 s | 5.8 s | 10.8-13.0 s | 7.4-7.7 s | 1.42 (bulk 1.03) |
| native_ts | 181 s | 60 s | 189 s | OOM-killed (64 GB RSS) | 140 (bulk 22) |
| virtual | 39-41 s (60 cold) | 35 s (45 cold) | 38-40 s | 68-72 s | 2.4 |

- **dl**: async iter overlaps only the four variables of a sample (1.6x); gather overlaps everything
  (2.5x) but stays behind the single bulk selection, which also removes the per-sample shard-index
  reads (1.42 to 1.03 GB wire).
- **ts**: concurrency cannot fix chunk-shape over-read. Async iter still moves 140 GB at 740 MB/s.
  Gather held 64 loads in flight, each decoding whole 57-lead chunks (about 5 GB per variable per
  sample before the one lead is sliced out), and was OOM-killed on the 64 GB node after a minute.
  Only the bulk selection helps ts, because it reads each lead-chunk once for all eight leads.
- **virtual**: every pattern lands at 35-41 s and 2.4 GB wire, about 60 MB/s and ~1,600 GET/s of
  40 KB HDF5 chunks: the virtual path is request-rate bound, and no client-side pattern changes
  that. Gather is slower (70 s): 64 concurrent loads over-subscribe the zarr fetch semaphore and
  the event loop. Wire bytes are 2.2x the 1.07 GB the samples need at the source's 5.3x compression,
  from HDF5 chunk boundaries that do not align with one lead.

CSVs: `docs/findings/experiments/q3-async/`. Code: commit f7cd308 (removed in the
stream refactor).
