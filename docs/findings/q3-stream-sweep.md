# Q3 as a batched stream: batch-size sweep (2026-09-23)

Ryan's decision (2026-09-23): the dataloader is a streaming workload, so Q3 is now `q3_stream`
(commit 3c2c1a1): N samples delivered in batches of B. A batch is a block of consecutive
(cycle, lead) positions read with ONE `batch_fields` call per backend (one xarray selection and
one load for the Icechunk repos; one SQL statement for the table); batches are visited in a seeded
random order and the samples inside a batch are permuted in memory. The sweep here is 32 samples
(cycles 2026/09/15 T0000Z-T1800Z, leads 0-120 h, the four Q3 variables; 11.3 GB decoded) at
B = 1, 2, 4, 8, 16, 32, two runs each, on one m7i.4xlarge (16 vCPU, 64 GB, 6.25 Gbps = 781 MB/s
baseline), Icechunk chunk cache and DuckDB file cache off, wire bytes from `/proc/net/dev`.
`steady` excludes the first batch; at B=32 there is one batch, so only the total is given.

## Results (best of two runs; s = seconds for 32 samples)

| B | native_dl total / steady GB/s | native_ts total / wire GB | virtual total / wire GB | table_hilbert total / wire GB |
|---|---|---|---|---|
| 1 | 23.8 / 0.48 | 372 / 282 | 89 / 4.8 | 1,502 / 11.8 (one run) |
| 2 | 16.5 / 0.69 | 243 / 162 | 76 / 4.8 | 823 / 7.8 |
| 4 | 13.4 / 0.84 | 151 / 83 | 72 / 4.8 | 527 / 5.7 |
| 8 | 12.4 / 0.92 | 112 / 45 | 70 / 4.8 | 334 / 4.7 |
| 16 | 12.0 / 0.98 | 121 / 44 (108-114 at concurrency 32-64) | 70 / 4.8 | 254 / 4.2 |
| 32 | 12.2 / one batch | OOM at concurrency 256; 105 / 44 at concurrency 32 | 69 / 4.8 | OOM-killed |

Wire rates at the plateau: dl 170-180 MB/s, ts 400-430 MB/s, virtual 67 MB/s, table 17 MB/s.
Decoded rates at the plateau: dl 0.93-0.98 GB/s, ts 0.10-0.11 GB/s, virtual 0.16 GB/s, table
0.045 GB/s. The line rate is 781 MB/s.

## What each backend does with a bigger batch

- **native_dl** is flat from B=8: 12 s for 32 samples, 0.95 GB/s decoded, 2.05 GB on the wire at
  every B (the wire bytes are exactly the 64 inner chunks per sample; the earlier loop's extra 0.4 GB
  came from `lead_fields` reading the variables separately, not from batch size). B=8 is within 5%
  of the plateau, so the realistic Anemoi-style operating point already saturates this layout.
- **native_ts** improves 3.5x from B=1 to B=8 because each 57-lead chunk is read once for every
  lead in the batch: wire bytes fall from 282 GB to 44 GB. Past B=8 it is flat at 105-120 s; the
  remaining 44 GB is the three lead-chunks the eight leads span, decoded in full (about 240 GB of
  pcodec output per run). Larger batches are a memory hazard: B=32 at zarr concurrency 256 and 64
  were OOM-killed at 64 GB RSS; at concurrency 32 the same read takes 105 s in ~40 GB.
- **virtual** is flat from B=4 at ~70 s and 4.8 GB wire (67 MB/s, ~1,700 GET/s of 40 KB HDF5
  chunks): request-rate bound, and batching only removes the per-sample overhead (89 to 70 s).
- **table_hilbert** improves 5.9x from B=1 to B=16 (1,502 to 254 s) because one statement is
  planned once instead of 32 times (the plan alone is ~3 s over 19k files) and DuckDB fans the
  scan over all cores. It is still the slowest by 20x, at 17 MB/s wire, and B=32 was OOM-killed:
  one query returns 707 M rows (11 GB Arrow) and DuckDB sorts them for the `order by`.

## Where the time goes (CPU samples, `/proc/stat` over the run)

| Run | CPU busy, 16 vCPU | wire MB/s | decoded GB/s |
|---|---|---|---|
| native_dl B=16, concurrency 256 | 16% | 172-175 | 0.97 |
| native_dl B=16, concurrency 32 | 17% | 177-178 | 0.97 |
| native_ts B=16, concurrency 32 / 64 | 32% / 29% | 391-411 | 0.10 (2.9 GB/s of chunk output) |
| native_ts B=32, concurrency 32 | 33% | 416-427 | 0.11 |

This answers the "1 GB/s decode ceiling" question from the bulk experiment: it is not pcodec and
it is not the network. dl at its plateau uses about 2.6 of 16 cores and a quarter of the line
rate. ts decodes 2.9 GB/s of chunk output on the same node at 5 cores, so pcodec can go at least
3x faster than dl's 0.97 GB/s. A single busy core plus a few decode threads is the signature of a
serial section in the Python read path (zarr's sharding pipeline does per-chunk bookkeeping,
slicing and copies under the GIL; then `batch_fields` transposes and copies the result). The next
step, if dl's ceiling matters for the cost model, is a `py-spy` profile of one B=16 dl run; the
candidates are the zarr sharding codec's per-chunk Python work and the output copies.

## Memory model for sharded reads

Zarr decodes every touched inner chunk of a shard before writing the selection into the output,
and keeps up to `async.concurrency` shards in flight, so peak memory is roughly
(shards in flight) x (decoded bytes of the touched chunks per shard), independent of the size of
the result. For ts (1.26 GB decoded per shard, all chunks touched) that is 60-80 GB at concurrency
48-64 and 40 GB at 32; for dl (64 of 1,296 chunks per shard, 354 MB) it never mattered. Commit
7a8049b adds `WXTCO_ZARR_CONCURRENCY` to bound it. This is a second zarr behaviour worth an
upstream note, next to the list-indexing fallback in `q3-bulk-fetch.md`.

## Cost-model input

Ryan chooses the B the cost model prices. B=8 is the realistic loader operating point (4-8 samples
in flight per GPU); at B=8 the 32-sample stream costs native_dl 12.4 s, native_ts 112 s, virtual
70 s, table_hilbert 334 s, against the old 16-sample loop numbers of 14-22, 181, 39 and 585-603 s.
CSVs: `docs/findings/bench/*-q3_stream-b*.csv` (query `q3_stream`, `detail` JSON holds the steady
rates); reruns with concurrency and CPU samples: `docs/findings/experiments/q3-stream/`.

## Unsharded Anemoi layout at B=8 (2026-09-24)

Ryan's question: do the 7.2 GB shards and their small inner chunks cost the dl layout throughput?
A temporary repo `wxtco/mogreps-g-native-anemoi` holds the same decoded values as dl for the Q3
subset only (cycles 2026/09/15 T0000Z-T1800Z, the 8 Q3 leads, the 4 Q3 variables): one `data`
array (init, lead, variable, member, lat, lon), ONE unsharded chunk per sample of shape
(1, 1, 4, 18, 960, 1280) = 354 MB decoded, pcodec level 8 (the project's Zarr codec; Anemoi's own
default is Blosc). Built from the dl repo in 26 s and verified bit-identical
(`experiments/anemoi/build_anemoi.py`). Same node type, same `q3_stream` B=8 over 32 samples,
chunk cache off, runs interleaved anemoi / dl / anemoi / dl.

| Layout | total s (4 runs) | first batch s | steady GB/s decoded | wire GB | wire MB/s | CPU busy |
|---|---|---|---|---|---|---|
| dl: 7.2 GB shard, 1,296 inner chunks of 4.4 MB | 12.03-12.23 | 2.9-3.1 | 0.93-0.94 | 2.05 | 166-169 | 16% |
| Anemoi: one 354 MB chunk per sample, no shard | 9.02-9.13 | 2.3 | 1.24-1.27 | 2.05 | 224-228 | 17% |

- **Unsharded is 1.34x faster** at the same wire bytes (the compressed bytes are identical, so this
  is pure read-path overhead): 64 GETs of one ~64 MB object per sample-batch member instead of 16
  inner-chunk range reads per variable plus a shard-index read per sample.
- **It does not remove the ceiling.** The node is still 17% busy (about 2.7 of 16 cores) at a
  quarter of the line rate. With 8 chunks per batch in flight, only ~3 decode at once: the pcodec
  binding appears to hold the GIL, or zarr serialises the per-chunk decode. The next measurement is
  a single-chunk decode timing with 1 vs 8 threads; if pcodec is GIL-bound, a larger batch cannot
  help and a GIL-releasing codec (Blosc/zstd) or process-level parallelism would.
- Stored size is 2.50 GB, which includes ~0.6 GB of chunks orphaned by a first build attempt that
  failed on the append (xarray re-wrote the string `variable` coordinate with a new dtype; fixed by
  appending only the init-dependent variables). The repo is temporary; delete it with the Arraylake
  API when the comparison is no longer needed.

## Blosc LZ4 instead of pcodec, and a direct decode test (2026-09-24)

Ryan's follow-up: try Blosc LZ4, the fastest decoder. A second temporary repo
`wxtco/mogreps-g-native-anemoi-lz4` has the same unsharded one-chunk-per-sample layout with
Blosc LZ4, byte shuffle, clevel 5 (bit-identical to dl; 4.03 GB stored, ratio 2.81x against
pcodec's 5.69x). Same node type, Q3 stream B=8, 32 samples, interleaved, 4 runs each.

| Layout and codec | total s | first batch s | steady GB/s decoded | wire GB | wire MB/s | CPU busy |
|---|---|---|---|---|---|---|
| dl, sharded, pcodec 8 | 11.74-12.15 | 2.9-3.1 | 0.93-0.96 | 2.05 | 171 | 17% |
| Anemoi, unsharded, pcodec 8 | 8.90-9.25 | 2.25-2.40 | 1.24-1.28 | 2.05 | 227 | 17% |
| Anemoi, unsharded, Blosc LZ4 | 9.32-9.59 | 2.34-2.47 | 1.19-1.22 | 4.11 | 435 | 16% |

One 354 MB sample chunk decoded in memory on the same node (`experiments/anemoi-lz4/decode_timing.py`):

| Codec | ratio | 1 chunk, 1 thread | 8 chunks, 8 threads | thread speedup |
|---|---|---|---|---|
| pcodec 8 | 5.69x | 0.40 s (0.88 GB/s) | 0.50 s (5.71 GB/s) | 6.5x |
| Blosc LZ4 + shuffle | 2.81x | 0.20 s (1.79 GB/s) | 0.29 s (9.87 GB/s) | 5.5x |

- **The codec is not the bottleneck.** LZ4 decodes 2x faster and moves 2x the bytes, and Q3 time
  does not change (9.4 vs 9.0 s). pcodec is not GIL-bound: 8 threads decode 6.5x faster than one.
- **Neither is the network.** A B=8 batch is 2.83 GB decoded; its decode takes 0.3-0.5 s on 8 threads
  and its wire transfer 0.7 s (pcodec) or 1.3 s (LZ4) at the 781 MB/s line rate, yet a batch takes
  2.3 s for both codecs. About 1.5 s per batch is spent somewhere independent of both the codec and the
  byte count: the candidates are the per-batch output allocation (2.83 GB of fresh pages), the copies
  between zarr's decoded chunk buffers, the xarray result and `astype("float32")` in `batch_fields`,
  and no overlap between one batch's I/O and the next batch's decode. A `py-spy` profile of one B=8
  run would attribute it.
- **pcodec stays the right codec here**: same speed, half the storage and half the egress.

The two temporary repos (`mogreps-g-native-anemoi`, `mogreps-g-native-anemoi-lz4`) can be deleted with
the Arraylake API when the comparison is closed.
