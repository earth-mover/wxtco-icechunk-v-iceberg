# 011 Native Icechunk chunking and codec

**Status:** DEFERRED to stage B by design (Ryan, 2026-09-18). Decide after `table` and `virtual` query results exist; see spec section 4.3 and phasing step 7.

Source HDF5 chunks are (1 member, 128, 128) ~40 KB compressed; too small for object storage
(Planetary Computer measured 2x slower reads than h5netcdf at this size).

**Codec constraint (Ryan, 2026-09-20): every native Zarr layout encodes with pcodec**
(`zarr.codecs.numcodecs.PCodec`, `pcodec` package added to `pyproject.toml`). zstd/blosc only as a
measured comparison point, never as the default. Layout candidates under evaluation (2026-09-20):
Anemoi trajectories layout (ML dataloading) and the dynamical.org time-optimized schema (point
series); see the session log and `docs/findings/notes.md`.

**Stage B layout proposal (2026-09-20, after reviewing the Anemoi trajectories layout and
dynamical.org's GEFS archive):** one native Icechunk repo, pcodec throughout, two groups.

1. `timeseries` (GEFS pattern): one array per variable, dims (init, member, lead, lat, lon), inner
   chunk (1, 18, 57, 16, 16) ~1.05 MB, shard (1, 18, 171, 320, 320): 12 shards per init and variable,
   972 objects per cycle. **The extents divide the axes** (171 = 3 x 57, 960 = 60 x 16,
   1280 = 80 x 16), so a shard write covers whole shards and Zarr never read-modify-writes a stored
   shard on ingest; the earlier (1, 18, 64, 17, 16) / (1, 18, 192, 323, 320) pair did not divide and
   forced a read of every shard it touched. Q1 = 3 inner chunks (the lead axis is 3 chunks),
   Q2 (UK box, 2 vars, 2 cycles) = 144 chunks / 151 MB, Q3 = 19,200 chunks / 20.2 GB per sample
   (57x over-read, one lead read as 57): owns Q1 and Q2.
2. `dataloader` (Anemoi pattern, lat-lon shape kept because MOGREPS-G is a regular grid, Ryan):
   one `data` array, dims (init, lead, variable, member, lat, lon) holding **all 81 study slugs**
   (Ryan, 2026-09-20), the same set as `timeseries` so Q1-Q3 compare like for like; one shard per
   sample (1, 1, 81, 18, 960, 1280) ~7.2 GB of 1296 inner chunks (1, 1, 1, 18, 240, 320) ~5.5 MB
   read by byte range. **171 shard objects per cycle**, 4,788 for the 28-cycle study: the Anemoi
   one-object-per-sample property is kept, and a variable subset is byte ranges inside one object.
   A sample write covers exactly one whole shard, so Zarr never read-modify-writes a stored shard
   (verified: 0 GETs on a `data/c/...` key on both the first write and a rewrite).
   Per-variable `count`, `sums`, `squares`, `minimum`, `maximum` arrays, dims (init, lead, variable),
   chunk (1, 1, 81); a `variable` coordinate; `valid_time` (init, lead). Owns Q3.
   - **Sub-chunk order: Morton, the zarr default, on both layouts** (Ryan, 2026-09-22). An earlier
     proposal set `ShardingCodec(subchunk_write_order="lexicographic")` on the `dataloader` shard.
     zarr 3.4 does not persist that setting, so every region write reopens the array from metadata
     and reverts to Morton; the only workaround reached into `zarr.Array._async_array` and took the
     `dl` data write out of xarray. Not worth it
     (`docs/findings/xarray-issue-subchunk-write-order.md` is the issue record).
     **Why Morton is acceptable:** it keeps aligned power-of-two variable groups contiguous. With a
     4x4 spatial chunk grid, four consecutive aligned variables occupy one span of 64 inner chunks,
     so Q3's four variables at index 0-3 are the first 64 Morton codes, one contiguous ~350 MB span
     the reader's coalescer (1 MiB gap, 16 MiB maximum range) folds into sequential ranges.
     **Residual cost:** a subset that crosses a power-of-two boundary fragments into single inner
     chunks (~5.5 MB) more than 1 MiB apart, which the coalescer cannot merge, so it becomes one
     GET per chunk. Only the Q3 span is tuned. `timeseries` keeps Morton as well: it holds 2-D
     locality for box reads.
   - **Variable order**: `list(Q3_VARIABLES)` first, then the remaining study slugs in study order
     (`native_layout.dl_variable_order`), frozen in the `variable` coordinate at repo creation. It
     is what makes Q3 the first aligned group of four, hence one contiguous span.

Both layouts now hold the same 81 variables, so the native storage bill is **two full copies** of
the archive; the benchmark decides whether the `dataloader` copy beats serving Q3 from
`timeseries`. Lossless (no mantissa rounding), unlike dynamical.org.

Candidate for the native repo: dims (init, lead, member, lat, lon), chunk
(1, 1, 18, 240, 320) ≈ 5.5 MB float32 uncompressed, zstd level 3 with byte shuffle.
Consider a second layout optimized for point timeseries (chunk across leads) and report the
trade-off, since chunking is the tensor-side equivalent of table partitioning.
