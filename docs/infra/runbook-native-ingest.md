# Runbook: native Zarr ingest, one cycle at a time

Two independent layouts (decision 011), one Arraylake repo each, both pcodec level 8 and sharded:

| layout | repo | array shape | ingest unit | shard |
|---|---|---|---|---|
| `ts` | `<org>/mogreps-g-native-ts` | one array per variable, (init, member, lead, lat, lon) | one variable per cycle | (1, 18, 171, 320, 320), inner chunk (1, 18, 57, 16, 16) |
| `dl` | `<org>/mogreps-g-native-dl` | one `data` array, (init, lead, variable, member, lat, lon) | one lead per cycle | (1, 1, 81, 18, 960, 1280) ~7.2 GB, inner chunk (1, 1, 1, 18, 240, 320) ~5.5 MB |

Both layouts hold **all 81 study slugs** (`wxtco.slugs.study_slugs()`), frozen at repo creation,
so Q1-Q3 compare like for like. `Q3_VARIABLES` names the four benchmark variables only.

`dl` also carries Anemoi-style per-sample statistics, dims (init, lead, variable): `count`,
`sums`, `squares`, `minimum`, `maximum`. Mean and stdev are derived by readers.

**One object per sample.** A `dl` shard is the whole (variable, member, lat, lon) slab of one
(init, lead): **171 shard objects per cycle**, 4,788 for the 28-cycle study, each ~7.2 GB
uncompressed. Object count stays Anemoi-like; a subset read is byte ranges inside one object.

**Sub-chunk order: Morton, the Zarr default, on both layouts** (Ryan, decision 011). Zarr packs the
1296 inner chunks of a `dl` shard in Morton order and does **not** store which order it used, so a
reopened array always writes Morton; the alternative was a write-time-only codec setting that no
region write could preserve (`docs/findings/xarray-issue-subchunk-write-order.md`).

Morton order is acceptable because it keeps **aligned power-of-two variable groups contiguous**.
With dimension order (init, lead, variable, member, lat, lon) and a 4x4 spatial chunk grid, the
four variables of a group interleave with their 16 spatial chunks over one span of 64 inner chunks.
The `variable` coordinate puts the Q3 variables first (`native_layout.dl_variable_order`), so Q3 is
variables 0-3, the first 64 Morton codes, one contiguous span of ~350 MB that the reader's range
coalescer (1 MiB gap, 16 MiB maximum range) folds into sequential ranges.

**Residual cost:** a variable subset that is not an aligned power-of-two group fragments. Crossing a
boundary (say variables 3 and 4) splits the read into single inner chunks of ~5.5 MB whose
neighbours are more than 1 MiB away, so the coalescer cannot merge them and the read becomes one
GET per chunk instead of a few sequential ranges. Only the Q3 span is tuned; an arbitrary subset is
not. `ts` keeps Morton as well, which holds 2-D locality for box reads.

The two layouts share nothing. Run them on **two nodes in parallel**, one per layout.

## How a layout is written

Every write goes through xarray. Dask supplies the placeholders that make a write metadata-only.

1. **Template.** `native_layout.create_ts` / `create_dl` build an `xarray.Dataset` from a
   `GridSpec` (members, lat, lon, leads) with the full dims and coordinates, an **empty init axis**
   and a `dask.array.full` placeholder per data variable, and write it with
   `ds.to_zarr(session.store, mode="w-", compute=False, zarr_format=3, consolidated=False,
   encoding=...)`. `compute=False` writes metadata only. Per variable the encoding carries
   `chunks` (the inner chunk), `shards`, `serializer=PCodec(level=8)`, `compressors=None`,
   `filters=None` and `fill_value=np.nan`. The coordinates carry the CF encoding (`init` and
   `valid_time` int64 seconds since 1970-01-01, `lead` int64 seconds), so readers decode
   `datetime64` and `timedelta64` with no keyword.
2. **One init slot per cycle.** `native_layout.append_init` writes the real `init` and `valid_time`
   coordinates plus a lazy NaN placeholder for **every** data variable with
   `to_zarr(append_dim="init", compute=False)`. xarray resizes every array and stores no data
   chunk, so there is no Zarr-level resize any more. `to_zarr` **replaces** the group attributes,
   so the stored `wxtco.slugs` list rides along on that write. Verified on a local repo and by
   `tests/test_native_ingest.py::test_append_init_grows_every_array_without_writing_data`: every
   data array grows by one, the only chunk keys written are `init/c/0` and `valid_time/c/0/0`, the
   new slot reads NaN, and a later region write fills it with zero chunk GETs.
3. **Unit writes.** Each worker builds a one-slab `Dataset` and writes it into its fork with
   `slab.drop_encoding().to_zarr(fork.store, region={"init": slice(i, i + 1), ...})`. The slab
   spans whole shards on every non-init axis, so Zarr never read-modify-writes a stored shard
   (`test_ts_region_write_reads_no_chunk` and `test_dl_region_write_reads_no_chunk` count the chunk
   GETs). The `dl` sample shard and its statistics row go through that same call.
4. **Commit.** The parent merges every fork, then commits once with
   `session.commit(msg, rebase_with=ic.ConflictDetector(), rebase_tries=5)`.

`tests/test_native_ingest.py::test_layout_metadata_matches_the_golden_capture` compares the stored
array metadata against `tests/native_layout_golden.json`, a capture of the layout as the
earlier hand-built Zarr implementation wrote it. Any change to codecs, dimension names, fill values
or chunk and shard shapes must update that file deliberately.

## Where to run

- **EC2**, instance role `wxtco-ec2` (Plan 04 Task 8 launch scripts). This is the normal path: the
  ingest reads every NetCDF byte of `s3://em-tco-mogreps/netcdf/` and rewrites it, so it must run
  in-region with a lot of RAM.
- **Ryan's laptop**, `export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>`, only for a `--variables`
  smoke run.

Do **not** run this in the Claude Code cloud session: the `wxtco-controller` key is denied
`s3:ListBucket` on `netcdf/` (see `docs/infra/access-checklist.md`).

## Prereqs

- `.env` with `ARRAYLAKE_TOKEN` and `ARRAYLAKE_ORG` (EC2 reads the token from Secrets Manager
  `wxtco/arraylake-token`).
- `study_cycles.txt` present; the copy verified (`uv run wxtco copy verify --cycle "$CYCLE"`).
- Arraylake bucket config `wxtco-storage` on the org. No virtual chunk container and no virtual
  chunk access policy: the native repos hold their own bytes.
- Commit before an EC2 launch: `launch.sh` ships `git archive HEAD`.

## 1. Create the repos

There is no separate create step. The first `ingest native` call for a layout creates the repo,
reads one source file for the grid, and commits the layout (coordinates, arrays, the frozen
`variable` list for `dl`). The lead axis is always the fixed 171 leads; the grid comes from the
cycle, so **run the first cycle with the full selection**, no `--variables`.

## 2. Node A, `ts` layout

    CYCLE=$(head -1 study_cycles.txt)
    uv run wxtco ingest native --group ts --cycle "$CYCLE" --workers 5 --file-workers 8
    tail -n +2 study_cycles.txt | while read c; do
      uv run wxtco ingest native --group ts --cycle "$c" --workers 5 --file-workers 8 || exit 1
    done

## 3. Node B, `dl` layout

    CYCLE=$(head -1 study_cycles.txt)
    uv run wxtco ingest native --group dl --cycle "$CYCLE" --workers 6 --file-workers 8
    tail -n +2 study_cycles.txt | while read c; do
      uv run wxtco ingest native --group dl --cycle "$c" --workers 6 --file-workers 8 || exit 1
    done

`dl` ingests all 81 study slugs, ordered Q3 first. The list is frozen in the repo attributes and
in the `variable` coordinate when the repo is created, and a later run with a different set is
rejected; the ingest block always follows the stored coordinate, not the caller's slug order.
`--variables` is rejected for `dl`: one shard holds every variable, so a restricted run would
rewrite the shard and blank the variables left out. Use it for `ts` only.

**`|| exit 1` is not optional.** A failed cycle must stop the loop: if the loop goes on, the next
cycle appends a newer init slot and the failed cycle can no longer be added (section 5).

## 4. Workers and memory

`--workers` is the number of ingest units in flight, one worker process each (spawned, never
forked: obstore and Icechunk hold tokio runtime threads). `--file-workers` is the threads that
read NetCDF files inside one unit.

| layout | per worker | workers | node |
|---|---|---|---|
| `ts` | ~15 GB block + ~5 GB encode buffers + 8 x 88 MB reads = **~21 GB** | 4 (~84 GB), default 5 (~105 GB) | `--workers 5` needs ~110 GB of headroom, e.g. `r7i.8xlarge` (32 vCPU, 256 GB); use `--workers 4` on a 128 GiB node |
| `dl` | 7.2 GB sample + ~6 GB shard encode buffer = **~12-15 GB** | 6 (~90 GB), 4 (~60 GB) | `--workers 6` needs a 128 GiB node, e.g. `r7i.8xlarge`; use `--workers 4` on a 64 GiB node |

`ts` is the memory-hungry one: one unit holds a whole (18, 171, 960, 1280) float32 variable in
memory. The worker caps Zarr at `async.concurrency` 4, which halves the encode buffer peak at the
same wall clock. Halve `--workers` rather than the node size if RAM is short; throughput falls
roughly linearly.

`dl` is now the second memory-hungry one: a unit holds all 81 variables of one lead (7.2 GB) and
Zarr assembles the whole shard before the PUT, measured at 0.8-0.9x the block on a full-grid write
(3 and 8 variable trials). Its worker caps `async.concurrency` at 4 as well. Do not raise
`--workers` past what RAM allows; `dl` is read-bandwidth bound only below that ceiling.

## 5. Commit model and resume

One cycle is **one commit**. The parent session appends one init slot, which grows every
init-axis array by one and stores only the two coordinate chunks, then calls `Session.fork()`
once per unit for the worker processes, merges the forks back and commits. Icechunk 2.2.2 accepts
a fork taken after an uncommitted append and the workers see the new shape, so no separate
metadata commit is needed
(verified on a local repo; `tests/test_native_ingest.py::test_two_cycles_append_and_decode`
asserts the ancestry length). The only extra commit is the one-time layout commit.

Rerunning a cycle is safe and cheap:

- `_progress/native_<group>/<cycle_id>.json` in the copy bucket records the finished units
  (`var=<name>` for `ts`, `lead=<minutes>` for `dl`); a rerun skips them and makes no commit.
- If the progress mark is lost, the **init coordinate** is the second guard: a cycle already on
  the init axis reuses its slot instead of appending a new one, so the rerun overwrites the same
  chunks rather than duplicating the cycle.
- A worker failure aborts the cycle before the commit, so a failed cycle leaves no partial data.
- A commit that races another writer on the same branch rebases with `ConflictDetector`, up to
  5 tries (`session.commit(..., rebase_with=..., rebase_tries=5)`). A rebase that fails is a real
  conflict (the other writer took our init slot); the error names the cycle and the run stops.

Cycles must be ingested oldest first. The init axis is append-only, so a cycle that is **absent and
older than the last slot** is rejected. To recover from a skipped cycle, ingest it before any newer
one; once a newer cycle is committed the only fix is to rebuild the repo from the oldest cycle.
This is why every loop above ends in `|| exit 1`.

## 6. Verify a cycle

    uv run python -c "
    import numpy as np
    from wxtco.config import Settings
    from wxtco.ingest.native import open_native_repo
    from wxtco.queries.native_backend import open_native_dataset
    group = 'ts'
    ds = open_native_dataset(open_native_repo(Settings.from_env(), group, 'arraylake'), group)
    print(ds.sizes)
    v = ds['temperature_at_screen_level'].isel(init=-1)
    print('finite fraction', float(np.isfinite(v.isel(member=0, lead=0).values).mean()))"

Expect `lead=171`, `member=18`, `lat=960`, `lon=1280`, one `init` per cycle ingested, a
`datetime64` `init`, a `timedelta64` `lead`, and a finite fraction of 1.0 for a diagnostic that
exists at that lead. A NaN field means the diagnostic has no file at that lead, which is normal
for the 3-hourly accumulations and for `precipitation_accumulation-PT01H` at lead 0.

For `dl`, also check the statistics line up with the data:

    uv run python -c "
    import numpy as np, zarr
    from wxtco.config import Settings
    from wxtco.ingest.native import open_native_repo
    store = open_native_repo(Settings.from_env(), 'dl', 'arraylake').readonly_session('main').store
    print(zarr.open_array(store, path='count')[-1, :3])
    print(zarr.open_array(store, path='minimum')[-1, :3])"

Expect `count` = 18 x 960 x 1280 = 22,118,400 per variable at a populated lead, and 0 where the
variable has no file.

## 7. Record the result

Append wall clock and the instance type to `docs/findings/etl/native_ts.csv` and
`docs/findings/etl/native_dl.csv`. Create either file with this header if it is absent:

    cycle,instance_type,workers,read_seconds,write_seconds,commit_seconds,seconds,bytes_uncompressed

The per-cycle summary line printed by the CLI carries all of these. `bytes_uncompressed` is the
decoded block size, not the stored bytes; read stored bytes from the repo instead.

The CLI also logs two coverage lines per cycle: source leads that are off the repo lead axis (with
the first few values) and the count of (variable, lead) pairs with no file, whose slabs stay NaN.
Both counts are on the summary line as `leads dropped` and `missing files`.

## 8. Query the layouts

    uv run wxtco bench --method native_ts --query q1 --params '{"lat":51.5,"lon":-0.1,"cycle":"'"$CYCLE"'"}' --runs 5 --out docs/findings/bench/q1.csv
    uv run wxtco bench --method native_dl --query q3 --params '{"vars":["temperature_at_screen_level","wind_speed_at_10m"],"cycles":["'"$CYCLE"'"],"leads":[0,24,48]}' --runs 5 --out docs/findings/bench/q3.csv

Add `--local PATH` to point either method at a local Icechunk repo instead of Arraylake.
