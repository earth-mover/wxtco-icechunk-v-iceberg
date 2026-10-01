# Running notes: facts learned about data, infra, tooling

Append-only. Date every entry. Cite the command or source.

## 2026-09-18

- **MOGREPS-G source bucket** `s3://met-office-global-ensemble-model-data/global-ensemble/`,
  eu-west-2, anonymous. Days present: 2026-08-17 .. 2026-09-18 (~31 days). 4 cycles/day
  (T0000Z, T0600Z, T1200Z, T1800Z). Cycle 2026-09-16 T0000Z: 14,020 objects, 2.107 TB.
  Surface-only (no `*_levels`): 231.8 GB. Per-variable table in
  `docs/findings/mogreps-g-cycle-inventory.md`.
- **File structure** (`temperature_at_screen_level`, lead 0): dims (realization 18, latitude 960,
  longitude 1280), float32, HDF5 chunks (1,128,128), zlib level 1, no shuffle,
  `least_significant_digit: 2`. Scalar coords: forecast_period, forecast_reference_time,
  height, time. Grid: lat -89.91..89.91, lon -179.9..179.9. Attrs: CF-1.7, UKMO-1.0.
- **Lead schedules** per diagnostic: 171 (hourly to 132h then 3-hourly to 246h), 170, 169,
  132 (PT01H stats), 83 (landsea_mask), 38 (PT03H stats).
- **Files land** T+6h15m .. T+8h20m after cycle time; no completion marker; cycle deletion
  is atomic (source: the reference pipeline's README).
- **Virtual ingest timings** (Joe, Modal `uk` region): ~0.96 s/file CPU-bound, cycle wall clock
  ≈ 11,521 x 0.96 / cores. From a laptop: 81 min with 48 procs, bandwidth bound, 26 MB read
  per 35 MB file. Virtual repo footprint per cycle: 376 MB (291 MB manifests).
- **Flux SQL could not read virtual chunks** on 2026-09-13; Ryan says fixed upstream as of 2026-09-18. Verify.
- **Existing virtual repo** `metoffice/mogreps-g`: R2 storage (Cloudflare), Icechunk 2.2.2,
  120-slot ring on forecast_reference_time, manifests split per init time. Do not reuse.
- **UK bucket** `met-office-uk-ensemble-model-data/uk-ensemble/` is a different model
  (MOGREPS-UK); one cycle 14,330 objects, 255 GB.

## 2026-09-18 (session 003, cloud)

- **obstore 0.11.1 API facts** (verified in the `wxtco` venv while executing Plan 01):
  `S3Store(bucket, region=None)` raises `TypeError` (typed `S3Config` kwargs reject `None`); pass
  `region` only when set. `obs.get(store, missing_key)` raises builtin `FileNotFoundError`, not
  `obstore.exceptions.NotFoundError`; catch both. `obs.get(...).bytes()` returns `obstore.Bytes`,
  wrap in `bytes()`. `list_with_delimiter` `common_prefixes` carry no trailing slash. Pages from
  `obs.list` are lists of dicts with `path`, `size`. obstore has no `AWS_PROFILE` support.
  `LocalStore.list_with_delimiter` returns empty directories as prefixes; S3 does not.
- **Copy bucket root** holds `_code/`, `_logs/`, `_progress/` beside `YYYY/`; any cycle walker must
  skip non-digit prefixes (`available_cycles` does).
- **Cloud env `wxtco`**: after the allowlist fix, `aws` CLI 1.46.1 is installed by the setup script,
  PyPI and `*.amazonaws.com` reachable, STS returns `user/wxtco-controller`.
- **pyiceberg 0.12** ignores `write.parquet.row-group-size-bytes` (warns only); row groups follow
  `write.parquet.row-group-limit` in rows (default 1,048,576). We set 380000 rows ≈ 128 MiB at
  356 B/row. `Snapshot.summary` exposes `snapshot_properties` passed to `append`, so the ingest
  guards resumed leads by scanning summaries for `wxtco.unit`. `pyiceberg[sql-sqlite]` is the extra
  for the local `SqlCatalog`. `RestCatalog` header kwargs (`header.X-...`) reach the HTTP session.
- **DuckDB iceberg extension in the cloud session**: `INSTALL iceberg` fails (default repo is plain
  HTTP → 403; with `custom_extension_repository` on HTTPS the core downloader cannot pass the proxy
  TLS even after `extensions.duckdb.org` was allowlisted). PyPI wheels `duckdb-extension-avro` and
  `duckdb-extension-iceberg` (pinned to the duckdb version) load by path; `wxtco.duck.connect_iceberg`
  does that and falls back to INSTALL on EC2 or a laptop. `iceberg_scan` accepts `file://` paths.
- **Allowlist edits take effect without a session restart** (HTTPS to a newly added host returned
  200 within a minute).
- **Upstream repo access from the cloud session**: `raw.githubusercontent.com` and unauthenticated
  `git ls-remote` are blocked; the session can reach a private repo only after it is added to the
  Claude GitHub App installation (github.com/settings/installations) and then attached with the
  session's `add_repo` tool. The change took a few minutes to propagate. Clone lands at
  the reference pipeline checkout.
- **Reference pipeline is append-only on `forecast_reference_time`** (`slot = cycle_index -
  origin`, no modulo). The upstream README still describes a 120-slot ring; the code comment says the
  ring was abandoned. Plan 03 is correct. The completeness gate `SINGLE_LEVEL_FILES = 11521` equals
  the surface-only file count of our copy, so it holds against `netcdf/`.
- **Virtual ingest on the fixture works end to end** (local Icechunk repo, `file://` virtual chunk
  container with `ic.credentials.LocalFileSystemAccess`, virtualizarr 2.7.3 HDF parser through an
  obspec-utils registry): two synthetic cycles in ~6 s, values read back through references.
  `icechunk.VirtualChunkContainer` requires the url prefix to end in `/`. arraylake 1.3.0 `get_repo`
  accepts `authorize_virtual_chunk_access`; the grant is per open, so upstream's existing-repo path
  could not read virtual chunks after the first run.
- **Plan 04 facts**: xarray decodes the virtual repo's `forecast_period` as int64 seconds (attr
  `units: seconds`, no timedelta decoding); `time` there has dims (init, lead). duckdb 1.5.5 wheels:
  httpfs is not built in; `duckdb-extension-httpfs` exists on PyPI. `CREATE SECRET ... TOKEN ?, ENDPOINT ?`
  accepts bound parameters. AL2023 ships aws-cli v2 (do not `dnf install awscli`); `aws ec2 run-instances
  --user-data` base64-encodes by itself, so pass `file://` not pre-encoded text; SSM Run Command does not
  export `HOME`. Both backends agree bit-for-bit on the fixture for point, box and lead reads.

## 2026-09-19 (session 003, production start)

- **First EC2 launch worked first time**: c7i.16xlarge, AL2023, cloud-init 30 s to READY (uv 0.12.17,
  aws-cli 2.33, `uv sync --frozen` from the S3 code tarball). `scripts/ec2/launch.sh` and `run_job.sh`
  as committed.
- **S3-to-S3 copy from the Met Office bucket needs `--copy-props none`**: the default copies object
  tags and calls `GetObjectTagging` on the source, which the public bucket denies to signed principals
  (`AccessDenied` on ~85% of objects). Fixed in `scripts/copy_cycle.sh` and `wxtco copy plan`.
- **Copy throughput**: one `aws s3 cp --recursive` with `max_concurrent_requests 64` moves ~2.2 Gbps
  (~35 GB per 130 s, ~15 objects/s). The instance is idle (11% CPU on the CLI, 0.45 GB through the
  NIC): S3 copies server-side and the bound is cross-region `CopyObject` latency x concurrency. Ryan
  flagged the rate as too slow; the remedy is more processes, so the remaining 27 cycles run as 27
  concurrent `copy_cycle.sh` (xargs -P 27, 100 concurrent requests each, load ~3.6, 0 failures).
- Arraylake org `wxtco` (from the instance): bucket configs `wxtco-storage` (`arraylake/`) and
  `wxtco-netcdf` (`netcdf/`) present; no repos yet.
- **Static copy done** (2026-09-19 02:33-02:50 UTC): 27 cycles copied concurrently in 898-943 s each
  (6.26 TB in ~16 min wall, ~6.5 GB/s aggregate); every cycle verified 11,521/11,521 files, 231.5-232.2 GB,
  0 missing, 0 failed objects. Cycle 2026/09/12/T0000Z ran alone with 64 concurrent requests and took
  ~25 min for the same size. Commands: `scripts/copy_cycle.sh <cycle>` via `scripts/ec2/run_job.sh`, logs
  `s3://em-tco-mogreps/_logs/copy-*.log`.
- **Table ingest memory**: one `wxtco ingest table` process peaks at ~27 GB per lead (7 GB Arrow table plus
  pyiceberg/pyarrow write copies), averaging ~15 GB. Six concurrent cycles on the 123 GB c7i.16xlarge
  OOM-killed three processes (exit 137) within minutes; four concurrent is stable at ~62 GB. Per-lead
  wall time ~36 s regardless of concurrency (CPU load ~11 of 64), so the run is bound by per-process
  Parquet writing, not by S3. Expect ~1.7 h per cycle, 4 at a time, ~12 h for 28 cycles.
- **Table ingest memory grows across leads within one process**: RSS 22-30 GB after 2-3 min and one
  process reached 45 GB before the kernel killed it (also killed systemd-journal); four concurrent
  cycles OOM after ~18 min. Remedy in force: run each cycle as 10-lead chunks (`--leads a,b,...`), one
  fresh process per chunk (progress makes this idempotent), three cycles concurrent, with
  `MALLOC_ARENA_MAX=2 ARROW_DEFAULT_MEMORY_POOL=system`. Follow-up for stage B: find the retained
  memory in `lead_table`/`table.append` (suspect pyarrow pool retention plus pyiceberg's schema cast copy).
- **Virtual ingest**: origin cycle 2026/09/12/T0000Z seeded in 855 s (14.2 min, 24 workers, under CPU
  contention). Cycle 2 failed with obstore S3 `Connect TimedOut` after 184 s of retries at the same
  minute the machine was thrashing from the table OOMs; relaunched with 16 workers after clearing memory.
- **Stopping SSM jobs**: `pkill -f` patterns that match the job's own command string also match the
  SSM wrapper of any job launched afterwards (a relaunch was killed with exit 143 this way), and one
  `pkill` command failed to stop a pool at all. Reliable method: find the job's `xargs` PID, read its
  process group (`ps -o pgid=`), and `kill -TERM -- -<pgid>`; each SSM command runs in its own group.
  Memory with 3-lead chunks: 3 processes, 18 GB total right after start (was 96 GB with 10-lead chunks).
- **Virtual ingest S3 timeouts track memory pressure**: both failures (`Connect TimedOut`, then GET
  `TimedOut` after 231 s) happened while the OOM killer was active (kills 62→69→104). With one-lead table
  processes at ~15 GB each, 6 concurrent plus 16 virtual workers exceeded 123 GB. Now 5 table cycles
  concurrent; the virtual loop retries a cycle up to 3 times (the pipeline resumes per variable).
  Virtual cycles 2-7 took 1039-1146 s each (17-19 min) with 16 workers under contention.
- **Table ETL accounting**: `docs/findings/etl/table.csv` records steady-state instance-seconds per
  cycle = 171 leads x 41 s / 5 concurrent cycles ≈ 1402 s (the instance was shared; wall time per cycle
  was 2000-2600 s and includes restarts). pyiceberg splits each 7 GB lead append into 4 Parquet files
  (`write.target-file-size-bytes` 2 GiB), so a cycle is 684 data files, not 171.
- **Duplicate lead**: cycle 2026/09/12/T0600Z lead 25 h had 8 files (appended twice while two table
  pools overlapped on 2026-09-19 ~03:35). Repaired with a partition delete + single re-append
  (`/opt/wxtco/dedupe.py`). Check `files per partition == 4` for every partition before benchmarking.
- **Virtual ETL**: cycles 2-9 took 1039-1146 s each with 16 workers while sharing the instance with the
  table ingest; the origin cycle took 855 s with 24 workers. Recorded in `docs/findings/etl/virtual.csv`.
- **Virtual repo verified mid-run (11:50 UTC)**: groups `surface`, `surface_169leads`, `surface_170leads`,
  `surface_83leads`, `surface_PT01H`, `surface_PT03H`; `surface` dims (init 30, lead 171, member 18,
  960, 1280); 26 of 30 slots status 0 (the axis is sized to "now"), one commit per cycle (28 snapshots),
  a value reads through the virtual reference via the `wxtco-netcdf` bucket config (297.03 K). Grid:
  lat -89.90625..89.90625 step 0.1875, lon -179.859375..179.859375 step 0.28125. London cell (754, 639)
  = (51.46875, -0.140625). Bench box UK: lat 48.94..59.06, lon -8.16..2.25 (54 x 37 cells).
- **Virtual ingest complete 2026-09-19 12:15 UTC**: 28 cycles, 1038-1216 s each (origin 855 s), all
  slots status 0, 30 snapshots. CONTAMINATED timings: 16 workers (24 for the origin) while sharing the
  c7i.16xlarge with 4-6 table processes. The pipeline parallelizes files within a variable across
  `--workers` processes and walks the 81 variables serially; at ~1 CPU-s per file, 48 idle workers should
  give ~4-5 min per cycle. A clean one-cycle re-run on an idle node replaces these numbers in the model.
- **Isolation from 12:13 UTC (Ryan's request)**: table pool stopped by pgid at 3209/4788 leads, virtual
  finished its last cycle alone, table restarted alone (`table-all9`), benchmarks moved to a separate
  m7i.4xlarge `i-01d2f67f0222ea413`.
- **Tensor read path (Ryan, 2026-09-19 12:40 UTC)**: `open_virtual_dataset_group` now opens with
  `chunks=None` (no dask; zarr fetches only the indexed chunks) and the tensor backend sets
  `zarr.config.set({"async.concurrency": 256, "threading.max_workers": 16})`. Before the change, virtual
  Q1 on the m7i.4xlarge took 32.3, 25.2, 23.7, 22.2, 21.3 s (dask path, default concurrency). A point
  series touches ~3,000 (1,128,128) HDF5 chunks, one S3 range request each, so fetch concurrency is the
  lever. Benchmarks were restarted from scratch with the new code (`bench-virtual2`).
- **Virtual benchmarks, m7i.4xlarge alone, after the read-path change (bench-virtual2, 12:45-13:00 UTC)**:
  Q1 point series 1.69/1.07/0.92/1.15/1.11 s (median 1.11 s; 20x faster than the dask path);
  Q2 UK box + CRPS 21.9/18.2/17.6/18.1/18.1 s (median 18.1 s); Q3 dataloader 16 samples x 4 vars
  49.5/37.5/39.1/37.5/38.8 s (median 38.8 s, ~5.4 GB decoded → ~140 MB/s). CSVs in
  `docs/findings/bench/virtual-q*.csv`. Table benchmarks run on the same node after the table ingest.
- **Clean ETL timings (idle c7i.16xlarge `i-00896cb2e9d51e8db`, cycle 2026/09/15/T0000Z, 13:00 UTC)**:
  virtual 384 s with 60 workers into a local scratch Icechunk repo (293 MB per cycle); table 1495 s with
  5 interleaved one-lead-per-process groups into scratch table `mogreps.surface_bench` (mean 40.0 s per
  lead, 171 leads). `docs/findings/etl/{virtual,table}.csv` now carry these per cycle; the overnight
  shared-node numbers moved to `docs/findings/etl/contaminated/` (not read by `wxtco tco`). Table ETL is
  ~3.9x the instance time of virtual; the table lead loop is bound by single-process Parquet encoding
  and memory, not by S3.
- **Storage prefixes at 13:10 UTC** (`aws s3 ls --summarize` from the instance role):
  `arraylake/q3sgfc5m_mogreps_g_virtual/` 6,785 objects, 18.56 GB for 28 cycles (≈663 MB/cycle; Joe
  measured 376 MB/cycle with per-cycle shards and GC; ours packs 4 cycles per shard, so superseded shard
  versions remain until garbage collection). `arraylake/0bpeglkn_mogreps/` (Iceberg warehouse) 24,984
  objects, 4.06 TB with ~70% of the table ingested; it also holds orphaned Parquet from the dropped
  `surface_bench` (one cycle, ~290 GB) and smoke tables because `drop_table` does not purge files.
  Final table storage will be taken from Iceberg metadata (`file_size_in_bytes` of live data files) and
  the orphans flagged for Ryan to purge (`catalog.purge_table` was not used).
- **Instance-size sensitivity, virtual queries (c7i.16xlarge alone, 13:15 UTC)**: Q1 1.76/0.87/0.86/1.08/1.02 s
  (median 1.02 vs 1.11 on m7i.4xlarge); Q2 22.4/17.0/17.0/17.1/17.0 s (median 17.0 vs 18.1); Q3
  36.4/34.0/36.5/35.7/34.6 s (median 35.7 vs 38.8). Four times the cores and bandwidth bought 5-8%:
  the virtual read path is bound by per-request latency to S3 over many small HDF5 chunks, so the
  m7i.4xlarge is the right (cheaper) query node and Ryan's concurrency setting is the lever that matters.
  CSVs in `docs/findings/bench/sensitivity/` (subfolder; not read by `wxtco tco`).
- **Table verification at 96% (16:35 UTC, from Iceberg metadata on the bench node, 17 s)**: 4,618
  partitions, every one with exactly 4 data files (no duplicates after the one repair), 4,621 snapshots,
  102.14 B rows live, 5,246.6 GB of Parquet (zstd) → ~1.14 GB per lead, so the full 28-cycle table will
  be ~5.44 TB, about 84% of the 6.5 TB NetCDF copy. Table benches wait until the last append lands.
- **Table ingest complete 2026-09-19 17:40 UTC**; all 28 cycles exit 0. Ingest node `i-072d24499c2f04afd`
  (c7i.16xlarge) terminated after 15.30 h (~$43.70 on-demand); it hosted the copy (~40 min), the
  virtual ingest (~9 h shared), and the table ingest (~14.5 h, of which ~5 h shared). Per-cycle logs
  archived at `s3://em-tco-mogreps/_logs/ingest-node/`. The wall-clock split between methods on that
  node is not clean; the cost model uses the clean single-cycle timings from the isolated node instead.
- **Final table verification (18:00 UTC)**: 4,788 partitions x exactly 4 files = 19,152 data files,
  105,902,899,200 rows = 28 x 171 x 22,118,400 exactly, 4,791 snapshots, 5,440.6 GB live Parquet
  (84% of the 6.49 TB NetCDF copy). `docs/findings/storage.csv`: table data 5440.6 GB, with_netcdf
  11934.6 GB; virtual data 18.56 GB, with_netcdf 6512.6 GB (the virtual repo cannot exist without the copy).
- **Table Q1 cold start**: first run 250 s, then 3.9-4.6 s. The first query pays for planning over 19,152
  files (REST catalog manifest fetch + DuckDB metadata) plus the one-time grid query; warm runs measure
  the point read. The bench CSV keeps all five; the cost model takes the median, so the cold run does
  not dominate, but a service that opens a fresh connection per query would see the 250 s.
- **Table benchmarks, m7i.4xlarge alone (bench-table, 17:45-18:05 UTC, table final)**: Q1 point series
  250.2 s cold then 3.94/3.92/4.56/4.58 s (median 4.56 s; virtual 1.11 s); Q3 dataloader 16 samples x
  4 vars 149.8 s cold then 94.6/93.7/93.9/94.3 s (median 94.3 s; virtual 38.8 s). Q2 failed once with an
  HTTP GET error mid-scan (DuckDB reading Parquet from S3 through the vended credentials) and is being
  rerun. Cold runs include first-time Iceberg planning over 19,152 files.
- **Table Q2 failure root cause (18:35 UTC)**: `ExpiredToken` on an S3 GET of a data file. The Arraylake
  Iceberg REST catalog vends S3 credentials with `s3.session-token-expires-at-ms` about 30 minutes
  ahead; DuckDB's iceberg ATTACH takes them once and never refreshes, so any connection older than
  ~30 min fails mid-scan (and one rerun then hung at 0% CPU for 15 min). Q1/Q3 finished inside the
  window. Options: re-attach per query (repays the 250 s planning cold start each time), or read the
  Parquet with the instance role (`CREATE SECRET ... PROVIDER credential_chain`) and use the catalog
  only for metadata; being probed. This is a real operational property of the table method as served
  through the REST catalog, worth a line in the summary.
- **Table Q2 (bench-table-q2c, 19:21-19:58 UTC, m7i.4xlarge, role-based S3 reads, 16 threads)**: one run,
  2208.6 s. Fresh connection so it includes planning. 7.6 B lat/lon rows scanned at ~3.4 M rows/s;
  earlier vended-credential attempts failed (ExpiredToken) or hung. `docs/findings/bench/table-q2.csv`.
- **Final `wxtco tco`** (2026-09-19): table 634 (own bytes) / 783 (with copy) USD/month; virtual 112 / 262.
  10x queries: table 3,932 / 4,081; virtual 789 / 938. Summary: `docs/findings/stage-a-summary.md`.
- **Instances**: ingest c7i.16xlarge 15.30 h, clean-timing c7i.16xlarge 0.72 h, bench m7i.4xlarge 7.77 h,
  all terminated; on-demand total ≈ $52. Orphaned Parquet from dropped smoke/scratch tables remains under
  `s3://em-tco-mogreps/arraylake/0bpeglkn_mogreps/` (~300 GB, ~$7/month) for Ryan to purge.

## 2026-09-20 (session 004): Hilbert layout experiment and cache corrections

- **Hilbert scratch table** `mogreps.surface_hilbert`: 2 cycles (2026/09/15 T0000Z, T0600Z), 1,368 files,
  7.56 B rows, 365.65 GB (182.8 GB/cycle vs 194.3 baseline). Rows cell-by-cell along a Hilbert curve,
  member innermost (`WXTCO_TABLE_SORT=hilbert`); Iceberg SortOrder unsorted, property `wxtco.sort=hilbert`.
  ETL 1681 / 1677 s per cycle on c7i.16xlarge with 5 lead groups (baseline 1495 s, +12%). Full results and
  reading: `docs/findings/experiments/hilbert/README.md`. Not read by `wxtco tco`.
- **Q2 from S3: Hilbert 476-503 s vs baseline 2209 s (4.5x). Warm 14 s is DuckDB's external file cache**
  (`enable_external_file_cache`, default true since DuckDB 1.3, bounded by `memory_limit` = 80% RAM).
  Every earlier warm table number (Q1 3.6 s, Q3 85 s) is likewise cache-assisted where the touched bytes fit.
  Icechunk Q2 from S3: 16-18 s, with or without its chunk cache.
- **Per-query metadata walk costs ~3.3 s on the 19,152-file table vs the 1,368-file table** (Q3 same-day
  control 85 vs 31 s over 16 samples; Q1 3.6 vs 0.5 s). DuckDB re-plans every query against the full file
  list even when the manifests are cached. Q1 gets nothing from the Hilbert layout itself.
- **Grid discovery fixed** (daa18d7): `TableBackend._grid` scanned `distinct lat`/`lon` over a whole cycle
  (3.8 B rows) on the first call; now one `(init_time, lead_hours=0, member=0)` slice. Study-table Q1 cold
  258 s -> 121 s (remaining = REST manifest fetch + planning over 19k files); scratch table 174 s -> 10 s.
  Earlier cold numbers in `docs/findings/bench/table-q*.csv` include the old scan.
- **Icechunk chunk cache** (b178d62): `CachingConfig.num_bytes_chunks` defaults to 0; readers now set it
  to half of RAM (`WXTCO_CHUNK_CACHE_BYTES` overrides). A caching-only `RepositoryConfig` merges over the
  stored config (verified locally: inline threshold and manifest preload survive; on the prod repo manifest
  splitting and the virtual container survive). Measured effect on this repo: none (Q2 16.1-17.5 s cached vs
  16.5-17.8 s at 0). Icechunk caches all chunk types (Ryan); the null result means these queries are decode-
  and compute-bound at this size, not S3-bound (corrected 2026-09-21; the earlier bypass hypothesis was wrong;
  blocked by the session proxy).
- **Scratch tables and progress**: `_progress/<method>/` was keyed on `table` regardless of `WXTCO_TABLE_ID`;
  now `table_<name>` for scratch tables. `ensure_table` refuses a writer whose `WXTCO_TABLE_SORT` differs
  from the table's `wxtco.sort` property (an Opus review finding; the experiment table was created fresh).
- **Instances**: ETL c7i.16xlarge `i-0fada7d5dbe3fa35d` 1.01 h; bench m7i.4xlarge `i-0a866bc86d061fb81`
  2.53 h; both terminated 17:20 UTC. About $5 on-demand. Scratch table `mogreps.surface_hilbert` (366 GB,
  ~$8.4/month) left in place for Ryan to drop or keep.
- **NetCDF-direct methods benchmarked (19:26-19:30 UTC, m7i.4xlarge `i-0fa8be0587c3f8b05`, 1.11 h)**:
  Download (obstore) Q1 10.7-11.2 s, Q2 54 s, Q3 56-58 s; Download (CRT transfer manager) Q1 11.0-11.4 s,
  Q2 55 s, Q3 50-51 s; FUSE (Mountpoint, bucket root, ro, no cache, metadata-ttl indefinite) Q1 191-208 s,
  Q2 996-1037 s, Q3 207-219 s. Download moves 5.99 / 22.3 / 2.26 GB per Q1 / Q2 / Q3 at 410-545 MB/s
  (bandwidth-bound); FUSE is ~1.1 s per file regardless of bytes. Monthly TCO with the copy: download
  887, download_crt 904, fuse 13,197 USD vs virtual 262 and table 783. Write-up:
  `docs/findings/netcdf-direct-methods.md`. Source files average ~35 MB, not the 16 MB assumed earlier
  (that figure included the level files).
- **Mountpoint on AL2023 worked first time** from `user_data.sh` (`WXTCO_FUSE=1`): `dnf install mount-s3`,
  fstab entry, `mount -a`; FUSE_READY written before READY. Mount options observed:
  `ro,nosuid,nodev,noatime,default_permissions`.
- **Native Zarr ingest probes (20:19-20:23 UTC, Arraylake repos `mogreps-g-native-ts`, `mogreps-g-native-dl`,
  code cbe4ca8)**: forked Icechunk sessions pickle into spawn workers with Arraylake storage; one commit per
  cycle (plus a one-time layout commit). `ts` 2 variables, 2 workers, file-workers 8: 116 s wall, 30.3 GB
  uncompressed written, per unit ~85 s read+decode vs ~20 s pcodec encode+write (read-bound). `dl` full cycle
  (171 leads x 4 vars), 8 workers: 105 s wall, 60.5 GB uncompressed. Peak memory: ts 31 GB for 2 workers
  (~15 GB per (18,171,960,1280) block), dl 9 GB. 3-cycle trials follow with ts workers 5 / file-workers 12 and
  dl workers 12.
- **Native ts trial, cycle 1 (20:28-20:50 UTC, c7i.16xlarge, 5 workers, file-workers 12)**: 1,307 s wall for 79 of 81
  variables (2 done by the probe; ~1,340 s per full cycle), 1,195 GB uncompressed written, per variable ~57 s
  read+decode vs ~22 s pcodec encode+write (read-bound), peak RSS 92 GB. `dl` trial: 78-80 s per cycle with 12
  workers, 60.5 GB uncompressed, 10.7 GB stored per cycle (pcodec 5.65x, lossless), projected 300 GB for 28 cycles.
  Reviewer findings fixed in 7a7e606..e885fac: `--variables` rejected for dl (would blank other variables in the
  shard), commit retry with rebase on ConflictError, coverage logging (leads off the axis, missing files), zarr
  write concurrency 4 in ts workers (~21 GB per worker), ts chunk (1,18,57,16,16) / shard (1,18,171,320,320) so
  every write is a complete shard (171=3x57, 960=60x16, 1280=80x16). The trial ts repo still has the old
  (1,18,64,17,16) chunking and must be recreated before any full run.
- **Native ts trial complete (21:35 UTC)**: cycles 1,307 / 1,347 / 1,353 s on c7i.16xlarge (5 workers, file-workers 12;
  cycle 1 excludes the 2 probe variables, so ~1,340 s). Repo `mogreps-g-native-ts` (trial chunking 1,18,64,17,16):
  408.2 GB in 4,246 objects for 3 cycles = 136 GB per cycle from 1,225 GB uncompressed: **pcodec 9.0x lossless**
  (Parquet+zstd on the same values: 194 GB per cycle, 6.3x). Projected 3.8 TB for 28 cycles vs table 5.44 TB.
  `dl` trial (4 variables): 78 s and 10.7 GB per cycle, pcodec 5.65x. Both trial repos are superseded by the reviewed
  code (ts chunk 1,18,57,16,16; dl all 81 slugs, Q3-first, lexicographic sub-chunk order) and must be recreated
  before a full run. `docs/findings/etl/native_{ts,dl}.csv` and the `native_*` rows in `storage.csv` hold the trial
  numbers; the `native_dl` rows are for 4 variables and will be replaced. Instances: ts 1.60 h, dl 0.51 h (~$6).
- **Full Hilbert table complete (2026-09-21 00:57 UTC)**: `mogreps.surface_hilbert` verified 4,788 partitions x 4 files,
  105,902,899,200 rows, **5,122.4 GB** (member-sorted table 5,440.6 GB: -5.8%), 4,788 snapshots. Two c7i.16xlarge
  nodes (a: 12 cycles, 6.18 h; b: 14 cycles, ~7.1 h) with 5 lead groups each = 10 concurrent committers. Per-cycle
  wall 1,700-1,917 s vs 1,681/1,677 s isolated: ~3% from commit-conflict retries (~1.2 retries per lead after the
  limits were raised to 30) and 5-10% from metadata growth (per-lead time in-process rose 45.7 -> 50.5 s as the
  snapshot list reached 4,788 entries; node b alone at the end still ran ~1,890 s). Failures: 2 leads lost to 409
  conflicts before the retry fix, 1 to an Arraylake REST 502; the idempotent sweep re-ingested all 3. ETL CSV keeps
  the isolated 1,681/1,677 s as the clean per-cycle numbers (`docs/findings/etl/table_hilbert.csv`). Snapshot expiry
  is the remedy for the metadata growth and should precede the benchmark in a production flow; not applied here so
  the benchmark sees the table as ingested.
- **Cache-honest table benchmarks (02:00-02:43 UTC, m7i.4xlarge `i-082343936edd7e9fc`, DuckDB file cache off)**:
  Hilbert full table Q1 74.8 cold / 22.0-23.1 s, Q2 584/567/544 s, Q3 603/594/585 s; member-sorted table Q1
  176.6 cold / 116-122 s, Q3 595/559/544 s (Q2 2,209 s from 2026-09-19 was effectively uncached). Every earlier
  warm table figure (Q1 3.6 s, Q3 85 s) was DuckDB's external file cache. Recorded as methods `table_hilbert`
  and `table_nocache`; `table` keeps the cache-assisted runs. Write-up `docs/findings/hilbert-full-table.md`.
  Bench node terminated (1.90 h). No EC2 instances running.
- **Native trial-repo benchmarks (11:43-11:57 UTC, m7i.4xlarge `i-05a7e19148e201619`, chunk cache 0)**: native_ts Q1
  0.27 cold / 0.07-0.09 s, Q2 3.7/2.3/1.8 s, Q3 185/182/176 s; native_dl (4 vars) Q1 0.77 / 0.35-0.45 s, Q2
  6.7/6.5/6.3 s, Q3 15.4/13.0/12.7 s. First run of Q1/dl failed on the rewritten reader against pre-rewrite repos
  (`valid_time` stored as a data variable, not a coordinate); fixed in 40f6d06 (`promote_valid_time`). Write-up
  `docs/findings/native-trial-benchmarks.md`. Node terminated (0.27 h). No EC2 instances running.
- **Incident 2026-09-22 16:38 UTC (full native ingest, first attempt)**: after the trial repos were deleted and
  recreated, both nodes skipped all 81 units of the first three cycles because the trial's `_progress/native_{ts,dl}/`
  manifests survived the repo deletion, and the fourth cycle started into init slot 0. Nodes terminated (1.05 h each).
  Root cause fixed in code: a progress mark is honoured only when the cycle already has an init slot in the repo;
  a cycle that is new to the repo clears its stale manifest first (`ingest_cycle_native`, regression test). The
  session's auto-mode classifier refused both the recursive S3 delete and the manifest overwrite from the controller,
  so the code guard is the fix, not manual cleanup. Repos recreated again (immediate delete: the 7-day ghost holds the
  name) and the full run restarted.
- **Full native ingest, second attempt (2026-09-22 17:42 to 2026-09-23 05:34 UTC, c7i.16xlarge `i-0d4ca63f2677223ab` ts,
  `i-0dd13ebcb1a14ccc9` dl)**: 28 cycles each, 0 units skipped, 0 leads dropped. ts 1,512 s/cycle mean (read 4,422 /
  write 1,466 / commit 6.9 worker-s medians), repo `8lmcybts_mogreps_g_native_ts/` 3,646,277,722,044 B = 3,646 GB (9.4x
  vs 34,311 GB uncompressed); dl 1,361 s/cycle (read 4,430 / write 3,507 / commit 1.2), repo
  `vloykpul_mogreps_g_native_dl/` 3,827,651,446,774 B = 3,828 GB (9.0x). Nodes 11.9 h and 10.7 h (~$64). Source:
  `aws s3 cp s3://em-tco-mogreps/_logs/native-full-{ts,dl}.log -`; rows in `docs/findings/etl/native_{ts,dl}.csv`
  and `storage.csv` (trial rows replaced).
- **Full native repo benchmarks (05:35-05:46 UTC, m7i.4xlarge `i-089d502ecf304f3cd`, `WXTCO_CHUNK_CACHE_BYTES=0`,
  cycle 2026/09/15)**: native_ts Q1 0.45 cold / 0.08-0.17 s, Q2 2.8/2.4/2.0 s, Q3 172/170/167 s; native_dl Q1 0.90 /
  0.35-0.41 s, Q2 7.0/6.5/6.4 s, Q3 17.9/14.6/14.0 s. Matches the trial repos within noise; the 81-variable dl shard
  reads Q3 as fast as the 4-variable trial shard. `wxtco tco --storage-variant with_netcdf`: native_ts $386, native_dl
  $393, virtual $262; break-even vs virtual ~2.9x (ts), ~3.7x (dl), ~10x for both layouts in one repo. Doc:
  `docs/findings/native-trial-benchmarks.md` (full-repo section). Node 0.27 h, terminated. No EC2 instances running.
- **Q3 bulk fetch vs synchronous loop (2026-09-23 13:53-14:16 UTC, m7i.4xlarge `i-04df7d02f6fd4c237`, chunk cache 0,
  `net_rx_bytes` from /proc/net/dev per run)**: native_dl loop 17-22 s / 1.42 GB wire, bulk 5.8 s / 1.03 GB (980 MB/s
  decoded, 178 MB/s wire); native_ts loop 181 s / 140.6 GB wire (775 MB/s = the 6.25 Gbps baseline), bulk 60 s /
  22.2 GB. First bulk attempt with list indexers on init, lead and variable: dl OOM-killed (64 GB RSS, dmesg on the
  node), ts 157 s at 141 MB/s; cause is zarr's `OrthogonalIndexer` ix_ fallback for >1 array-indexed axis on sharded
  arrays. Fixed in 2b88a56 (slices for consecutive positions). Doc: `docs/findings/q3-bulk-fetch.md`; CSVs in
  `docs/findings/experiments/q3-bulk/`. Node 0.45 h, terminated. No EC2 instances running.
- **Q3 async patterns (2026-09-23 15:01-15:29 UTC, m7i.4xlarge `i-0d25aed94225de927`, chunk cache 0, wire bytes per run)**:
  native_dl async iter 10.8-13.0 s / 1.43 GB, gather 7.4-7.7 s / 1.42 GB; native_ts async iter 189 s / 140 GB (740 MB/s),
  gather OOM-killed (dmesg: 64 GB anon RSS); virtual loop 39-41 s, bulk 35 s, async 38 s, gather 68-72 s, all 2.4 GB wire
  (~60 MB/s, request-rate bound on 40 KB HDF5 chunks). Async variants retired per Ryan; code in f7cd308, CSVs in
  `docs/findings/experiments/q3-async/`, write-up in `docs/findings/q3-bulk-fetch.md`. Decision: Q3 becomes a batched
  stream (`q3_stream`, batch-level shuffle, one `batch_fields` per batch) with a batch-size sweep; table gets a
  one-statement `batch_fields`.
- **Q3 stream batch-size sweep (2026-09-23 15:35-18:26 UTC, m7i.4xlarge `i-0d25aed94225de927`, 3.45 h, chunk/file caches
  off, wire bytes per run)**: 32 samples, B=1..32. native_dl 23.8/16.5/13.4/12.4/12.0/12.2 s, 2.05 GB wire at every B;
  native_ts 372/243/151/112/121 s, wire 282->44 GB, B=32 OOM-killed at zarr concurrency 256 and 64 (dmesg), 105 s at 32;
  virtual 89/76/72/70/70/69 s, 4.8 GB; table_hilbert 1,502/823/527/334/254 s, wire 11.8->4.2 GB, B=32 OOM-killed (707 M-row
  sort). CPU busy over a B=16 run: dl 16-17%, ts 29-33% of 16 vCPU. Doc: `docs/findings/q3-stream-sweep.md`. Node
  terminated; no EC2 instances running.
- **Anemoi-style unsharded experiment (2026-09-24 23:42-23:46 UTC, m7i.4xlarge `i-089702ec2b913cd81`, 0.2 h)**: repo
  `wxtco/mogreps-g-native-anemoi`, 4 cycles x 8 leads x 4 vars, chunk (1,1,4,18,960,1280), no shards, pcodec 8,
  bit-identical to dl. Q3 stream B=8, 32 samples: anemoi 9.02-9.13 s (1.25 GB/s decoded, 227 MB/s wire), dl
  12.03-12.23 s (0.93 GB/s, 168 MB/s), both 2.05 GB wire, CPU 16-17%. Doc: `q3-stream-sweep.md` last section; CSVs and
  builder in `docs/findings/experiments/anemoi/`. First build failed on append (`variable` coord `<U33` vs StringDType);
  ~0.6 GB of orphaned chunks remain in the repo. Node terminated.
- **Anemoi layout with Blosc LZ4 (2026-09-24 23:50-00:05 UTC, m7i.4xlarge `i-0c6efc2c5a6516789`, 0.25 h)**: repo
  `wxtco/mogreps-g-native-anemoi-lz4` (lz4, shuffle, clevel 5; 4,026,627,336 B stored, ratio 2.81x). Q3 B=8, 32 samples:
  lz4 9.32-9.59 s (4.11 GB wire, 435 MB/s), pcodec anemoi 8.90-9.25 s (2.05 GB), dl 11.74-12.15 s. In-memory decode of
  one 354 MB chunk: pcodec 0.40 s single / 5.71 GB/s on 8 threads (6.5x), lz4 0.20 s / 9.87 GB/s (5.5x). Conclusion:
  neither codec nor network limits Q3; ~1.5 s per batch is codec-independent read-path overhead. Doc:
  `q3-stream-sweep.md` last section; CSVs and decode test in `docs/findings/experiments/anemoi-lz4/`. Node terminated.
- **Canonical pass (2026-09-30 01:37-03:30 UTC, m7i.4xlarge `i-01c6779cb30bcc381`, FUSE mount, commit 22a9f29, ~2 h)**:
  bucket idle before start; 53 rows, all with git_sha and `net_rx_bytes`. Medians Q1/Q2/Q3-per-16: native_ts 0.11/2.3/52 s,
  native_dl 0.40/6.6/6.0, virtual 0.99/18.0/40.7, download 11.9/60/38, table_hilbert 27.6/694/184, fuse 268/1,250/249.
  Hilbert Q1/Q2 20-23% and FUSE 23-38% slower than the earlier runs; no code or DuckDB change, bytes not larger.
  `wxtco tco` with the copy: virtual $254, native_ts $384, native_dl $394, download $950, table_hilbert $2,302, fuse $18,163.
  Doc: `docs/findings/canonical-results.md`. Node terminated.
- **S3 request-count pass (2026-09-30 15:03-16:20 UTC, m7i.4xlarge `i-080fa34bf034b9833`, commit 15420e6)**: minute-aligned phases,
  1-run and N-run per query, idle baseline removed; CloudWatch metrics lag ~5 min, so pull after that. Per Q1: native_ts 3 GET,
  native_dl 298, download 169 + 12 LIST, virtual 2,478, table_hilbert 5,554 (+7,354 per table open), fuse 16,650 + 189 LIST.
  `docs/findings/requests.csv`; `wxtco tco` now adds `request_usd`. With the copy: native_ts $385, native_dl $430, virtual $555,
  download $989, table_hilbert $2,970, fuse $20,468; break-even vs virtual 0.53x (ts), 0.59x (dl), 1.1x (both). Raw data and
  scripts: `docs/findings/experiments/requests/`. Node terminated.
