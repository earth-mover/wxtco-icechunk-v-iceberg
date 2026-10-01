# Project status

Single source of truth for where the project is. Every session updates this file
before it ends. Newest entry first in the log. Keep the "Now" block current.

## Now

- **Phase:** 6 (document and synthesize); stage B complete.
- **CANONICAL RESULTS DONE (2026-09-30, decision 016), now with S3 request costs.** Six methods, one clean pass from commit
  22a9f29 plus a request-count pass: `docs/findings/canonical-results.md`, `bench/`, `requests.csv`. Monthly cost with the
  NetCDF copy: native_ts $385, native_dl $430, virtual $555 (of which $301 S3 GETs: ~2,500 per point query), download $989,
  table_hilbert $2,970, fuse $20,468. Native ts is cheapest from 0.53x the decision-010 query workload. No instances running.
- **NO INSTANCES RUNNING (2026-09-23 05:47 UTC).** Full 28-cycle native ingest finished on both nodes (second attempt,
  no skipped units, no dropped leads): `mogreps-g-native-ts` 3,646 GB, 1,512 s/cycle; `mogreps-g-native-dl` 3,828 GB,
  1,361 s/cycle. Both repos benchmarked from S3 (m7i.4xlarge, chunk cache 0): ts Q1 0.08-0.45 s, Q2 2.0-2.8 s,
  Q3 167-172 s; dl Q1 0.35-0.90 s, Q2 6.4-7.0 s, Q3 14-18 s. Cost with copy: native_ts $386/mo, native_dl $393,
  virtual $262; break-even vs virtual ~3x (one layout), ~10x (both). See `docs/findings/native-trial-benchmarks.md`.
  Stage B is complete; every method in `wxtco tco` now has full-data rows.
- **Q3 is a batched stream (2026-09-23):** `q3_stream` (batch-level shuffle, one `batch_fields` per batch; table reads a
  batch with one SQL statement). Sweep B=1..32 over 32 samples on one m7i.4xlarge: dl flat from B=8 (12 s, 0.95 GB/s decoded,
  16% CPU, 175 MB/s wire: a serial Python section, not pcodec or network); ts 3.5x better at B=8 (112 s) and OOM at B=32 unless
  `WXTCO_ZARR_CONCURRENCY<=32`; virtual flat at 70 s (request-rate bound); table_hilbert 5.9x better at B=16 (254 s), OOM at B=32.
  Async variants tried and retired. Unsharded Anemoi chunking (one 354 MB chunk per sample, temp repo `mogreps-g-native-anemoi`)
  is 1.34x faster than dl at B=8 (9.1 vs 12.1 s, same 2.05 GB wire) but still 17% CPU. Open for Ryan: which B the cost model
  prices (B=8 proposed); Blosc LZ4 is no faster (9.4 s, 2x bytes) and pcodec is not GIL-bound, so ~1.5 s per batch is
  codec-independent read-path overhead (py-spy next); delete the two temp anemoi repos when done.
  Docs: `docs/findings/q3-stream-sweep.md`, `q3-bulk-fetch.md`.
- **Ryan's native ingest decisions (2026-09-22) applied in c3fdc93:** Morton shard order (write-order helper deleted),
  dask for `to_zarr(compute=False)` templates and metadata-only appends, `GridSpec` dataclass. xarray issue record:
  `docs/findings/xarray-issue-subchunk-write-order.md`. Billing: every resource tagged `BillingCategory=Marketing`
  (`scripts/aws/tag_billing.sh` for existing S3/IAM/secret; Ryan runs it with PowerUser).
- **Datasets in Arraylake org `wxtco`:** Iceberg `mogreps.surface` (member sort, 5,441 GB) and `mogreps.surface_hilbert`
  (Hilbert sort, 5,122 GB), both 28 cycles, both kept; Icechunk `mogreps-g-virtual` (28 cycles, 18.6 GB); native
  Icechunk repos `mogreps-g-native-ts` (28 cycles, 3,646 GB) and `mogreps-g-native-dl` (28 cycles, 3,828 GB).
- **Findings (all pushed):** `docs/findings/stage-a-summary.md` (+ 2026-09-21 addendum), `hilbert-full-table.md`,
  `netcdf-direct-methods.md`, `native-trial-benchmarks.md`, `experiments/hilbert/README.md`, `notes.md`.
  Cost model rows exist for: virtual, table (cache-assisted), table_nocache, table_hilbert, download, download_crt,
  fuse, native_ts, native_dl (all full data). Key correction: every warm table number before 2026-09-21 was DuckDB's file cache.
- **BLOCKED ON RYAN:** nothing. Open for Ryan: submit the xarray issue
  (`docs/findings/xarray-issue-subchunk-write-order.md`); run `scripts/aws/tag_billing.sh` with PowerUser.
- **Housekeeping for Ryan:** purge orphaned Parquet under `arraylake/0bpeglkn_mogreps/` (~300 GB); optional snapshot
  expiry on both Iceberg tables (4,788 snapshots each) and GC of the virtual repo; delete the wxtco-controller key at
  project end.
- **Next session should:** write the final synthesis from `canonical-results.md` (include the Hilbert lesson) (blog draft) from the
  findings files, including a 10x-workload view and the request-latency diagnosis of the table method.

## Phase checklist (mirrors CLAUDE.md)

- [x] 1. Structure and harness for independent sessions and parallel subagents
- [x] 2. Clarify open questions, decide infrastructure (`docs/decisions/`)
- [x] 3. Detailed implementation plan (`docs/superpowers/plans/`, stage A plans 01-04)
- [x] 4. Verify infrastructure access, document usage (`docs/infra/`)
- [x] 5. Implement (`src/wxtco/`, `scripts/`): stage A code and production runs done
- [~] 6. Document and synthesize findings: stage A summary written; blog draft pending

## Log

### 2026-09-23 — session 005 continued (full native ingest and benchmarks, unattended)

- Full 28-cycle native ingest completed on both c7i.16xlarge nodes (ts 11.9 h, dl 10.7 h, ~$64), no skipped
  units. Repos: native_ts 3,646 GB (9.4x), native_dl 3,828 GB (9.0x). Both benchmarked from S3 on an m7i.4xlarge
  with the chunk cache off; results match the trial within noise. Cost rows and `wxtco tco` updated for every
  method with full data. Break-even vs virtual: ~3x the D010 query workload for one native layout, ~10x for both.
  All nodes terminated. Next: final synthesis.

### 2026-09-19 — session 003 continued (production run, mostly unattended)

- Study window pinned (28 cycles). Copy: 6.49 TB in ~16 min (27 concurrent), verified. Table ingest
  ~14.5 h on c7i.16xlarge after memory tuning (one lead per process); virtual ingest 28 cycles.
  Ryan asked for method isolation: table finished alone, clean single-cycle ETL timings on an idle node
  (virtual 384 s, table 1495 s), benches on a separate m7i.4xlarge. Ryan's read-path changes
  (no dask, zarr concurrency 256) cut virtual Q1 from ~25 s to ~1 s. Table backend switched to
  role-based S3 reads after vended-credential expiry broke Q2. Results in
  `docs/findings/stage-a-summary.md`; monthly TCO table 634 vs virtual 112 (own bytes).
- Fixes committed along the way: `--copy-props none`, `WXTCO_TABLE_ID`, `tco --storage-variant`.

### 2026-09-18 — session 003 (Fable, cloud controller, env `wxtco`)

- Diagnostics pass after the allowlist fix (PyPI, STS as `wxtco-controller`, aws CLI, earthmover API).
- Plan 01 Tasks 1-7 implemented via Opus subagents with spec and quality review per task; 7 feature
  commits, full suite green, ruff clean. Smoke-tested listing and window selection against the real
  source bucket. Details: [docs/sessions/2026-09-18-003.md](docs/sessions/2026-09-18-003.md)
- Ryan reviewed Plan 02 Task 4 (`to_dataframe` vs numpy); benchmarked, kept numpy with `np.meshgrid`.
- Plan 02 Tasks 1-7 implemented the same way: `wxtco ingest table` end to end on the fixture with a
  local sqlite catalog, DuckDB verification via extension wheels, runbook written. Ryan added
  `extensions.duckdb.org` to the allowlist (takes effect live; DuckDB still needs the wheels here).
- Plan 03 Tasks 1-4: the reference virtualization pipeline adapted (repo attached after Ryan added it to the
  GitHub App), parametrized for the static copy, driver + `wxtco ingest virtual` CLI, end-to-end fixture
  test into a local Icechunk repo, runbook. Review found upstream `get_repo` lacked the virtual chunk
  grant; fixed in the adapted copy.
- Plan 04 Tasks 1-9: DuckDB and xarray backends that agree on the fixture, Q1/Q2/Q3, bench runner, cost
  model, EC2 scripts, `wxtco bench|tco|jobs`. Reviews fixed: per-instance pricing in tco, user-data
  double base64, SSM HOME/set -e, silent empty Q2, lead_fields shape inference, bound Arraylake secret.

### 2026-09-18 — session 002 (Fable, cloud controller, env `wxtco`)

- Ran first-session diagnostics. `uv` 0.8.17 present, no aws cli, `api.earthmover.io` reachable,
  AWS env vars present. `uv sync` and all `*.amazonaws.com` endpoints denied by the environment
  egress policy (`host_not_allowed`). Stopped per protocol; Plan 01 not started (Tasks 8, 9 were
  already done before this session).
- Recorded results in `docs/infra/access-checklist.md`; updated network row in
  `docs/infra/cloud-session.md`.

### 2026-09-18 — session 001 (Fable, coordinator)

- Read `project_plan.md`. Probed source bucket, reference repo, local credentials.
- Built repo harness: this file, `docs/` tree, decisions register, infra checklist,
  session protocol, subagent dispatch template.
- Wrote recommendations for every open question in `project_plan.md`.
- Ryan resolved decisions 001, 002, 008, 009, 010; approved spec (two-stage: table+virtual first, native after).
- Wrote stage A implementation plans 01-04 (foundations, table ingest, virtual ingest, queries/bench/TCO/EC2).
- Cloud-session control-plane design approved: low-privilege controller key, EC2 does data work, code via S3 tarball. Plans 01/04 and spec updated. Repo pushed to GitHub (private).
- Details: [docs/sessions/2026-09-18-001.md](docs/sessions/2026-09-18-001.md)
