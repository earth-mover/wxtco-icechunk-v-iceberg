# Weather Forecast Data TCO Study: Design

Date: 2026-09-18, revised same day. Status: draft for Ryan's review. Decisions referenced as D00N live in
`docs/decisions/`.

## 1. Goal

Measure total cost of ownership and query performance for one week of MOGREPS-G surface
forecasts (D001, D002) stored three ways:

| method | id | store |
|---|---|---|
| 1. Flattened table | `table` | Iceberg on Parquet, Arraylake Iceberg catalog, S3 us-east-1 |
| 2. Native tensor | `native` | Icechunk repo, rechunked and recompressed, Arraylake, S3 us-east-1 |
| 3. Virtual tensor | `virtual` | Icechunk repo of chunk references to the static NetCDF copy |

Outputs: a reproducible library (`wxtco`), a findings directory with every measurement and
the command that produced it, and a long-form blog post.

**Sequencing (Ryan, 2026-09-18):** build and benchmark `table` and `virtual` first
(stage A). Design the `native` chunking only after stage A query results exist (stage B).
The native layout is then an informed design, not a guess, and stage A results show what
the tensor model buys before any rechunking.

## 2. Non-goals

- Ingesting each forecast step as it is released. Whole cycles only.
- Level (3-D) diagnostics.
- Tuning either side beyond what a competent engineer would do by default plus one
  documented optimization pass. The study reports trade-offs, not a leaderboard.

## 3. Data

Source: `s3://met-office-global-ensemble-model-data/global-ensemble/`, eu-west-2, anonymous.
Study window: 28 consecutive cycles (7 days), chosen at copy time so all are complete and
at least 3 days from deletion. 81 surface diagnostics, all lead times.

Per cycle: 11,521 files, 232 GB compressed, ~1.0 TB uncompressed float32.
Study total: ~323k files, ~6.5 TB compressed, ~28 TB uncompressed.

Static copy (D003): `s3://em-tco-mogreps/netcdf/<YYYY>/<MM>/<DD>/<THHMMZ>/<original-key>` in
the Sandbox account (D009), us-east-1. Arraylake reaches the bucket through the delegation role
`wxtco-arraylake` (D013): repo metadata, chunks, and the Iceberg warehouse live under
`s3://em-tco-mogreps/arraylake/`. All three methods read only from the copy. The copy
is the "with originals" storage variant.

## 4. Storage layouts

### 4.1 Table (`table`)

One Iceberg table `mogreps.surface`, wide (D004). Columns:

- keys: `init_time timestamp`, `lead_hours int16`, `valid_time timestamp`, `member int8`,
  `lat float32`, `lon float32`
- values: one `float32` column per diagnostic (81), NULL where the diagnostic has no file
  for that lead.

Partition spec: `init_time` (identity), `lead_hours` (identity). Sort order within a data
file: `member, lat, lon`. Parquet: zstd, dictionary off for floats, row group ~1M rows.
Expected ~1.06e11 rows.

Engines: DuckDB with the Iceberg extension against the Arraylake REST catalog (D005).
Athena is a stretch because it needs a Glue catalog; if pursued, register the same
metadata location in Glue rather than re-writing data.

### 4.2 Virtual (`virtual`), stage A

One Icechunk repo `<org>/mogreps-g-virtual`. Groups follow lead schedule, as in Joe's code:
`surface` (50 diagnostics, 171 leads), `surface_PT01H`, `surface_PT03H`, `surface_170leads`,
`surface_169leads`, `surface_83leads`. Each variable has dims
`(init_time, lead, member, lat, lon)`. Chunk references point into the static copy via
VirtualiZarr's HDF parser, so chunks are the source HDF5 chunks `(1, 128, 128)`. Built
from Joe's `virtualize_met_office_mogreps_g.py` adapted to: our bucket, an append-only
(not ring) init axis, no Modal.

### 4.3 Native (`native`), stage B

One Icechunk repo `<org>/mogreps-g-native`, same groups and dims as `virtual`, data
copied and rechunked. **Chunk shape and codec are not decided in this spec** (D011 stays
DEFERRED). They are chosen in a short design note after stage A, using:

- Q1..Q3 timings and bytes-read for `table` and `virtual`, which show where the source
  HDF5 chunking hurts;
- the storage footprint of `table`, which sets the compression target;
- a chunk-shape sweep on one variable, one cycle (for example 3 shapes x 2 codecs),
  run as a cheap experiment before the full native ingest.

The native ingest code (rechunk + write region + commit per variable) is built in stage A
against the synthetic fixture with a placeholder chunk shape, so stage B only supplies
parameters. The `native` backend reuses `TensorBackend` unchanged.

## 5. Ingestion

Unit of work: one cycle. Backfill runs cycles sequentially (plan requirement). Within a
cycle, work is parallel across files or (cycle, lead) units.

Common front end: `wxtco source list-cycle <cycle>` returns the file inventory from the copy.

| method | per-cycle job | output |
|---|---|---|
| `table` (A) | for each of 171 leads: open the 81 files for that lead, build an Arrow table of 18 x 960 x 1280 = 22.1M rows, write one Parquet file, append to Iceberg in one commit per cycle | 171 data files per cycle |
| `virtual` (A) | Joe's pipeline: parse HDF5 chunk maps, concat leads, write region, one commit per variable | manifests only |
| `native` (B) | for each group and variable: read files for all leads, rechunk to the stage B shape, write region to the cycle's init slot, one commit per variable | chunks |

Compute (D007): one EC2 instance in us-east-1, same instance type for all three methods
(candidate: `c7i.16xlarge`, 64 vCPU, 128 GB; sized so the stage B rechunk also fits, to
keep ETL costs comparable across stages). Launched
by `scripts/ec2/launch.sh` with an IAM role that grants S3 access and reads the Arraylake
token from Secrets Manager. Jobs run under `nohup` with logs shipped to S3. Instance
hours are recorded per job from launch and terminate timestamps. AWS Batch only if a
cycle exceeds 3 hours wall clock on this instance.

Idempotency: every method records completion per (cycle, unit) in a manifest so a rerun
skips finished work. `table` commits per cycle; `native` and `virtual` commit per variable.

Control plane (Ryan, 2026-09-18): the study is driven from a Claude Code cloud session acting
as a low-privilege controller. It holds only an IAM user key that can launch, command, and stop
`project=wxtco` instances and read the `_code/`, `_logs/`, `_progress/` prefixes. Data access and
the Arraylake token live on the EC2 instance role and Secrets Manager. Code reaches EC2 as an S3
tarball of the committed tree, never via GitHub credentials. See `docs/infra/cloud-session.md`.

Cost tagging: every resource carries `project=wxtco` and `method=<id>` so Cost Explorer
splits ETL and query costs by method.

## 6. Queries

Each query is a function `q(backend, params) -> result` in `wxtco.queries`. Backends:
`TableBackend(duckdb)`, `TensorBackend(repo, group)` used for both `virtual` (stage A)
and `native` (stage B). Results must be numerically identical across backends (tested on
one cycle). Stage A benchmarks compare `table` vs `virtual`; stage B adds `native` to the
same tables without rerunning stage A.

| id | query | params | engine notes |
|---|---|---|---|
| Q1 | Point forecast timeseries for one (lat, lon), all members, one cycle; heating degree days per day from `temperature_at_screen_level` | point, cycle | table: predicate on lat/lon/init_time; tensor: `.sel` nearest |
| Q2 | Regional ensemble statistics: for a lat/lon box, per lead, ensemble mean and spread of `temperature_at_screen_level` and `wind_speed_at_10m`; CRPS against the ensemble mean of the next cycle at the same valid time as a proxy analysis | box, cycle | table: group by lead, member; tensor: xarray reductions, Dask if needed |
| Q3 | ML dataloader: iterate over (cycle, lead) in random order, load a fixed set of 10 global variables for all 18 members as one float32 array, measure samples per second and bytes per second into host memory | var list, batch size | table: DuckDB → Arrow → numpy; tensor: zarr batch reads |
| Q4 | ETL latency: wall clock from job start to commit for one cycle | cycle | measured by the ingestion harness |

Each query records: wall clock (p50, p95 over 5 runs, cold and warm), bytes read from S3
(from the storage client where available, else CloudWatch request metrics), and compute
cost (instance seconds x price). Query benchmarks run on a separate, fixed instance type
(candidate: `m7i.4xlarge`) so query cost is not tied to the ingest machine.

## 7. Cost model

Inputs, all recorded in `docs/findings/costs/`:

- storage GB-months per method, with and without the NetCDF copy, from `aws s3 ls
  --summarize` and Icechunk repo stats
- ETL: instance hours x price, S3 PUT/GET request counts x price, Arraylake fees if any
- query: per-query instance seconds and S3 requests, multiplied by the D010 workload
  (Q1 10k/day, Q2 4/day, Q3 1/day, Q4 4/day)

Output: monthly TCO per method at 1x and 10x workload, plus a sensitivity table over the
Q1 frequency, since point queries dominate request costs. A small `wxtco tco` command
reads the findings CSVs and emits the table.

## 8. Code layout

```
src/wxtco/
  config.py        # bucket names, org, repo names, instance types; from env + TOML
  source.py        # list cycles and files in the static copy; parse keys
  copy.py          # static copy from Met Office bucket (one-time)
  ingest/
    table.py       # NetCDF -> Arrow -> Parquet -> Iceberg
    native.py      # NetCDF -> rechunked Icechunk
    virtual.py     # NetCDF -> virtual Icechunk (adapted from Joe's script)
    progress.py    # per-(cycle, unit) completion manifest
  queries/
    base.py        # Backend protocol, timing + byte counting
    q1_point.py  q2_regional.py  q3_dataloader.py
  bench.py         # run queries N times, write CSV to docs/findings/
  tco.py           # cost model from findings CSVs
  cli.py           # wxtco copy | ingest | bench | tco | check-access
scripts/ec2/       # launch, run-job, terminate; IAM policy JSON
tests/             # unit tests on synthetic small arrays; one-cycle integration marked slow
```

## 9. Validation

- Unit tests with a synthetic 2-member, 8x8 grid NetCDF fixture exercise every ingest
  path and every query end to end locally (Icechunk local store, DuckDB on local Parquet).
- One-cycle integration run on EC2 for each method before the 28-cycle backfill; Q1..Q3
  results must match across backends to float32 tolerance.
- Every findings CSV includes git commit, instance type, region, and timestamp.

## 10. Risks

| risk | mitigation |
|---|---|
| Source cycles deleted before copy completes | copy first, from the newest complete week |
| 1e11-row table ETL is slow on one instance | per-lead units are independent; fall back to AWS Batch (D007) |
| DuckDB cannot read Arraylake REST catalog | test in week 1; fallback is PyIceberg scan planning + DuckDB over the file list |
| Native rechunk memory blowup | process one variable at a time; 171 x 88 MB = 15 GB per variable fits in 128 GB |
| Virtual reads slow due to 40 KB chunks | expected; it is a stage A finding and the input to the stage B chunk design |
| Stage A results do not clearly point to a chunking | run the one-variable chunk sweep anyway; pick the shape that wins Q3 (dataloader) since it moves the most bytes, and document the Q1 trade-off |
| Flux SQL on virtual chunks | reported fixed upstream; verify before making Zax SQL a deliverable |

## 11. Phasing

Stage A: `table` and `virtual`.

1. Infra: org, token, bucket, IAM, EC2 launch script, access check green.
2. Static copy of 28 cycles.
3. `table` and `virtual` ingest on synthetic fixture (TDD), then one real cycle each on EC2.
   `native` ingest code built against the fixture with a placeholder chunk shape.
4. Queries on synthetic fixture, then one real cycle; `table` vs `virtual` equality.
5. Backfill 28 cycles for `table` and `virtual`; record ETL costs.
6. Stage A benchmarks (Q1..Q4) and preliminary cost model. Write
   `docs/findings/stage-a-summary.md`.

Stage B: `native`, informed by stage A.

7. Chunk-shape sweep on one variable, one cycle. Write `docs/decisions/011-icechunk-chunking.md`
   with the chosen shape and codec (moves D011 to DECIDED).
8. One-cycle native ingest, three-way equality check, then 28-cycle backfill.
9. Full benchmarks and cost model with all three methods; findings.
10. Blog draft.
