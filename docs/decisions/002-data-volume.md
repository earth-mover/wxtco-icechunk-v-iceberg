# 002 How much data to ingest

**Status:** DECIDED 2026-09-18. Option A confirmed by Ryan.

## Context

Per cycle, MOGREPS-G surface-only (81 diagnostics, no level fields) is 11,521 files and 232 GB
gzip-compressed NetCDF. Each file: 18 members x 960 x 1280 float32 = 88 MB uncompressed,
~35 MB on disk, HDF5 chunks (1, 128, 128), zlib level 1, `least_significant_digit: 2`.
Uncompressed data per cycle: 11,521 x 88 MB = ~1.0 TB.

Lead schedules differ by diagnostic (171 / 170 / 169 / 132 / 83 / 38 files). Joe's code
splits them into six groups. A flat table copes with this trivially; the tensor layouts need
one group per schedule or NaN-fill.

## Options

| scope | cycles | files | NetCDF size | uncompressed |
|---|---|---|---|---|
| A. 7 days, 81 surface diagnostics | 28 | 323k | 6.5 TB | 28 TB |
| B. 7 days, `surface` group only (50 instantaneous diagnostics, 171 leads) | 28 | 239k | ~4.5 TB | 21 TB |
| C. 14 days, 81 surface diagnostics | 56 | 645k | 13 TB | 56 TB |

Flat table row count for A: 28 x 171 x 18 x 1,228,800 grid cells = **1.06e11 rows**
(fewer for diagnostics with shorter lead schedules; NULLs otherwise).

## Recommendation

Option A for the full study, matching the plan. Develop and validate every pipeline on
**one cycle** first; the harness must make "1 cycle" and "28 cycles" the same command.

## Consequences

Static copy is ~6.5 TB (see 003). Table ETL is a ~1e11-row job; this is the point of the
study, but plan compute for it.

## Chosen cycles (session 003, 2026-09-19)

`wxtco copy pick-window --write` selected 28 cycles, 2026/09/12/T0000Z through 2026/09/18/T1800Z,
pinned in `study_cycles.txt`. The newest cycle had 14,020 objects (complete) at selection time; the
oldest is 24 days newer than the oldest cycle then in the source bucket.
