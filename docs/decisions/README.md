# Decision register

One row per decision. Status: `PROPOSED` (coordinator recommendation, not yet approved),
`NEEDS RYAN` (human must choose), `DECIDED`, `SUPERSEDED`. Each decision has a file
`NNN-slug.md` with context, options, recommendation, and consequences.

| # | Decision | Status | Recommendation |
|---|----------|--------|----------------|
| 001 | Which dataset: MOGREPS-G (global) vs MOGREPS-UK | DECIDED | MOGREPS-G, bucket `met-office-global-ensemble-model-data` |
| 002 | How much data to ingest | DECIDED | 7 days = 28 cycles, 81 surface diagnostics, all leads (~6.5 TB, ~323k files) |
| 003 | Static copy of source NetCDFs | PROPOSED | Yes. Copy to our S3 bucket in us-east-1; all three methods build from the copy |
| 004 | Table layout for Iceberg/Parquet | PROPOSED | One wide table, one row per (init, lead, member, lat, lon), 81 value columns, partitioned by init and lead |
| 005 | Table query engine | PROPOSED | DuckDB primary (reproducible, single vendor). Athena secondary (AWS-native, per-TB pricing). Snowflake stretch |
| 006 | Tensor query engine | PROPOSED | Xarray + Zarr/Icechunk primary. Dask only for the regional-stats query. Zax SQL stretch |
| 007 | Ingestion compute and orchestration | PROPOSED | One EC2 instance in us-east-1 per method, sequential cycles, driven by a CLI in this repo. AWS Batch if fan-out is needed |
| 008 | Arraylake org for the study | DECIDED | Standalone org + project API tokens (Ryan, 2026-09-18). Name TBD at creation; needs `iceberg` flag |
| 009 | AWS account | DECIDED | Earthmover Sandbox `<ACCOUNT_ID>`, profile `PowerUserAccess-<ACCOUNT_ID>` |
| 010 | Query frequency model for TCO | DECIDED (baseline, revisit after data lands) | Point timeseries 10k/day, regional ensemble stats 4/cycle, ML dataloader 1 epoch/day, ETL 4 cycles/day |
| 011 | Native Icechunk chunking and codec | DEFERRED (stage B) | Decide from stage A results (`table` + `virtual` benchmarks) plus a one-variable chunk sweep |
| 012 | Project name and package name | PROPOSED | Repo as-is; Python package `wxtco` |
| 013 | Arraylake access to the study bucket | DECIDED | One IAM role `wxtco-arraylake` (customer-managed role delegation); bucket configs `wxtco-storage` (`arraylake/`, default) and `wxtco-netcdf` (`netcdf/`, VCAP private) |
| 014 | Controller credentials for the cloud session | DECIDED | Dedicated IAM user with a narrow control-plane policy; deleted at project end |
| 015 | NetCDF-direct methods (Download, FUSE) | DECIDED | No-ETL baselines: per-query parallel download (obstore vs CRT transfer manager) and Mountpoint read-only mount; Ryan 2026-09-20 |
| 016 | Canonical benchmark set for the report | DECIDED | Six methods (virtual, native_ts, native_dl, table_hilbert, download, fuse); Q3 = B=8 stream; one clean run; Ryan 2026-09-30 |

Details for each are in the numbered files. All blocking decisions resolved 2026-09-18. PROPOSED items stand unless edited.
