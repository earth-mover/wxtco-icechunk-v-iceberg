# 016 Canonical benchmark set for the report

**Status:** DECIDED 2026-09-30 (Ryan).

## Decision

- **Six methods are reported**: `virtual`, `native_ts`, `native_dl`, `table_hilbert` (Iceberg,
  Hilbert row order), `download` (obstore) and `fuse`. FUSE stays as the reference point for
  reading NetCDF in place.
- **Dropped from the headline results** and kept as experiments: the cache-assisted Iceberg table
  (DuckDB file cache, not an S3 measurement), `download_crt` (within 2-4% of obstore) and the
  member-sorted Iceberg table. The report must state why Hilbert sorting matters and the lesson
  learned: with member-innermost row order a point query touches every row group of a partition
  (Q1 119 s), Hilbert order lets Parquet statistics prune (Q1 23 s), and the first "cheap" table
  numbers came from DuckDB's local file cache, not from S3.
- **Queries**: Q1 and Q2 as in decision 010; Q3 is the batched stream (`q3_stream`) at B=8 over
  32 samples, priced per 16-sample job (`wxtco.tco.Q3_BATCH`, `Q3_SAMPLES`).
- **One canonical run**: one m7i.4xlarge, one commit (recorded in every row), idle bucket checked
  in CloudWatch first, Icechunk chunk cache and DuckDB file cache off, `net_rx_bytes` per run.
  Q1 x5, Q2 x3, Q3 x2 for every method except FUSE, which runs once per query: it is orders of
  magnitude slower and the extra trials add nothing.
- **No ETL reruns.** ETL rows stay as measured (virtual and Hilbert from 1-2 clean cycles).
- The temporary Anemoi experiment repos stay until publication.

## Consequences

`docs/findings/bench/` holds only the canonical CSVs; everything earlier is in
`docs/findings/experiments/pre-canonical-bench/`, and the dropped methods' storage and ETL rows are
in `docs/findings/experiments/dropped-methods/`. The NetCDF backends read a Q3 batch with one LIST
per cycle and one parallel transfer (`NetcdfFilesBackend.batch_fields`).
