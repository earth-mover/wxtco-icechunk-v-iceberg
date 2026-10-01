# 004 Table layout for Iceberg/Parquet

**Status:** PROPOSED

## Options

1. **Wide**: one row per (init_time, lead, member, lat, lon); one column per diagnostic (81).
   Natural for point queries and ML row batches. NULLs where a diagnostic lacks that lead.
2. **Long**: one row per (init_time, lead, member, lat, lon, variable, value). 81x more rows,
   no NULLs, worst for wide reads.
3. **One table per diagnostic**: 81 narrow tables. Joins for multi-variable queries.

## Recommendation

Wide table, partitioned by `init_time` and `lead` (Iceberg hidden partitioning), sorted within
partition by `member, lat, lon`. This is the layout a competent data engineer would choose and
gives tables their best shot; the study should not straw-man the table side.

Parquet: float32 columns, zstd, row groups sized ~1M rows. Record the exact settings in
`docs/findings/`.

## Consequences

ETL unit = one (cycle, lead): read 81 files, stack to rows, write one Parquet file, append
to Iceberg. 28 x 171 = 4,788 units; embarrassingly parallel.
