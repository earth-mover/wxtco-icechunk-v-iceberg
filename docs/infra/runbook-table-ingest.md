# Runbook: table ingest, one cycle

## Where to run

The data work reads `s3://em-tco-mogreps/netcdf/` and writes the Iceberg table. Run it on:

- **EC2**, instance role `wxtco-ec2` (Plan 04 Task 8 launch scripts). This is the normal path.
- **Ryan's laptop**, `export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>`, for a small first run.

Do **not** run the ingest in the Claude Code cloud session. That session holds only the
`wxtco-controller` key, which is denied `s3:ListBucket` on `netcdf/` (see
`docs/infra/access-checklist.md`). The session launches and watches jobs; it does not move data.

Prereqs on the machine that runs it: `.env` with `ARRAYLAKE_TOKEN` and `ARRAYLAKE_ORG`
(EC2 reads the token from Secrets Manager `wxtco/arraylake-token`), `study_cycles.txt`, and a
verified copy. Commit before an EC2 launch: `launch.sh` ships `git archive HEAD`.

## 1. Verify the copy

    CYCLE=$(head -1 study_cycles.txt)
    uv run wxtco copy verify --cycle "$CYCLE"

Exit 1 means files are missing or the source is empty. Fix the copy before going on.

## 2. Small ingest: 3 slugs, 2 leads

    WXTCO_SLUGS=temperature_at_screen_level,wind_speed_at_10m,pressure_at_mean_sea_level \
      uv run wxtco ingest table --cycle "$CYCLE" --leads 0,60 --workers 16

`WXTCO_SLUGS` overrides the study slug list, so the table is created with 3 data columns only.

## 3. Verify with DuckDB

    uv run python -c "
    import os
    from wxtco.duck import connect_iceberg
    c = connect_iceberg()
    c.execute(f\"CREATE SECRET al (TYPE ICEBERG, TOKEN '{os.environ['ARRAYLAKE_TOKEN']}', ENDPOINT 'https://api.earthmover.io/iceberg')\")
    c.execute(f\"ATTACH '{os.environ['ARRAYLAKE_ORG']}' AS wh (TYPE iceberg, SECRET al)\")
    print(c.execute('select lead_hours, count(*) from wh.mogreps.surface group by 1 order by 1').fetchall())"

Expect one row per lead, each with the full member x lat x lon count. `connect_iceberg()` loads
the extension from the installed `duckdb-extension-iceberg` and `duckdb-extension-avro` wheels,
because DuckDB's own extension downloader cannot pass the cloud session proxy. On EC2 or a
laptop `INSTALL iceberg; LOAD iceberg;` also works; `connect_iceberg()` falls back to it.

## 4. Drop the 3-column table

The smoke-test table has the wrong schema for the study. Drop it before the full run:

    uv run python -c "
    from wxtco.catalog import arraylake_catalog
    from wxtco.config import Settings
    arraylake_catalog(Settings.from_env()).drop_table('mogreps.surface')"

Also clear the progress manifest, or the full run skips leads 0 and 60 as already done:

    uv run python -c "
    from wxtco.config import Settings, store_for
    from wxtco.progress import Progress
    s = Settings.from_env()
    Progress(store_for(s.copy_url, region=s.copy_region), 'table', '$CYCLE').clear()"

## 5. Full cycle, 81 slugs, on EC2

Same command without `WXTCO_SLUGS` and without `--leads`:

    uv run wxtco ingest table --cycle "$CYCLE" --workers 16

Ingest is idempotent per lead, so a rerun of the same command resumes after any interruption.
Raise `--workers` to match the instance vCPU count.

## 6. Record the result

Append wall clock (the `Ns` in the command's final line) and the instance type to
`docs/findings/etl/table.csv`. Create the file with this header if it is absent:

    cycle,slugs,leads,instance_type,workers,rows,seconds
