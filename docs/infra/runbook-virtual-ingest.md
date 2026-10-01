# Runbook: virtual ingest, one cycle at a time

## Where to run

The pipeline parses every NetCDF file under `s3://em-tco-mogreps/netcdf/` and writes chunk
manifests to the Arraylake repo `<org>/mogreps-g-virtual`. No bytes are copied. Run it on:

- **EC2**, instance role `wxtco-ec2` (Plan 04 Task 8 launch scripts). This is the normal path;
  parsing is CPU bound, so use a high-vCPU instance and set `--workers` to the vCPU count.
- **Ryan's laptop**, `export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>`, for the plan-only run
  and a first small cycle.

Do **not** run this in the Claude Code cloud session. That session holds only the
`wxtco-controller` key, which is denied `s3:ListBucket` on `netcdf/` (see
`docs/infra/access-checklist.md`). The session launches and watches jobs; it does not move data.

## Prereqs

- `.env` with `ARRAYLAKE_TOKEN` and `ARRAYLAKE_ORG` (EC2 reads the token from Secrets Manager
  `wxtco/arraylake-token`).
- `study_cycles.txt` present; the copy verified (`uv run wxtco copy verify --cycle "$CYCLE"`).
- Arraylake bucket configs `wxtco-storage` (org default, holds the repo) and `wxtco-netcdf`
  (virtual chunk source) both present. Created by `scripts/aws/setup_arraylake_storage.sh`;
  see `docs/decisions/013-copy-bucket-access.md`.
- A **virtual chunk access policy on `wxtco-netcdf`**, subprefix empty, private, added by an org
  admin in the web app (Ryan). The code no longer sets it: the API is admin-only and 404s/403s
  with the project token. Without the VCAP the repo cannot be opened at all.
- Commit before an EC2 launch: `launch.sh` ships `git archive HEAD`.

## 1. Plan only, no writes

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>
    CYCLE=$(head -1 study_cycles.txt)
    uv run python -c "import wxtco.ingest.mogreps_virtual as m, sys; sys.exit(m.run(m.parse_args(['--cycle','$CYCLE','--plan'])))"

This prints how the diagnostics group by lead schedule and coordinate hash. Expect `surface` with
50 diagnostics at 171 leads, plus the `surface_PT01H`, `surface_PT03H` and the three odd-lead
groups. A group split that does not match means the copy is incomplete.

## 2. Origin cycle first, full selection

The init axis is append-only from the origin cycle, and the origin is the **first line** of
`study_cycles.txt`. The first ingest must be that cycle, with no `--variables` and no `--leads`,
because it seeds the groups, coordinates and array shapes for every later cycle.

    uv run wxtco ingest virtual --cycle "$CYCLE" --workers 48

## 3. Remaining cycles, in order

    tail -n +2 study_cycles.txt | while read c; do uv run wxtco ingest virtual --cycle "$c" --workers 48; done

Rerunning is safe. The pipeline commits per variable and skips variables already
committed for that cycle, and `Progress` at `_progress/virtual/` records each finished cycle, so
a repeated command resumes rather than duplicates. Use `--allow-incomplete` only if the copy is
knowingly a subset and the completeness gate rejects the cycle.

Expect CPU-bound parsing at roughly 1 s per file per core in-region.

## 4. Verify

Open the repo read-only and check the group dims and the `status` arrays for the slot just
written. Status values are `0` valid, `100` not yet populated (the fill value), `101` source
unavailable. A fully ingested cycle reads `0` everywhere.

    uv run python -c "
    import os, xarray as xr
    from arraylake import Client
    cycle = '$CYCLE'; p = cycle.split('/')
    ts = f'{p[0]}-{p[1]}-{p[2]}T{p[3][1:3]}'
    store = Client().get_repo(f\"{os.environ['ARRAYLAKE_ORG']}/mogreps-g-virtual\").readonly_session('main').store
    ds = xr.open_zarr(store, group='surface', zarr_format=3, consolidated=False)
    print(ds.sizes)
    st = xr.open_zarr(store, group='surface/status', zarr_format=3, consolidated=False)
    bad = {v: int(st[v].sel(forecast_reference_time=ts).max()) for v in st.data_vars}
    print('worst status:', max(bad.values()), sorted(k for k, s in bad.items() if s != 0)[:5])"

Expect `forecast_period=171`, `realization=18`, `latitude=960`, `longitude=1280`, one
`forecast_reference_time` slot per cycle ingested, and `worst status: 0`. A `100` means some
variable in that slot was never written; rerun the cycle. Label lookup by cycle time works
without sorting the init axis.

## 5. Record the result

Append wall clock and the instance type to `docs/findings/etl/virtual.csv`. Create the file with
this header if it is absent:

    cycle,instance_type,workers,seconds,files
