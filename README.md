# Weather forecast data: Icechunk vs. Iceberg

Code, measurements, and cost model behind the Earthmover blog post
[Weather Forecast Data: Icechunk vs. Iceberg Head-to-Head](https://earthmover.io/blog/weather-forecast-data-icechunk-vs-iceberg).

One week of UK Met Office MOGREPS-G ensemble forecasts (28 cycles, 81 surface variables, 6.5 TB of
NetCDF) stored six ways and measured for storage, ingestion, and three representative queries, then
priced as a monthly bill:

| Method | What it is |
|---|---|
| `download` | fetch whole NetCDF files per query |
| `fuse` | read NetCDF in place through Mountpoint for Amazon S3 |
| `table_hilbert` | Apache Iceberg on Parquet, Hilbert row order, queried with DuckDB |
| `virtual` | virtual Icechunk: chunk references into the NetCDF files |
| `native_ts` | native Icechunk, chunked for point timeseries |
| `native_dl` | native Icechunk, chunked for ML dataloading |

## Where things are

- `src/wxtco/`: the `wxtco` package. Ingest pipelines (`ingest/`), the three queries and their
  backends (`queries/`), the benchmark runner (`bench.py`), and the cost model (`tco.py`).
- `docs/findings/`: every measurement, with the command that produced it. `canonical-results.md`
  is the headline set; `experiments/` holds earlier runs and dropped methods.
- `docs/decisions/`: the decision register. Why each layout, engine, and workload was chosen.
- `docs/infra/`: runbooks for the AWS setup and each ingest.
- `scripts/`: AWS setup (IAM policies, bucket, Arraylake delegation role) and EC2 job scripts.
- `web/`: the interactive charts embedded in the blog post, and `README.md` on how to embed them.
- `prices.toml`: unit prices and the baseline workload for the cost model.

The repository also contains the coding-agent harness used to run the study (`CLAUDE.md`,
`STATUS.md`, `docs/sessions/`, `docs/superpowers/`). The blog post describes how it was used.

## Reproducing

Python via [uv](https://docs.astral.sh/uv/):

```
uv sync
uv run pytest -q
uv run wxtco --help
uv run wxtco tco --storage-variant with_netcdf          # the monthly cost table
uv run wxtco tco --storage-variant with_netcdf --multiplier 10
```

Rebuilding the datasets needs an AWS account, an S3 bucket for the static NetCDF copy, and an
Arraylake org for the Icechunk repositories and the Iceberg catalog. Start with
`scripts/aws/README.md`, then the runbooks in `docs/infra/`. Replace `<ACCOUNT_ID>` in the IAM policy
files with your own account, and set `WXTCO_BUCKET` if you do not use the default bucket name.

## License

Apache License 2.0. See `LICENSE`.
