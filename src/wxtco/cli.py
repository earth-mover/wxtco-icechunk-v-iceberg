"""CLI entry point. Sub-apps: fixture, source, copy, ingest, jobs; commands bench and tco."""

from __future__ import annotations

from pathlib import Path

import typer

from wxtco.config import Settings, split_url, store_for

app = typer.Typer(no_args_is_help=True, help="Weather forecast data TCO analysis")
fixture_app = typer.Typer(help="Synthetic test data")
source_app = typer.Typer(help="Inspect source or copy buckets")
copy_app = typer.Typer(help="Static copy of source cycles")
ingest_app = typer.Typer(help="Ingest cycles into one of the storage methods")
jobs_app = typer.Typer(help="Status of detached EC2 jobs")
app.add_typer(fixture_app, name="fixture")
app.add_typer(source_app, name="source")
app.add_typer(copy_app, name="copy")
app.add_typer(ingest_app, name="ingest")
app.add_typer(jobs_app, name="jobs")

DEFAULT_CYCLE = "2026/09/16/T0000Z"


def _store(settings: Settings, use_copy: bool):
    """Anonymous source store, or the copy store with the ambient AWS credentials."""
    if use_copy:
        return store_for(settings.copy_url, region=settings.copy_region)
    # The source is public unless it is the copy bucket, which needs the credentials.
    anonymous = (
        settings.source_url.startswith("s3://")
        and split_url(settings.source_url)[0] != split_url(settings.copy_url)[0]
    )
    return store_for(settings.source_url, region=settings.source_region, anonymous=anonymous)


@app.command()
def version() -> None:
    """Print the package version."""
    from importlib.metadata import version as v

    typer.echo(v("wxtco"))


@fixture_app.command("build")
def fixture_build(root: Path, cycle: str = DEFAULT_CYCLE) -> None:
    """Write one synthetic cycle under ROOT."""
    from wxtco.fixture import build_cycle

    paths = build_cycle(root, cycle)
    typer.echo(f"wrote {len(paths)} files under {root / cycle}")


@source_app.command("cycles")
def source_cycles(
    copy: bool = typer.Option(False, "--copy", help="List the static copy instead of the source"),
) -> None:
    """List cycles, oldest first."""
    from wxtco.source import available_cycles

    for c in available_cycles(_store(Settings.from_env(), copy)):
        typer.echo(c)


@source_app.command("inventory")
def source_inventory(
    cycle: str,
    copy: bool = typer.Option(False, "--copy", help="Inspect the static copy instead of the source"),
) -> None:
    """Per-diagnostic file count for one cycle."""
    from wxtco.source import list_cycle

    diags = list_cycle(_store(Settings.from_env(), copy), cycle)
    for d in diags:
        typer.echo(f"{len(d.files):4d}  {'level ' if d.is_level else 'surface'}  {d.slug}")
    typer.echo(f"{sum(len(d.files) for d in diags)} files, {len(diags)} diagnostics")


@copy_app.command("plan")
def copy_plan(cycles: str = typer.Option(..., help="Comma-separated cycles")) -> None:
    """Print the aws s3 cp commands for the given cycles."""
    from wxtco.copy import copy_commands

    s = Settings.from_env()
    wanted = [c.strip().strip("/") for c in cycles.split(",") if c.strip()]
    for cmd in copy_commands(s.source_url, s.copy_url, wanted, s.source_region, s.copy_region):
        typer.echo(cmd)


@copy_app.command("verify")
def copy_verify(cycle: str = typer.Option(...)) -> None:
    """Compare the copy with the source for one cycle. Exit 1 if files are missing or the source is empty."""
    from wxtco.copy import verify_copy

    s = Settings.from_env()
    r = verify_copy(_store(s, False), _store(s, True), cycle)
    typer.echo(
        f"{r.cycle}: {r.present}/{r.expected} files present, {r.bytes / 1e9:.1f} GB, {len(r.missing)} missing"
    )
    # An empty source is a bad cycle name or a bad url, not a complete copy.
    if r.expected == 0:
        typer.echo(f"no source files for {r.cycle}")
    for k in r.missing[:20]:
        typer.echo(f"  missing {k}")
    if len(r.missing) > 20:
        typer.echo(f"  ... and {len(r.missing) - 20} more")
    raise typer.Exit(code=1 if r.missing or r.expected == 0 else 0)


@copy_app.command("pick-window")
def copy_pick_window(
    days: int = typer.Option(7, help="Days in the window; 4 cycles per day"),
    write: bool = typer.Option(False, help="Write study_cycles.txt"),
) -> None:
    """Choose the study window from cycles available in the source."""
    # Keep these imports local: tests patch `wxtco.window.WINDOW_FILE` before the call.
    from wxtco.source import available_cycles
    from wxtco.window import WINDOW_FILE, pick_window

    w = pick_window(available_cycles(_store(Settings.from_env(), False)), days=days)
    for c in w:
        typer.echo(c)
    if write:
        WINDOW_FILE.write_text("\n".join(w) + "\n")
        typer.echo(f"wrote {WINDOW_FILE}")


@ingest_app.command("table")
def ingest_table(
    cycle: str = typer.Option(...),
    local: Path | None = typer.Option(None, help="Use a local sqlite catalog at this path"),
    workers: int = typer.Option(8, help="Threads that read NetCDF files per lead"),
    leads: str | None = typer.Option(None, help="Comma-separated lead minutes to restrict to"),
) -> None:
    """Ingest one cycle into the wide Iceberg table."""
    from wxtco.catalog import arraylake_catalog, local_catalog
    from wxtco.ingest.table import ingest_cycle_table
    from wxtco.slugs import study_slugs

    s = Settings.from_env()
    if local is not None:
        # Local mode: source store from WXTCO_SOURCE_URL, progress beside the catalog.
        catalog = local_catalog(local)
        store = _store(s, False)
        progress_store = store_for(f"file://{local}")
    else:
        catalog, store = arraylake_catalog(s), _store(s, True)
        progress_store = store
    lead_list = [int(x.strip()) for x in leads.split(",") if x.strip()] if leads else None
    r = ingest_cycle_table(
        catalog,
        store,
        progress_store,
        cycle,
        study_slugs(),
        workers=workers,
        leads=lead_list,
        log=typer.echo,
    )
    typer.echo(
        f"{r.cycle}: {r.units_done} leads done, {r.units_skipped} skipped, {r.rows} rows, {r.seconds:.0f}s"
    )


@ingest_app.command("virtual")
def ingest_virtual(
    cycle: str = typer.Option(..., help="Cycle to ingest, e.g. 2026/09/16/T0000Z"),
    local: Path | None = typer.Option(None, help="Local Icechunk repo path instead of Arraylake"),
    origin: str | None = typer.Option(None, help="Origin cycle; default first line of study_cycles.txt"),
    workers: int = typer.Option(8, help="Threads that read NetCDF headers per variable"),
    variables: str | None = typer.Option(None, help="Comma-separated variables to restrict to"),
    leads: int | None = typer.Option(None, help="Keep only the first N lead times"),
    allow_incomplete: bool = typer.Option(False, help="Write a cycle whose files are not all present"),
) -> None:
    """Ingest one cycle as virtual chunk references."""
    from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_repo
    from wxtco.window import study_cycles

    s = Settings.from_env()
    origin_cycle = origin or (study_cycles() or [cycle])[0]
    if local is not None:
        # Progress lives beside the repo, not inside it, to keep the Icechunk directory clean.
        repo = open_virtual_repo(s, "local", local_path=local)
        progress_store = store_for(f"file://{local}_progress")
    else:
        repo, progress_store = open_virtual_repo(s, "arraylake", cycle=cycle), _store(s, True)
    var_list = [v.strip() for v in variables.split(",") if v.strip()] if variables else None
    r = ingest_cycle_virtual(
        repo,
        progress_store,
        cycle,
        origin_cycle,
        workers=workers,
        variables=var_list,
        leads=leads,
        allow_incomplete=allow_incomplete,
        production=local is None,
        log=typer.echo,
    )
    typer.echo(f"{r.cycle}: {r.units_done} units done, {r.units_skipped} skipped, {r.seconds:.0f}s")


@ingest_app.command("native")
def ingest_native(
    group: str = typer.Option(..., help="Native layout to write: ts (per variable) or dl (per sample)"),
    cycle: str = typer.Option(..., help="Cycle to ingest, e.g. 2026/09/16/T0000Z"),
    local: Path | None = typer.Option(None, help="Local Icechunk repo path instead of Arraylake"),
    workers: int | None = typer.Option(None, help="Parallel ingest units; default 5 for ts, 6 for dl"),
    variables: str | None = typer.Option(None, help="Comma-separated variables to restrict this run to"),
    file_workers: int = typer.Option(8, help="Threads that read NetCDF files inside one unit"),
) -> None:
    """Ingest one cycle into a native Zarr layout. One cycle is one commit."""
    from wxtco.ingest.native import ingest_cycle_native, open_native_repo
    from wxtco.slugs import study_slugs

    if group not in ("ts", "dl"):
        raise typer.BadParameter(f"{group}; expected ts or dl", param_hint="--group")
    # A `dl` shard holds every variable, so a restricted run would blank the ones left out.
    if group == "dl" and variables:
        raise typer.BadParameter(
            "a dl shard holds all variables; a restricted run would blank the rest",
            param_hint="--variables",
        )
    s = Settings.from_env()
    if local is not None:
        # Progress lives beside the repo, not inside it, to keep the Icechunk directory clean.
        repo, store = open_native_repo(s, group, "local", local_path=local), _store(s, False)
        progress_store = store_for(f"file://{local}_progress")
    else:
        repo, store = open_native_repo(s, group, "arraylake"), _store(s, True)
        progress_store = store
    var_list = [v.strip() for v in variables.split(",") if v.strip()] if variables else None
    r = ingest_cycle_native(
        repo,
        store,
        progress_store,
        cycle,
        group,
        slugs=study_slugs(),
        variables=var_list,
        workers=workers,
        file_workers=file_workers,
        log=typer.echo,
    )
    typer.echo(
        f"{r.cycle} {r.group}: {r.units_done} units done, {r.units_skipped} skipped, "
        f"{r.bytes_uncompressed / 1e9:.1f} GB, read {r.read_seconds:.0f}s, write {r.write_seconds:.0f}s, "
        f"commit {r.commit_seconds:.1f}s, {r.seconds:.0f}s total ({r.commits} commits), "
        f"{r.leads_dropped} leads dropped, {r.missing_files} missing files"
    )


@app.command("bench")
def bench(
    method: str = typer.Option(
        ...,
        help="Storage method to time: table, virtual, native_ts, native_dl, native_anemoi, download, or fuse",
    ),
    query: str = typer.Option(..., help="Query to time: q1, q2, q3, or q3_stream"),
    params: str = typer.Option(
        ..., help='JSON query parameters, e.g. \'{"lat":51.5,"lon":-0.1,"cycle":"2026/09/16/T0000Z"}\''
    ),
    runs: int = typer.Option(5, help="Timed repeats; the first run is cold"),
    out: Path = typer.Option(..., help="CSV to append to; the header is written if it is new"),
    local: Path | None = typer.Option(
        None, help="Local catalog, Icechunk repo, or NetCDF root instead of the cloud"
    ),
    transfer: str = typer.Option(
        "obstore",
        help="Download client for --method download: obstore (one GET per object) or crt (S3 Transfer Manager)",
    ),
    concurrency: int = typer.Option(
        64, help="Objects fetched in parallel for --method download, both transfer clients"
    ),
    fuse_root: Path | None = typer.Option(
        None,
        envvar="WXTCO_FUSE_ROOT",
        help="Mount point of the read-only NetCDF FUSE mount, for --method fuse",
    ),
    instance_type: str = typer.Option(
        "local", envvar="WXTCO_INSTANCE_TYPE", help="Instance type recorded in the CSV, for the cost model"
    ),
) -> None:
    """Time one query N times and append to a findings CSV."""
    import json
    import os

    from wxtco.bench import git_sha, run_query

    try:
        parsed = json.loads(params)
    # Fail on a bad parameter string before the backend opens anything remote.
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"invalid JSON: {exc}", param_hint="--params") from exc

    s = Settings.from_env()
    if method == "table":
        from wxtco.queries.table_backend import TableBackend

        if local is not None:
            from wxtco.catalog import ensure_table, local_catalog
            from wxtco.slugs import study_slugs

            meta_loc = ensure_table(local_catalog(local), study_slugs()).metadata_location
            backend = TableBackend.local(meta_loc)
        else:
            backend = TableBackend.arraylake(s)
    elif method == "virtual":
        from wxtco.ingest.virtual import open_virtual_repo
        from wxtco.queries.tensor_backend import TensorBackend

        repo = open_virtual_repo(s, "local", local_path=local) if local else open_virtual_repo(s, "arraylake")
        backend = TensorBackend.from_repo(repo, name="virtual")
    elif method in ("native_ts", "native_dl") or method.startswith("native_anemoi"):
        from wxtco.ingest.native import REPO_PREFIX, open_native_repo
        from wxtco.queries.native_backend import open_native_backend

        # `native_anemoi*` are unsharded one-chunk-per-sample experiment repos with the dl array shape;
        # `native_anemoi_lz4` opens mogreps-g-native-anemoi-lz4.
        anemoi = method.startswith("native_anemoi")
        group = "dl" if anemoi else method.removeprefix("native_")
        name = f"{REPO_PREFIX}-" + method.removeprefix("native_").replace("_", "-") if anemoi else None
        target = "local" if local else "arraylake"
        repo = open_native_repo(s, group, target, local_path=local, name=name)
        backend = open_native_backend(repo, group, method)
    elif method in ("download", "fuse"):
        from wxtco.queries.netcdf_backend import DownloadBackend, FuseBackend

        # The NetCDF files themselves: the copy bucket, or a local fixture root.
        store = store_for(f"file://{local}") if local is not None else _store(s, True)
        if method == "download":
            if transfer == "crt" and local is not None:
                raise typer.BadParameter("crt needs an S3 bucket, not --local", param_hint="--transfer")
            bucket, prefix = split_url(s.copy_url) if transfer == "crt" else (None, "")
            backend = DownloadBackend(
                store,
                concurrency=concurrency,
                transfer=transfer,
                bucket=bucket,
                prefix=prefix,
                region=s.copy_region,
            )
        else:
            if fuse_root is None:
                raise typer.BadParameter(
                    "--fuse-root is required for --method fuse", param_hint="--fuse-root"
                )
            backend = FuseBackend(store, fuse_root)
    else:
        raise typer.BadParameter(
            f"{method}; expected table, virtual, native_ts, native_dl, native_anemoi, download, or fuse",
            param_hint="--method",
        )

    meta = {
        "instance_type": instance_type,
        "region": os.environ.get("AWS_DEFAULT_REGION", s.copy_region),
        "git_sha": git_sha(),
    }
    rows = run_query(backend, query, parsed, runs, out, meta)
    typer.echo(f"{method} {query}: " + ", ".join(f"{r.seconds:.2f}s" for r in rows))


@app.command("tco")
def tco(
    multiplier: float = typer.Option(1.0, help="Scale the query workload only"),
    findings: Path = typer.Option(
        Path("docs/findings"), help="Findings folder with storage.csv, etl/, bench/"
    ),
    prices: Path = typer.Option(Path("prices.toml"), help="Unit price table"),
    storage_variant: str = typer.Option("data", help="storage.csv variant: data or with_netcdf"),
) -> None:
    """Print the monthly TCO table from findings CSVs."""
    from wxtco.tco import tco_from_findings

    df = tco_from_findings(findings, prices, multiplier, storage_variant)
    typer.echo(df.round(2).to_markdown(index=False))


@jobs_app.command("status")
def jobs_status() -> None:
    """List progress manifests and finished job logs in the copy bucket."""
    import obstore as obs

    from wxtco.jobs import progress_lines

    store = _store(Settings.from_env(), True)
    for p in progress_lines(store):
        typer.echo(f"{p.method:8s} {p.cycle_id}  {p.units} units done")
    logs = [item["path"] for page in obs.list(store, prefix="_logs/") for item in page]
    typer.echo(f"{len(logs)} finished job logs; `wxtco jobs log <name>` to tail one")


@jobs_app.command("log")
def jobs_log(
    name: str = typer.Argument(..., help="Log name, without the _logs/ prefix and .log suffix"),
    lines: int = typer.Option(20, help="Lines to tail"),
) -> None:
    """Tail one finished job log from the copy bucket."""
    from wxtco.jobs import log_tail

    typer.echo(log_tail(_store(Settings.from_env(), True), name, lines))
