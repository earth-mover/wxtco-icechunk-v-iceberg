"""Drive the MOGREPS-G virtualization for one cycle into a local or Arraylake repo."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Literal

import icechunk as ic
import xarray as xr
from obstore.store import ObjectStore

from wxtco.config import Settings
from wxtco.ingest import mogreps_virtual as mv
from wxtco.ingest.table import IngestReport
from wxtco.progress import Progress

VirtualTarget = Literal["local", "arraylake"]
REPO_NAME = "mogreps-g-virtual"
BUCKET_NICKNAME = "wxtco-netcdf"


def chunk_cache_bytes() -> int:
    """Give the Icechunk chunk-data cache size: WXTCO_CHUNK_CACHE_BYTES, else half of physical RAM.
    Icechunk ships with this cache off; DuckDB's external file cache is bounded by its 80% memory limit."""
    env = os.environ.get("WXTCO_CHUNK_CACHE_BYTES")
    if env:
        return int(env)
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2


def read_config() -> ic.RepositoryConfig:
    """Give a reader config that only sets the chunk cache. Icechunk merges it over the stored config."""
    config = ic.RepositoryConfig()
    config.caching = ic.CachingConfig(num_bytes_chunks=chunk_cache_bytes())
    return config


def open_virtual_repo(
    settings: Settings, target: VirtualTarget, local_path: Path | None = None, cycle: str = ""
) -> ic.Repository:
    """Open the target repo: a local Icechunk repo, or the Arraylake repo for this org.
    Arraylake needs an org and a token in the environment; without them the client prompts."""
    if target == "local":
        return mv.dry_run_repo(str(local_path) if local_path else None, caching=read_config().caching)
    missing = [
        n
        for n, v in (("WXTCO_ORG", settings.arraylake_org), ("ARRAYLAKE_TOKEN", settings.arraylake_token))
        if not v
    ]
    if missing:
        raise ValueError(f"arraylake target needs {', '.join(missing)}")
    return mv.prod_repo(settings.arraylake_org, REPO_NAME, BUCKET_NICKNAME, None, cycle, config=read_config())


def _args(
    cycle: str,
    workers: int,
    variables: Sequence[str] | None,
    leads: int | None,
    allow_incomplete: bool,
    production: bool,
) -> argparse.Namespace:
    """The namespace Joe's run() expects. Fields match `parse_args`; the repo fields go unused."""
    return argparse.Namespace(
        cycle=cycle,
        workers=workers,
        variables=list(variables or []),
        include_levels=False,
        limit=None,
        leads=leads,
        plan=False,
        dry_run=not production,
        dry_run_path=None,
        fresh=False,
        allow_incomplete=allow_incomplete,
        horizon_hours=0,
        org=None,
        repo=None,
        bucket_nickname=None,
        storage_nickname=None,
    )


def ingest_cycle_virtual(
    repo: ic.Repository,
    progress_store: ObjectStore,
    cycle: str,
    origin_cycle: str,
    workers: int = 8,
    variables: Sequence[str] | None = None,
    leads: int | None = None,
    allow_incomplete: bool = False,
    production: bool = False,
    log: Callable[[str], None] = print,
) -> IngestReport:
    """One cycle, one unit. Joe's pipeline resumes per variable internally.
    `production` must match the repo target: the CLI ties them, other callers must do the same."""
    started = time.perf_counter()
    progress = Progress(progress_store, "virtual", cycle)
    if progress.done("cycle"):
        return IngestReport(cycle, 0, 1, 0, time.perf_counter() - started)
    mv.ORIGIN_CYCLE = origin_cycle
    if mv.written_cycle(repo, cycle):
        # The commit landed but the mark was lost. Catch the manifest up; do not write the slot twice.
        progress.mark("cycle")
        return IngestReport(cycle, 0, 1, 0, time.perf_counter() - started)
    try:
        code = mv.run_with_repo(
            repo, _args(cycle, workers, variables, leads, allow_incomplete, production), log=log
        )
    except SystemExit as exc:  # the pipeline exits on bad input; a loop must be able to catch it
        raise RuntimeError(f"virtual ingest {cycle}: {exc}") from exc
    if code != 0:
        return IngestReport(cycle, 0, 0, 0, time.perf_counter() - started)
    progress.mark("cycle")
    return IngestReport(cycle, 1, 0, 0, time.perf_counter() - started)


def open_virtual_dataset_group(repo: ic.Repository, group: str = "surface") -> xr.Dataset:
    """Read one group of the virtual archive through its chunk references."""
    # chunks=None: no dask; zarr fetches only the indexed chunks through its async pipeline.
    return xr.open_zarr(
        repo.readonly_session("main").store, group=group, consolidated=False, zarr_format=3, chunks=None
    )
