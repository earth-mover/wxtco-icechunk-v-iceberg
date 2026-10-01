"""Native Zarr ingest of the NetCDF copy into Icechunk: the `ts` and `dl` layouts, one repo each.

One cycle is one commit. The parent session appends one init slot, forks one session per ingest
unit for the worker processes, merges the forks back and commits.
"""

from __future__ import annotations

import multiprocessing
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from obstore.store import ObjectStore

from wxtco.config import Settings
from wxtco.ingest.native_layout import (
    DL_DIMS,
    LEAD_MINUTES,
    STATS,
    STATS_DIMS,
    TS_DIMS,
    ZARR_KWARGS,
    GridSpec,
    add_ts_arrays,
    append_init,
    array_name,
    create_dl,
    create_ts,
    dl_variable_order,
    frozen_slugs,
    write_region,
)
from wxtco.ingest.netcdf import grid_values, open_netcdf
from wxtco.ingest.virtual import read_config
from wxtco.progress import Progress
from wxtco.slugs import study_slugs
from wxtco.source import SourceFile, cycle_time, list_cycle, surface_only

NativeGroup = Literal["ts", "dl"]
NativeTarget = Literal["local", "arraylake"]
REPO_PREFIX = "mogreps-g-native"
BUCKET_NICKNAME = "wxtco-storage"
PROD_URI = "https://api.earthmover.io"
# Default parallel units: at full grid a `ts` variable block is ~15 GB, a `dl` sample ~7.2 GB.
DEFAULT_WORKERS = {"ts": 5, "dl": 6}
COMMIT_ATTEMPTS = 5


def open_native_repo(
    settings: Settings,
    group: NativeGroup,
    target: NativeTarget,
    local_path: Path | None = None,
    name: str | None = None,
) -> ic.Repository:
    """Open or create the native repo for one layout. No virtual chunk container is needed.

    `name` replaces the default repo name, for experiment repos that share a layout's reader.
    """
    if group not in DEFAULT_WORKERS:
        raise ValueError(f"group {group!r}; expected ts or dl")
    if target == "local":
        storage = ic.local_filesystem_storage(str(local_path)) if local_path else ic.in_memory_storage()
        return ic.Repository.open_or_create(storage, config=read_config())
    missing = [
        n
        for n, v in (("WXTCO_ORG", settings.arraylake_org), ("ARRAYLAKE_TOKEN", settings.arraylake_token))
        if not v
    ]
    if missing:
        raise ValueError(f"arraylake target needs {', '.join(missing)}")
    from arraylake import Client

    client = Client(service_uri=PROD_URI)
    org, name = settings.arraylake_org, name or f"{REPO_PREFIX}-{group}"
    if name in {r.name for r in client.list_repos(org)}:
        # `config` only carries the chunk cache; it merges over the stored config.
        return client.get_repo(f"{org}/{name}", config=read_config())
    if BUCKET_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org)}:
        raise ValueError(f"bucket config {BUCKET_NICKNAME} missing on org {org}")
    return client.create_repo(
        f"{org}/{name}",
        bucket_config_nickname=BUCKET_NICKNAME,
        description=f"Met Office MOGREPS-G, native Zarr, pcodec, {group} layout",
        metadata={"type": ["weather", "ensemble", "native"], "layout": [group]},
    )


@dataclass(frozen=True)
class NativeReport:
    """One cycle. `read_seconds` and `write_seconds` add up worker time, so they exceed `seconds`.

    `bytes_uncompressed` is the size of the decoded blocks, not the stored bytes.
    """

    cycle: str
    group: str
    units_done: int
    units_skipped: int
    read_seconds: float
    write_seconds: float
    commit_seconds: float
    seconds: float
    bytes_uncompressed: int
    commits: int
    leads_dropped: int = 0
    missing_files: int = 0


@dataclass(frozen=True)
class _TsUnit:
    """One variable for one cycle: a (member, lead, lat, lon) block into one init slab."""

    key: str
    name: str
    i_init: int
    shape: tuple[int, int, int, int]
    files: tuple[tuple[int, str], ...]
    file_workers: int


@dataclass(frozen=True)
class _DlUnit:
    """One lead for one cycle: a (variable, member, lat, lon) sample plus its statistics row."""

    key: str
    i_init: int
    i_lead: int
    shape: tuple[int, int, int, int]
    files: tuple[str | None, ...]
    file_workers: int


def _read_into(store: ObjectStore, key: str, out: np.ndarray) -> None:
    """Decode one file straight into its slice of a block. Threads write disjoint slices."""
    grid = grid_values(open_netcdf(store, key))
    if grid.shape != out.shape:
        raise ValueError(f"{key}: grid {grid.shape} does not fit {out.shape}")
    out[...] = grid


def _write_ts_unit(
    fork: ic.ForkSession, store: ObjectStore, unit: _TsUnit
) -> tuple[ic.ForkSession, float, float, int]:
    """Worker: read every file of one variable, then write one init slab. Returns the fork to merge."""
    # 4 encode tasks in flight: same wall clock as the default 10, half the encode buffer peak.
    zarr.config.set({"async.concurrency": 4})
    t0 = time.perf_counter()
    block = np.full(unit.shape, np.nan, dtype="float32")
    with ThreadPoolExecutor(max_workers=unit.file_workers) as pool:
        list(pool.map(lambda kv: _read_into(store, kv[1], block[:, kv[0]]), unit.files))
    read = time.perf_counter() - t0
    t1 = time.perf_counter()
    # The slab spans whole shards on every non-init axis, so Zarr writes without reading back.
    slab = xr.Dataset({unit.name: (TS_DIMS, block[np.newaxis])})
    write_region(fork.store, slab, {"init": slice(unit.i_init, unit.i_init + 1)})
    return fork, read, time.perf_counter() - t1, int(block.nbytes)


def _sample_stats(block: np.ndarray) -> dict[str, np.ndarray]:
    """Anemoi-style per-variable statistics of one sample, keyed by array name. NaN is not counted."""
    v = block.shape[0]
    count = np.zeros(v, "int64")
    sums = np.zeros(v, "float64")
    squares = np.zeros(v, "float64")
    lo = np.full(v, np.nan, "float32")
    hi = np.full(v, np.nan, "float32")
    for i in range(v):
        x = block[i].reshape(-1)
        good = ~np.isnan(x)
        n = int(good.sum())
        count[i] = n
        if n == 0:
            continue
        vals = x if n == x.size else x[good]
        wide = vals.astype("float64")
        sums[i], squares[i] = wide.sum(), (wide * wide).sum()
        lo[i], hi[i] = vals.min(), vals.max()
    return {"count": count, "sums": sums, "squares": squares, "minimum": lo, "maximum": hi}


def _write_dl_unit(
    fork: ic.ForkSession, store: ObjectStore, unit: _DlUnit
) -> tuple[ic.ForkSession, float, float, int]:
    """Worker: read the variables of one lead, then write one shard and its statistics row.

    The returned write time includes `_sample_stats`, which runs between the read and the write.
    """
    # Same cap as `ts`: the block is ~7.2 GB at full grid, so bound the shard encode buffers.
    zarr.config.set({"async.concurrency": 4})
    t0 = time.perf_counter()
    block = np.full(unit.shape, np.nan, dtype="float32")
    present = [(i, k) for i, k in enumerate(unit.files) if k is not None]
    with ThreadPoolExecutor(max_workers=unit.file_workers) as pool:
        list(pool.map(lambda ik: _read_into(store, ik[1], block[ik[0]]), present))
    read = time.perf_counter() - t0
    t1 = time.perf_counter()
    stats = _sample_stats(block)
    region = {"init": slice(unit.i_init, unit.i_init + 1), "lead": slice(unit.i_lead, unit.i_lead + 1)}
    # The sample is one whole shard, so Zarr encodes it without reading the stored shard back.
    slab = xr.Dataset(
        {"data": (DL_DIMS, block[np.newaxis, np.newaxis])}
        | {n: (STATS_DIMS, stats[n][np.newaxis, np.newaxis]) for n in STATS}
    )
    write_region(fork.store, slab, region)
    return fork, read, time.perf_counter() - t1, int(block.nbytes)


def _grid_axes(store: ObjectStore, key: str) -> tuple[np.ndarray, np.ndarray, int]:
    """Give (lat, lon, members) from one source file, to size the arrays."""
    ds = open_netcdf(store, key)
    lat = ds["latitude"].values.astype("float32")
    lon = ds["longitude"].values.astype("float32")
    return lat, lon, int(ds.sizes["realization"])


def _init_slot(inits: np.ndarray, value: object) -> tuple[int, bool]:
    """Give (index, appended) for one init time. A cycle already on the axis reuses its slot.

    An older cycle is accepted when its slot exists; only an absent older init is rejected,
    because the axis is append-only and cannot take an out-of-order value.
    """
    if inits.size:
        hit = np.flatnonzero(inits == value)
        if hit.size:
            return int(hit[0]), False
        if value < inits[-1]:
            raise ValueError(
                f"init {value} has no slot and is older than the last slot {inits[-1]}; "
                "the init axis is append-only, so ingest cycles in chronological order. "
                "To recover, rerun the skipped cycle before any newer one, or rebuild the repo "
                "from the oldest cycle."
            )
    return int(inits.size), True


def _commit(session: ic.Session, message: str, cycle: str) -> None:
    """Commit, rebasing on a conflict. Two ingest processes on one repo race on the same branch."""
    try:
        session.commit(message, rebase_with=ic.ConflictDetector(), rebase_tries=COMMIT_ATTEMPTS)
    # A rebase failure means the other writer touched what we wrote, e.g. it appended an init
    # slab and our slot is now a different cycle. The data is unsafe to commit.
    except (ic.ConflictError, ic.RebaseFailedError) as exc:
        raise ValueError(
            f"{cycle}: commit conflicted after {COMMIT_ATTEMPTS} rebase tries ({exc}); "
            "another process wrote this repo; run one ingest process per repo, then rerun this cycle"
        ) from exc


def _coverage(
    variables: Sequence[str], by_slug: dict[str, dict[int, SourceFile]], axis: Sequence[int]
) -> tuple[list[int], int]:
    """Give (source leads off the lead axis, count of (variable, lead) pairs with no file)."""
    on_axis = set(axis)
    dropped = sorted({lead for s in variables for lead in by_slug.get(s, {}) if lead not in on_axis})
    missing = sum(1 for s in variables for lead in axis if lead not in by_slug.get(s, {}))
    return dropped, missing


def _open_layout(session: ic.Session) -> xr.Dataset:
    """Give the stored layout: coordinates and attributes, no data read."""
    return xr.open_zarr(session.store, chunks=None, **ZARR_KWARGS)


def _lead_axis(ds: xr.Dataset) -> list[int]:
    """Give the stored lead axis in minutes."""
    return [int(v) for v in ds["lead"].values.astype("timedelta64[m]").astype("int64")]


def _ensure_group(
    repo: ic.Repository,
    store: ObjectStore,
    group: NativeGroup,
    slugs: Sequence[str],
    leads: Sequence[int] | None,
    first_key: str,
    log: Callable[[str], None],
) -> int:
    """Create the layout on first use. Gives the number of commits made (0 or 1)."""
    session = repo.writable_session("main")
    try:
        if "lead" in _open_layout(session).dims:
            return 0
    # A repo that never held a layout has no root group yet.
    except zarr.errors.GroupNotFoundError:
        pass
    lat, lon, members = _grid_axes(store, first_key)
    axis = list(leads or LEAD_MINUTES)
    build = create_ts if group == "ts" else create_dl
    build(session.store, GridSpec(members, lat, lon, axis), slugs)
    session.commit(f"create native {group} layout: {len(slugs)} variables, {len(axis)} leads")
    log(f"created native {group} layout: {members} members, {lat.size}x{lon.size}, {len(axis)} leads")
    return 1


def ingest_cycle_native(
    repo: ic.Repository,
    store: ObjectStore,
    progress_store: ObjectStore,
    cycle: str,
    group: NativeGroup,
    slugs: Sequence[str] | None = None,
    variables: Sequence[str] | None = None,
    leads: Sequence[int] | None = None,
    workers: int | None = None,
    file_workers: int = 8,
    log: Callable[[str], None] = print,
) -> NativeReport:
    """Write one cycle into one native layout as a single commit.

    `slugs` is the repo variable set, frozen when the repo is created; `variables` restricts this run
    and is `ts` only. Reruns skip units in `_progress/native_<group>/` and reuse the cycle init slot.
    """
    started = time.perf_counter()
    if group not in DEFAULT_WORKERS:
        raise ValueError(f"group {group!r}; expected ts or dl")
    # A `dl` shard holds every variable, so a partial write would blank the ones left out.
    if group == "dl" and variables is not None:
        raise ValueError("variables cannot restrict a dl run: one shard holds all variables")
    workers = workers or DEFAULT_WORKERS[group]
    slugs = list(slugs) if slugs is not None else study_slugs()
    # `dl` stacks the variables in one array, so the axis order is fixed before the repo is made.
    if group == "dl":
        slugs = dl_variable_order(slugs)
    by_slug: dict[str, dict[int, SourceFile]] = {
        d.slug: {f.lead_minutes: f for f in d.files} for d in surface_only(list_cycle(store, cycle))
    }
    known = [s for s in slugs if s in by_slug]
    if not known:
        raise ValueError(f"cycle {cycle} has no files for any of {len(slugs)} variables")
    progress = Progress(progress_store, f"native_{group}", cycle)
    commits = _ensure_group(repo, store, group, slugs, leads, next(iter(by_slug[known[0]].values())).key, log)

    session = repo.writable_session("main")
    ds = _open_layout(session)
    stored = frozen_slugs(ds.attrs)
    axis = _lead_axis(ds)
    if leads is not None and list(leads) != axis:
        raise ValueError(f"repo lead axis has {len(axis)} leads; {len(list(leads))} were asked for")
    if group == "dl" and stored != slugs:
        raise ValueError(f"dl variable list is frozen at {stored}")
    if group == "dl":
        slugs = stored  # The block variable axis follows the stored coordinate, not the caller.
    if group == "ts":
        # A variable added after the repo exists gets its own array at the stored init length.
        add_ts_arrays(session.store, ds, slugs)
        ds = _open_layout(session)  # reopen, so the init append below also grows the new array
    index = {lead: i for i, lead in enumerate(axis)}
    wanted = [s for s in known if variables is None or s in set(variables)]
    # Coverage is over what the slabs span: the wanted variables for `ts`, all of them for `dl`.
    dropped, missing = _coverage(wanted if group == "ts" else slugs, by_slug, axis)
    log(f"{cycle} {group}: {len(dropped)} source leads off the lead axis{_first(dropped)}")
    log(f"{cycle} {group}: {missing} (variable, lead) pairs have no file and stay NaN")
    init_at = np.datetime64(int(cycle_time(cycle).timestamp()), "s")
    i_init, appended = _init_slot(ds["init"].values.astype("datetime64[s]"), init_at)
    shape = (ds.sizes["member"], len(axis), ds.sizes["lat"], ds.sizes["lon"])
    units = _units(group, slugs, wanted, by_slug, index, i_init, shape, file_workers)
    if appended and progress.units():
        # The cycle has no slot in this repo, so its manifest is from a repo that no longer exists.
        log(f"{cycle} {group}: {len(progress.units())} stale progress marks ignored (repo has no slot)")
        progress.clear()
    pending = [u for u in units if not progress.done(u.key)]
    if not pending:
        wall = time.perf_counter() - started
        return NativeReport(
            cycle, group, 0, len(units), 0.0, 0.0, 0.0, wall, 0, commits, len(dropped), missing
        )

    if appended:
        append_init(session.store, ds, init_at)
    write = _write_ts_unit if group == "ts" else _write_dl_unit
    read_s = write_s = 0.0
    nbytes = 0
    forks = []
    # Spawn, not fork: by now obstore and Icechunk hold tokio runtime threads, and forking deadlocks.
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = {pool.submit(write, session.fork(), store, u): u for u in pending}
        for future in as_completed(futures):
            unit = futures[future]
            fork, read, wrote, size = future.result()
            forks.append(fork)
            read_s, write_s, nbytes = read_s + read, write_s + wrote, nbytes + size
            log(f"{cycle} {group} {unit.key}: read {read:.1f}s write {wrote:.1f}s {size / 1e6:.0f} MB")
    # The merge rewrites the change set, so it belongs in the commit cost, not in nothing.
    t0 = time.perf_counter()
    session.merge(*forks)
    _commit(session, f"{cycle} {group}: {len(pending)} units at init slot {i_init}", cycle)
    commit_s = time.perf_counter() - t0
    for unit in pending:
        progress.mark(unit.key)
    return NativeReport(
        cycle,
        group,
        len(pending),
        len(units) - len(pending),
        read_s,
        write_s,
        commit_s,
        time.perf_counter() - started,
        nbytes,
        commits + 1,
        len(dropped),
        missing,
    )


def _first(leads: Sequence[int], n: int = 5) -> str:
    """Give a short ` (first: ...)` tail for a log line, or nothing when the list is empty."""
    return f" (first: {list(leads[:n])})" if leads else ""


def _units(
    group: NativeGroup,
    slugs: Sequence[str],
    wanted: Sequence[str],
    by_slug: dict[str, dict[int, SourceFile]],
    index: dict[int, int],
    i_init: int,
    shape: tuple[int, int, int, int],
    file_workers: int,
) -> list[_TsUnit | _DlUnit]:
    """Build the ingest units: one per variable for `ts`, one per populated lead for `dl`."""
    members, n_leads, ny, nx = shape
    if group == "ts":
        out: list[_TsUnit | _DlUnit] = []
        for slug in wanted:
            files = tuple((index[lead], f.key) for lead, f in sorted(by_slug[slug].items()) if lead in index)
            if files:
                name = array_name(slug)
                out.append(
                    _TsUnit(f"var={name}", name, i_init, (members, n_leads, ny, nx), files, file_workers)
                )
        return out
    # `dl`: the variable order is the frozen repo order, and a variable with no file stays NaN.
    picked = set(wanted)
    units: list[_TsUnit | _DlUnit] = []
    for lead, i in index.items():
        keys = tuple(
            by_slug[s][lead].key if s in picked and lead in by_slug.get(s, {}) else None for s in slugs
        )
        if any(k is not None for k in keys):
            units.append(
                _DlUnit(f"lead={lead}", i_init, i, (len(slugs), members, ny, nx), keys, file_workers)
            )
    return units
