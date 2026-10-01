"""Virtualize one MOGREPS-G cycle into an Icechunk repo.

The source is the static copy (`Settings.copy_url`), driven by `wxtco.ingest.virtual`..
Config travels by environment variables only. Spawned workers re-import this module and re-read them.
"""

from __future__ import annotations

import argparse
import functools  # Partial() keeps the default log flushing
import hashlib
import multiprocessing
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence  # Callable types the log hook
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
from itertools import groupby

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import LocalStore, S3Store  # LocalStore serves `file://` sources in tests
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser
from xarray.backends.zarr import FillValueCoder
from xarray.coding.times import encode_cf_datetime

from wxtco.config import Settings, split_url  # The source is configured, not hard coded

# Source is the static copy, from wxtco.config. `file://` roots are used by tests.
_SETTINGS = Settings.from_env()
BUCKET_URL = _SETTINGS.copy_url.rstrip("/") + "/"
BUCKET_NAME, ROOT_PREFIX = split_url(_SETTINGS.copy_url)
REGION = _SETTINGS.copy_region
PROD_URI = "https://api.earthmover.io"
KEY_RE = re.compile(r"^(?P<valid>\d{8}T\d{4}Z)-PT(?P<hours>\d{4})H(?P<minutes>\d{2})M-(?P<slug>.+)\.nc$")
WINDOW_RE = re.compile(r"-(PT\d{2}H)$")
CYCLE_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})/T(\d{2})(\d{2})Z$")
INIT_DIM = "forecast_reference_time"
LEAD_DIM = "forecast_period"
PER_LEAD = frozenset({LEAD_DIM, "time"})
GRID_DIMS = frozenset({"realization", "latitude", "longitude"})
ROOT_MESSAGE_SUFFIX = "root attributes"

# The init axis is append-only, never a ring. slot = cycle_index - origin, with no modulo, so array
# order is time order permanently. A ring buffer was tried first and had to be abandoned: once it
# wrapped, the newest cycle landed on slot 0 and forecast_reference_time stopped being sorted.
#
# The axis therefore grows by one slot per cycle (~1460/yr), but only the ~120 cycles inside the Met
# Office retention window ever hold data. An empty slot costs no chunks and, because manifests are
# split per init time, no manifest either -- expiring a slot deletes its chunk prefix and the shard
# goes with it. The coordinate entry stays, which is what keeps the axis monotonic forever.
# The study window's first cycle. Set by wxtco.ingest.virtual before run().
ORIGIN_CYCLE = "2026/01/01/T0000Z"
CYCLE_SECONDS = 6 * 3600
SINGLE_LEVEL_FILES = 11521  # a complete cycle is 14,020 objects; 2,499 of them are level diagnostics

# Left to itself xarray derives time units from whatever cycle seeded the array, e.g.
# "days since 2026-09-12 06:00:00" as int64 -- under which a 12Z cycle is 0.25 days and rounds away.
# Every time coordinate is pinned to an absolute epoch so any cycle is representable.
CF_UNITS = "seconds since 1970-01-01"
CF_CALENDAR = "proleptic_gregorian"

# One contiguous chunk per 1-D coordinate. The init coordinate is the one that gets this wrong by
# default: the seeding cycle creates it at length 1, so xarray infers a chunk of 1 and the later
# resize inherits it -- 129 chunks for a 1 KB array, and 129 manifest references to describe them.
# Sized well past the axis so it stays a single chunk as the archive grows; a chunk may exceed the
# array it belongs to, and an int64 chunk this size is 32 KB whether it holds 129 values or 4096.
COORD_INIT_CHUNK = 4096

# Population tracking, one uint8 cell per (init, lead) per variable, in a `status` subgroup beside the
# data. Vocabulary is shared with the other Earthmover archives (see earthmover-public/ifs-hres-archive-2)
# so tooling reads the same flags everywhere. The fill value does the heavy lifting: a cell nobody has
# written already reads as not_yet_populated, so an empty ring slot is self-describing at zero cost.
STATUS_GROUP = "status"
STATUS_VALID = 0
STATUS_PENDING = 100  # fill value
STATUS_UNAVAILABLE = 101  # source object no longer in the bucket, references are dangling
STATUS_ATTRS = {
    "flag_values": [0, 100, 101, 102, 103, 200, 201, 202, 203],
    "flag_meanings": (
        "valid not_yet_populated unavailable will_never_exist not_requested "
        "malformed_data_received decompression_error read_error decoding_error"
    ),
    "description": "population state of each (forecast_reference_time, forecast_period) cell",
}


@dataclass(frozen=True)
class SourceFile:
    key: str
    slug: str
    lead_minutes: int


@dataclass(frozen=True)
class Diagnostic:
    slug: str
    files: tuple[SourceFile, ...]

    @property
    def window(self) -> str:
        match = WINDOW_RE.search(self.slug)
        return match[1] if match else ""

    @property
    def schedule(self) -> tuple[int, ...]:
        return tuple(f.lead_minutes for f in self.files)

    @property
    def name(self) -> str:
        return self.slug.replace("-", "_")

    @property
    def is_level(self) -> bool:
        return self.slug.endswith("_levels")


@dataclass(frozen=True)
class Inspection:
    loadable: tuple[str, ...]
    first: xr.Dataset


@dataclass(frozen=True)
class Placement:
    diagnostic: Diagnostic
    loadable: tuple[str, ...]
    signature: str
    group: str
    grid: str


def parse_key(key: str) -> SourceFile | None:
    match = KEY_RE.match(key.rsplit("/", 1)[-1])
    if match is None:
        return None
    return SourceFile(key=key, slug=match["slug"], lead_minutes=int(match["hours"]) * 60 + int(match["minutes"]))


def cycle_time(cycle: str) -> datetime:
    match = CYCLE_RE.match(cycle)
    if match is None:
        raise SystemExit(f"bad cycle {cycle!r}, want YYYY/MM/DD/THHMMZ")
    year, month, day, hour, minute = (int(g) for g in match.groups())
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def cycle_index(cycle: str) -> int:
    """Global 6-hourly index, so slots are reproducible independent of ingest order."""
    return int(cycle_time(cycle).timestamp()) // CYCLE_SECONDS


def slot_of(cycle: str, origin: int) -> int:
    """Position on the init axis. No modulo: array order is time order, permanently."""
    slot = cycle_index(cycle) - origin
    if slot < 0:
        raise SystemExit(f"cycle {cycle} precedes the archive origin; nothing to write")
    return slot


def cycle_at(origin: int, slot: int) -> datetime:
    return datetime.fromtimestamp((origin + slot) * CYCLE_SECONDS, tz=UTC)


def window_slots(origin: int, horizon_hours: int) -> int:
    """Slots from the origin through `horizon_hours` ahead, rounded up to a whole cycle.

    Sized past the present on purpose: the coordinate is laid out for the whole ingest run up front,
    so a cycle landing mid-run writes into an existing slot instead of resizing the axis underneath
    a reader.
    """
    end = datetime.now(UTC) + timedelta(hours=horizon_hours)
    end = end.replace(minute=0, second=0, microsecond=0)
    end += timedelta(hours=(-end.hour) % 6)
    return int((end.timestamp() // CYCLE_SECONDS) - origin) + 1


@cache
def source_store() -> LocalStore | S3Store:
    # Local store for tests, S3 with ambient credentials for the copy bucket.
    if BUCKET_URL.startswith("file://"):
        return LocalStore(BUCKET_NAME)
    return S3Store(BUCKET_NAME, prefix=ROOT_PREFIX or None, region=REGION)


@cache
def registry() -> ObjectStoreRegistry:
    # BUCKET_URL already ends with ROOT_PREFIX and a slash, and the store is rooted there,
    # so a virtual chunk url is BUCKET_URL + a key as listed.
    return ObjectStoreRegistry({BUCKET_URL: source_store()})


def child_prefixes(prefix: str) -> list[str]:
    listing = source_store().list_with_delimiter(prefix=prefix)
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in listing["common_prefixes"])


def available_cycles() -> list[str]:
    """Every cycle currently in the bucket, oldest first. Keys must be listed, never constructed."""
    cycles = []
    # Prefixes are relative to the store root, which already carries ROOT_PREFIX.
    for year in child_prefixes(""):
        for month in child_prefixes(f"{year}/"):
            for day in child_prefixes(f"{year}/{month}/"):
                cycles.extend(f"{year}/{month}/{day}/{stamp}" for stamp in child_prefixes(f"{year}/{month}/{day}/"))
    return sorted(cycles, key=cycle_index)


def single_level_count(diagnostics: Sequence[Diagnostic]) -> int:
    return sum(len(d.files) for d in diagnostics if not d.is_level)


def list_cycle(cycle: str) -> list[Diagnostic]:
    pages = source_store().list(prefix=f"{cycle}/")  # Key is relative to the store root
    files = sorted(
        filter(None, (parse_key(obj["path"]) for page in pages for obj in page)),
        key=lambda f: (f.slug, f.lead_minutes),
    )
    return [Diagnostic(slug, tuple(group)) for slug, group in groupby(files, key=lambda f: f.slug)]


def open_virtual(key: str, loadable: Sequence[str] | None) -> xr.Dataset:
    return open_virtual_dataset(
        url=BUCKET_URL + key,
        parser=HDFParser(),
        registry=registry(),
        loadable_variables=None if loadable is None else list(loadable),
    )


def is_per_lead(name: str, var: xr.Variable) -> bool:
    return name in PER_LEAD or var.dims == ("bnds",)


def as_lead_slice(vds: xr.Dataset) -> xr.Dataset:
    per_lead = {name: var for name, var in vds.variables.items() if is_per_lead(name, var)}
    main = {name for name, var in vds.data_vars.items() if var.ndim >= 3}
    static_small = [name for name in vds.data_vars if name not in main and name not in per_lead]
    init = vds[INIT_DIM].values
    lead = int(vds[LEAD_DIM].values)
    expanded = {
        name: ((INIT_DIM, LEAD_DIM, *var.dims), var.values[None, None, ...], var.attrs)
        for name, var in per_lead.items()
        if name != LEAD_DIM
    }
    out = (
        vds.drop_vars([*per_lead, INIT_DIM])
        .set_coords(static_small)
        .expand_dims({INIT_DIM: [init], LEAD_DIM: [lead]})
        .assign_coords(expanded)
    )
    out[LEAD_DIM].attrs = vds[LEAD_DIM].attrs
    out[INIT_DIM].attrs = vds[INIT_DIM].attrs
    return out


def inspect_first(key: str) -> Inspection:
    probe = open_virtual(key, None)
    loadable = tuple(name for name, var in probe.variables.items() if var.ndim < 3)
    return Inspection(loadable=loadable, first=as_lead_slice(open_virtual(key, loadable)))


def parse_one(args: tuple[str, tuple[str, ...]]) -> xr.Dataset:
    key, loadable = args
    return as_lead_slice(open_virtual(key, loadable))


def concat_leads(slices: Sequence[xr.Dataset]) -> xr.Dataset:
    return xr.concat(
        slices,
        dim=LEAD_DIM,
        data_vars="minimal",
        coords="minimal",
        compat="override",
        join="exact",
        combine_attrs="override",
    )


def shared_coords(ds: xr.Dataset) -> dict[str, xr.DataArray]:
    return {name: coord for name, coord in ds.coords.items() if coord.ndim >= 1 and INIT_DIM not in coord.dims}


def level_dims(ds: xr.Dataset) -> list[str]:
    return sorted(d for d in ds.dims if d not in GRID_DIMS and d not in (INIT_DIM, LEAD_DIM, "bnds"))


def grid_label(ds: xr.Dataset) -> str:
    levels = "".join(f" {d}={ds.sizes[d]}" for d in level_dims(ds))
    return f"{ds.sizes['latitude']}x{ds.sizes['longitude']} members={ds.sizes['realization']}{levels}"


def signature(diagnostic: Diagnostic, first: xr.Dataset) -> str:
    digest = hashlib.sha1()
    digest.update(repr(diagnostic.schedule).encode())
    digest.update(diagnostic.window.encode())
    for name, coord in sorted(shared_coords(first).items()):
        digest.update(name.encode())
        digest.update(repr(coord.dims).encode())
        digest.update(coord.values.tobytes())
    return digest.hexdigest()[:10]


def base_group_name(diagnostic: Diagnostic, first: xr.Dataset) -> str:
    levels = level_dims(first)
    base = "surface" if not levels else "_".join(f"{d}_levels" for d in levels)
    return f"{base}_{diagnostic.window}" if diagnostic.window else base


def place(diagnostics: Sequence[Diagnostic], inspections: Sequence[Inspection]) -> list[Placement]:
    sigs = [signature(d, i.first) for d, i in zip(diagnostics, inspections)]
    bases = [base_group_name(d, i.first) for d, i in zip(diagnostics, inspections)]
    sig_count_per_base = {base: len({s for s, b in zip(sigs, bases) if b == base}) for base in set(bases)}
    majority_sig = {base: Counter(s for s, b in zip(sigs, bases) if b == base).most_common(1)[0][0] for base in set(bases)}
    candidates = {
        s: b if sig_count_per_base[b] == 1 or s == majority_sig[b] else f"{b}_{len(d.schedule)}leads"
        for d, s, b in zip(diagnostics, sigs, bases)
    }
    owners = Counter(candidates.values())
    names = {s: c if owners[c] == 1 else f"{c}_{s[:6]}" for s, c in candidates.items()}
    return [
        Placement(diagnostic=d, loadable=i.loadable, signature=s, group=names[s], grid=grid_label(i.first))
        for d, i, s in zip(diagnostics, inspections, sigs)
    ]


def demote_scalars(ds: xr.Dataset, targets: Iterable[str]) -> xr.Dataset:
    scalars = {
        name: coord for name, coord in ds.coords.items() if coord.ndim == 0 and name != INIT_DIM and "grid_mapping_name" not in coord.attrs
    }
    out = ds.drop_vars(list(scalars))
    for target in targets:
        for name, coord in scalars.items():
            out[target].attrs[name] = coord.values.item()
            if "units" in coord.attrs:
                out[target].attrs[f"{name}_units"] = coord.attrs["units"]
    return out


def pin_time_encoding(ds: xr.Dataset) -> xr.Dataset:
    """Force every datetime coordinate onto an absolute epoch, and keep 1-D coordinates contiguous.

    Without the pinned units xarray picks them from the seed cycle, and later cycles either round
    away or get re-encoded against units the stored attrs no longer describe. Without the chunk
    override the init coordinate is created one element per chunk.
    """
    for name, var in ds.variables.items():
        encoding = dict(ds[name].encoding)
        if np.issubdtype(var.dtype, np.datetime64):
            encoding |= {"units": CF_UNITS, "calendar": CF_CALENDAR, "dtype": "int64"}
        if var.ndim == 1:
            size = COORD_INIT_CHUNK if var.dims[0] == INIT_DIM else var.sizes[var.dims[0]]
            encoding["chunks"] = (size,)
        ds[name].encoding = encoding
    return ds


def finalize(ds: xr.Dataset, diagnostic: Diagnostic) -> xr.Dataset:
    main = [name for name, var in ds.data_vars.items() if var.ndim >= 5]
    renames = {name: diagnostic.name if len(main) == 1 else f"{diagnostic.name}__{name}" for name in main}
    for name in main:
        ds[name].attrs["source_variable"] = name
        ds[name].attrs["source_diagnostic"] = diagnostic.slug
    return pin_time_encoding(demote_scalars(ds, main).rename(renames))


def build_variable(pool: Executor, placement: Placement, leads: int | None) -> tuple[xr.Dataset, dict[str, float]]:
    """Returns the dataset and a phase breakdown, since parse, concat and commit costs differ wildly.

    concat runs single-threaded in the driver over one ManifestArray per lead time, so it does not
    benefit from the pool at all -- worth watching as the thing that bounds a cycle.
    """
    files = placement.diagnostic.files[:leads] if leads else placement.diagnostic.files
    t0 = time.perf_counter()
    slices = list(pool.map(parse_one, ((f.key, placement.loadable) for f in files)))
    t1 = time.perf_counter()
    out = finalize(concat_leads(slices), placement.diagnostic)
    t2 = time.perf_counter()
    return out, {"parse": t1 - t0, "concat": t2 - t1}


def root_attrs(cycle: str, origin: int, slots: int) -> dict[str, object]:
    return {
        "title": "MOGREPS-G Model Forecast on Global 20 km Standard Grid (virtualized)",
        "source": BUCKET_URL,  # BUCKET_URL already ends with ROOT_PREFIX and a slash
        "institution": "Met Office",
        "license": "British Crown copyright, Met Office, CC BY-SA 4.0",
        "registry": "https://registry.opendata.aws/met-office-global-ensemble/",
        "layout": (
            "diagnostics that share a lead-time schedule, grid, member list and level axis are merged into one group; "
            "dims forecast_reference_time, forecast_period, realization, [level,] latitude, longitude"
        ),
        "rolling_window": (
            "forecast_reference_time is append-only and always sorted: cycle k occupies slot "
            "k - origin_cycle_index, with no wrapping. The axis is pre-populated, so a slot whose data "
            "has not been ingested (or whose source has aged out of the Met Office bucket) reads as "
            "fill value; the status group says which is which."
        ),
        "origin_cycle_index": origin,
        "origin_cycle": ORIGIN_CYCLE,
        "initial_window_slots": slots,
        "first_cycle": cycle,
        "note": "chunk manifests only; source files expire from the Met Office bucket after ~31 days",
    }


# How many init times share one manifest shard for the big data arrays. 1 means a shard per cycle,
# which is what the archive was first built with; measured at 127 slots that produced 10,287 data
# shards of 54k-246k refs (1-3.9 MB each) -- healthy sizes, but a lot of objects. 4 packs a day per
# shard, cutting object count 4x at ~1M refs / ~13 MB, still inside the band icechunk documents.
#
# The cost is write amplification in steady state: a shard spanning 4 cycles is rewritten as each of
# those cycles lands, so three superseded versions accumulate before it goes immutable. Backfill can
# avoid that entirely by committing a whole day at once; the 6-hourly cron cannot, so it leans on
# regular garbage collection to clear the superseded copies.
DATA_SPLIT_CYCLES = 4

# Arrays that must never be split. Splitting keys off chunk index, not size, so a coordinate or a
# status array -- one chunk per init time -- yields a shard holding a single reference. At 127 slots
# that was 13,147 shards of ~200 bytes, 56% of every manifest in the repo, for 0.003 GB of content.
# Left unsplit they are one manifest each: ~103 objects instead of ~13,100.
UNSPLIT_NAMES = r"^(forecast_reference_time|forecast_period|time|realization|latitude|longitude|latitude_longitude|.*_bnds)$"


def manifest_config(commit_fetches: int = 32) -> ic.ManifestConfig:
    """Split only the big data arrays, and only as coarsely as they need.

    Rules are evaluated in order and the first match wins, so the no-split rule for coordinates and
    the status subgroup has to come first. An empty dim mapping means "do not split this array".
    """
    dont_split = ic.ManifestSplitCondition.or_conditions(
        [
            # every array under any `status` subgroup, whatever the group is called
            ic.ManifestSplitCondition.path_matches(r"/status/"),
            # coordinates and bounds sitting beside the data
            ic.ManifestSplitCondition.name_matches(UNSPLIT_NAMES),
        ]
    )
    splitting = ic.ManifestSplittingConfig.from_dict(
        {
            dont_split: {},
            ic.ManifestSplitCondition.AnyArray(): {
                ic.ManifestSplitDimCondition.DimensionName(f"^{INIT_DIM}$"): DATA_SPLIT_CYCLES
            },
        }
    )
    return ic.ManifestConfig(
        splitting=splitting,
        max_concurrent_manifest_fetches_during_commit=commit_fetches,  # default 1 updates shards serially
        preload=ic.ManifestPreloadConfig(max_arrays_to_scan=200),  # default 50 < our array count
    )


def group_attrs(placements: Sequence[Placement]) -> dict[str, object]:
    first = placements[0]
    return {
        "lead_time_minutes": list(first.diagnostic.schedule),
        "window": first.diagnostic.window or "instantaneous",
        "grid": first.grid,
        "coordinate_signature": first.signature,
        "diagnostics": [p.diagnostic.slug for p in placements],
    }


def existing_arrays(repo: ic.Repository) -> dict[str, set[str]] | None:
    try:
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except zarr.errors.GroupNotFoundError:
        return None
    return {name: set(group.array_keys()) for name, group in root.groups()}


def archive_origin(repo: ic.Repository) -> int:
    return int(zarr.open_group(repo.readonly_session("main").store, mode="r").attrs["origin_cycle_index"])


def written_cycle(repo: ic.Repository, cycle: str) -> bool:
    """Has this cycle already been committed?

    One commit per init time makes this a yes/no rather than a per-variable tally: a cycle is either
    entirely present or entirely absent, so a retry either skips or redoes the whole slot.
    """
    try:
        ancestry = repo.ancestry(branch="main")
    except Exception:  # noqa: BLE001
        return False
    return any((s.metadata or {}).get("cycle") == cycle for s in ancestry)


def write_root(repo: ic.Repository, cycle: str, origin: int, slots: int) -> None:
    session = repo.writable_session("main")
    # Replace rather than merge: a --fresh reset keeps the old root-attributes snapshot, so merging
    # would leave a repo advertising both this layout and whatever preceded it.
    zarr.open_group(session.store, mode="a").attrs.put(root_attrs(cycle, origin, slots))
    session.commit(f"MOGREPS-G {cycle}: {ROOT_MESSAGE_SUFFIX}")


def init_axis_arrays(group: zarr.Group) -> Iterable[zarr.Array]:
    for _, array in group.arrays():
        names = array.metadata.dimension_names
        if names and names[0] == INIT_DIM:
            yield array


def on_init_axis(ds: xr.Dataset) -> xr.Dataset:
    """Drop everything without the init axis.

    A region write rejects variables that share no dimension with the region. The ones that fall out
    here are the static coordinates (latitude, longitude, realization, forecast_period, bounds, grid
    mapping); they are written once at slot 0 and are byte-identical across cycles by construction,
    since `signature` is what decides two diagnostics may share a group in the first place.
    """
    return ds.drop_vars([name for name, var in ds.variables.items() if INIT_DIM not in var.dims])


def status_group_path(group_name: str) -> str:
    return f"{group_name}/{STATUS_GROUP}"


def decode_init_coord(session: ic.Session, group_name: str) -> np.ndarray:
    """The init coordinate of a group as datetime64, decoded from its own CF attrs."""
    from xarray.coding.times import decode_cf_datetime

    array = zarr.open_array(session.store, path=f"{group_name}/{INIT_DIM}", mode="r")
    return decode_cf_datetime(array[:], array.attrs.get("units", CF_UNITS), array.attrs.get("calendar", CF_CALENDAR))


def ensure_status(session: ic.Session, group_name: str, name: str, n_init: int, n_leads: int) -> zarr.Array:
    """The status array for one variable, created on first use and grown with the ring.

    Chunked one row per init time so a slot's status is a single chunk, matching how the data
    manifests are split.
    """
    group = zarr.open_group(session.store, path=status_group_path(group_name), mode="a")
    if name in group:
        array = group[name]
        if array.shape[0] < n_init:
            array.resize((n_init, array.shape[1]))
        return array
    array = group.create_array(
        name,
        shape=(n_init, n_leads),
        chunks=(1, n_leads),
        dtype="uint8",
        fill_value=STATUS_PENDING,
        dimension_names=(INIT_DIM, LEAD_DIM),
    )
    array.attrs.update(STATUS_ATTRS)
    return array


def ensure_status_coords(session: ic.Session, group_name: str, leads: Sequence[int], n_init: int) -> None:
    """Mirror the two coordinates into the status group so it opens standalone in xarray."""
    group = zarr.open_group(session.store, path=status_group_path(group_name), mode="a")
    if LEAD_DIM not in group:
        lead = group.create_array(LEAD_DIM, shape=(len(leads),), chunks=(len(leads),), dtype="int64", dimension_names=(LEAD_DIM,))
        lead[:] = np.array(leads, dtype="int64") * 60  # schedule is in minutes, coordinate is seconds
        lead.attrs.update({"units": "seconds", "standard_name": LEAD_DIM})
    if INIT_DIM not in group:
        init = group.create_array(INIT_DIM, shape=(n_init,), chunks=(n_init,), dtype="int64", dimension_names=(INIT_DIM,))
        init.attrs.update({"units": CF_UNITS, "calendar": CF_CALENDAR, "standard_name": INIT_DIM})
    elif group[INIT_DIM].shape[0] < n_init:
        group[INIT_DIM].resize((n_init,))


def write_init_coord(session: ic.Session, group_name: str, slot: int, when: datetime) -> None:
    """Write forecast_reference_time into its slot directly.

    xarray drops index coordinates on a region write ("can't modify indexes with region writes"), so
    left to `to_icechunk` the ring's own coordinate would silently never advance.
    """
    array = zarr.open_array(session.store, path=f"{group_name}/{INIT_DIM}", mode="a")
    units = array.attrs.get("units", CF_UNITS)
    calendar = array.attrs.get("calendar", CF_CALENDAR)
    stamp = np.array([np.datetime64(when.replace(tzinfo=None), "us")])
    encoded, used, _ = encode_cf_datetime(stamp, units, calendar, dtype=np.dtype(array.dtype))
    if used != units:
        # encode_cf_datetime silently falls back to finer units when the requested ones cannot
        # represent the value, which would store a number the array's own attrs then misdecode.
        raise SystemExit(f"{group_name}/{INIT_DIM} stores units {units!r} but {when:%Y-%m-%dT%H%M} needs {used!r}")
    array[slot] = encoded[0]


def array_init_size(repo: ic.Repository, group: str, name: str) -> int | None:
    try:
        array = zarr.open_array(repo.readonly_session("main").store, path=f"{group}/{name}", mode="r")
    except (zarr.errors.ArrayNotFoundError, KeyError, FileNotFoundError):
        return None
    return array.shape[0]


def mark_status(session: ic.Session, placement: Placement, ds: xr.Dataset, slot: int, cycle: str, value: int) -> None:
    """Record that this variable's slot is populated, and mirror the init time into the status group."""
    n_init = zarr.open_array(session.store, path=f"{placement.group}/{placement.diagnostic.name}", mode="r").shape[0]
    n_leads = ds.sizes[LEAD_DIM]
    ensure_status_coords(session, placement.group, placement.diagnostic.schedule[:n_leads], n_init)
    ensure_status(session, placement.group, placement.diagnostic.name, n_init, n_leads)[slot, :] = value
    write_init_coord(session, status_group_path(placement.group), slot, cycle_time(cycle))


def write_all_init_coords(session: ic.Session, group_name: str, origin: int, slots: int) -> None:
    """Lay out the whole init axis at once, sorted, before any data lands.

    Pre-populating is what makes the axis readable during a long ingest: every slot already has its
    true forecast_reference_time, and ingest only fills data in underneath. Without this the axis
    would grow one entry at a time and a reader would see it change shape under them.
    """
    array = zarr.open_array(session.store, path=f"{group_name}/{INIT_DIM}", mode="a")
    if array.shape[0] < slots:
        array.resize((slots,))
    # Always lay out the array's full current length, not just the requested count: callers ask for
    # "at least this many" and the array may already be longer, in which case a short write does not
    # line up with `array[:]` and numpy refuses to broadcast.
    slots = array.shape[0]
    units = array.attrs.get("units", CF_UNITS)
    calendar = array.attrs.get("calendar", CF_CALENDAR)
    stamps = np.array([np.datetime64(cycle_at(origin, s).replace(tzinfo=None), "us") for s in range(slots)])
    encoded, used, _ = encode_cf_datetime(stamps, units, calendar, dtype=np.dtype(array.dtype))
    if used != units:
        raise SystemExit(f"{group_name}/{INIT_DIM} stores units {units!r} but the window needs {used!r}")
    array[:] = encoded


def ensure_capacity(session: ic.Session, group_name: str, slot: int, origin: int) -> None:
    """Grow the axis if a cycle lands beyond the pre-populated horizon.

    Only happens in steady state, once the archive outlives the window it was created with. Growth is
    one slot at a time and strictly at the end, so the axis stays sorted.
    """
    group = zarr.open_group(session.store, path=group_name, mode="a")
    arrays = [*init_axis_arrays(group)]
    if STATUS_GROUP in group:
        arrays += [*init_axis_arrays(group[STATUS_GROUP])]
    for array in arrays:
        if array.shape[0] <= slot:
            array.resize((slot + 1, *array.shape[1:]))
    for path in (group_name, status_group_path(group_name)):
        write_all_init_coords(session, path, origin, slot + 1)


def expire_slot(session: ic.Session, group_name: str, slot: int) -> int:
    """Free an aged-out slot: drop its chunks, keep its coordinate, flag it unavailable.

    Deleting the chunk prefix takes the manifest shard with it (manifests are split per init time),
    which is what stops an append-only axis from accumulating dead references for ever. The
    coordinate entry stays so the axis remains monotonic, and the slot reads as fill value.
    """
    from zarr.core.sync import sync

    group = zarr.open_group(session.store, path=group_name, mode="a")
    status = group[STATUS_GROUP] if STATUS_GROUP in group else None  # noqa: SIM401
    freed = 0
    for name, array in group.arrays():
        names = array.metadata.dimension_names
        # Only the data variables. Coordinates share the init axis but must survive: deleting
        # forecast_reference_time's chunks would blow a hole in the very axis this layout exists to
        # keep sorted, and `time` is what makes an expired slot still describable.
        if not names or names[0] != INIT_DIM or array.ndim < 3 or slot >= array.shape[0]:
            continue
        sync(session.store.delete_dir(f"{group_name}/{name}/c/{slot}"))
        freed += 1
        if status is not None and name in status and status[name].ndim == 2:
            status[name][slot, :] = STATUS_UNAVAILABLE
    return freed


def expire_before(repo: ic.Repository, oldest_kept: datetime) -> int:
    """Expire every slot whose cycle is older than the Met Office retention window."""
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    cutoff = np.datetime64(oldest_kept.replace(tzinfo=None), "ns")
    freed = 0
    for group_name, _ in root.groups():
        times = decode_init_coord(session, group_name)
        for slot, when in enumerate(times):
            if not np.isnat(when) and when < cutoff:
                freed += expire_slot(session, group_name, slot)
    if freed:
        session.commit(f"expire {freed} arrays with sources older than {oldest_kept:%Y-%m-%d %H:%M}Z")
    return freed


def advertise_fill_value(session: ic.Session, group_name: str, name: str) -> None:
    """Publish the array's fill value as `_FillValue` so xarray masks empty slots.

    The fill inherited from the source HDF5 files is 9.96921e+36, the CF default. Without the
    attribute xarray hands that back as an ordinary float, so a slot that was never ingested (or whose
    source has aged out) silently poisons a mean or a plot instead of reading as NaN.
    """
    array = zarr.open_array(session.store, path=f"{group_name}/{name}", mode="a")
    if "_FillValue" not in array.attrs and array.fill_value is not None:
        # Zarr v3 wants this base64-encoded rather than a bare float; use xarray's own coder so the
        # value round-trips through the reader that will decode it.
        array.attrs["_FillValue"] = FillValueCoder.encode(float(array.fill_value), array.dtype)


def write_variable(
    session: ic.Session, placement: Placement, members: Sequence[Placement], ds: xr.Dataset, cycle: str, slot: int
) -> None:
    """Write one variable into its slot, without committing.

    The caller commits once for the whole cycle, so an init time is all-or-nothing: no reader ever
    sees a slot with half its variables. Deliberately avoiding `append_dim`: virtualizarr appends
    loadable coordinates too, so 50 variables sharing a group would each append to the init axis.
    A region write of the same coordinate values is idempotent instead.
    """
    checksum = datetime.now(UTC)
    if zarr.open_group(session.store, path=placement.group, mode="a").get(placement.diagnostic.name) is None:
        ds.vz.to_icechunk(session.store, group=placement.group, mode="a", last_updated_at=checksum)
    else:
        on_init_axis(ds).vz.to_icechunk(
            session.store, group=placement.group, region={INIT_DIM: slice(slot, slot + 1)}, last_updated_at=checksum
        )
    advertise_fill_value(session, placement.group, placement.diagnostic.name)
    mark_status(session, placement, ds, slot, cycle, STATUS_VALID)
    zarr.open_group(session.store, path=placement.group, mode="a").attrs.update(group_attrs(members))


def reset_to_root(repo: ic.Repository) -> None:
    snapshots = [s for s in repo.ancestry(branch="main") if s.message.endswith(ROOT_MESSAGE_SUFFIX)]
    if not snapshots:
        raise SystemExit("no root-attributes snapshot on main; refusing to reset")
    repo.reset_branch("main", snapshots[-1].id)
    print(f"reset main to {snapshots[-1].id}", flush=True)


def read_probe(repo: ic.Repository, group: str, name: str, slot: int) -> str:
    ds = xr.open_zarr(repo.readonly_session("main").store, group=group, consolidated=False, zarr_format=3)
    started = time.perf_counter()
    sample = ds[name].isel({INIT_DIM: slot, LEAD_DIM: 0, "realization": 0}).isel(latitude=slice(0, 128), longitude=slice(0, 128)).load()
    when = str(ds[INIT_DIM].values[slot])[:16]
    return (
        f"{group}/{name}: {ds[name].shape}, {len(ds.data_vars)} variables in group, slot {slot} holds {when}, "
        f"one chunk read in {time.perf_counter() - started:.2f}s, mean {float(sample.mean()):.3f}"
    )


def dry_run_repo(path: str | None = None, caching: ic.CachingConfig | None = None) -> ic.Repository:
    config = ic.RepositoryConfig.default()
    config.manifest = manifest_config()
    if caching is not None:  # Chunk cache for readers
        config.caching = caching
    # The virtual container must match the source root, local or S3.
    if BUCKET_URL.startswith("file://"):
        config.set_virtual_chunk_container(ic.VirtualChunkContainer(BUCKET_URL, ic.local_filesystem_store(BUCKET_NAME)))
        credentials = ic.credentials.LocalFileSystemAccess  # a None credential is deprecated in icechunk 2.2.2
    else:
        config.set_virtual_chunk_container(ic.VirtualChunkContainer(BUCKET_URL, ic.s3_store(region=REGION)))
        credentials = ic.Credentials.S3(ic.S3Credentials.FromEnv())
    storage = ic.local_filesystem_storage(path) if path else ic.in_memory_storage()
    return ic.Repository.open_or_create(storage, config=config, authorize_virtual_chunk_access={BUCKET_URL: credentials})


def prod_repo(org: str, repo_name: str, nickname: str, storage_nickname: str | None, cycle: str, config: ic.RepositoryConfig | None = None) -> ic.Repository:
    from arraylake import Client

    client = Client(service_uri=PROD_URI)
    # Bucket configs come from scripts/aws/setup_arraylake_storage.sh (decision 013).
    nicknames = {b.nickname for b in client.list_bucket_configs(org)}
    if nickname not in nicknames:
        raise SystemExit(f"bucket config {nickname} missing on org {org}; run scripts/aws/setup_arraylake_storage.sh")
    full_name = f"{org}/{repo_name}"
    if repo_name in {r.name for r in client.list_repos(org)}:
        # The grant is per-open, not stored, so an existing repo needs it as well.
        # `config` (chunk cache) merges over the stored config; manifest splitting and containers persist.
        return client.get_repo(full_name, config=config, authorize_virtual_chunk_access={BUCKET_URL: nickname})
    config = ic.RepositoryConfig.default()
    config.manifest = manifest_config()
    repo = client.create_repo(
        full_name,
        bucket_config_nickname=storage_nickname,
        description="Met Office MOGREPS-G global ensemble, rolling 30-day archive, virtualized from AWS Open Data",
        metadata={"type": ["weather", "ensemble", "virtual"], "source": [BUCKET_URL]},
        config=config,
        authorize_virtual_chunk_access={BUCKET_URL: nickname},
    )
    repo.save_config()  # persist splitting so every later writer and reader inherits it
    print(f"created repo {full_name} with manifest splitting on {INIT_DIM}", flush=True)
    return repo


def select(diagnostics: Sequence[Diagnostic], only: Sequence[str], include_levels: bool, limit: int | None) -> list[Diagnostic]:
    wanted = [d for d in diagnostics if (not only or d.slug in only) and (include_levels or not d.is_level)]
    return wanted[:limit] if limit else wanted


# Default log flushes, so per-variable progress is visible under nohup. A module-level
# singleton, because ruff rejects a call in an argument default (B008).
_FLUSHING_PRINT = functools.partial(print, flush=True)


# The plan goes through the caller's log hook, like the rest of run_with_repo.
def print_plan(placements: Sequence[Placement], log: Callable[[str], None] = _FLUSHING_PRINT) -> None:
    by_group = groupby(sorted(placements, key=lambda p: (p.group, p.diagnostic.slug)), key=lambda p: p.group)
    for group, members in by_group:
        members = list(members)
        first = members[0]
        log(
            f"{group}: {len(members)} diagnostics, {len(first.diagnostic.schedule)} leads, {first.grid}, window={first.diagnostic.window or 'none'}"
        )
        for p in members:
            log(f"    {p.diagnostic.slug}")


def resolve_cycle(requested: str | None) -> str:
    """Newest cycle whose single-level files have all landed. There is no completion marker."""
    if requested:
        return requested
    for cycle in reversed(available_cycles()):
        count = single_level_count(list_cycle(cycle))
        if count == SINGLE_LEVEL_FILES:
            return cycle
        print(f"{cycle}: {count}/{SINGLE_LEVEL_FILES} single-level files, still landing", flush=True)
    raise SystemExit("no complete cycle in the bucket")


def run(args: argparse.Namespace) -> int:
    # Repo choice moved out of the body so a driver can supply its own repo.
    # `--plan` returns before the repo is used, so it needs none.
    if args.plan:
        repo = None
    elif args.dry_run:
        repo = dry_run_repo(args.dry_run_path)
    else:
        # The repo is created before the completeness gate, so an incomplete production cycle
        # can leave an empty repo behind; the next run seeds it normally.
        repo = prod_repo(args.org, args.repo, args.bucket_nickname, args.storage_nickname, resolve_cycle(args.cycle))
    return run_with_repo(repo, args)


# The body of run(), with the repo injected and every print routed through `log`.
def run_with_repo(repo: ic.Repository | None, args: argparse.Namespace, log: Callable[[str], None] = _FLUSHING_PRINT) -> int:
    started = time.perf_counter()
    cycle = resolve_cycle(args.cycle)
    listing = list_cycle(cycle)
    count = single_level_count(listing)
    if count != SINGLE_LEVEL_FILES and not args.allow_incomplete:
        raise SystemExit(f"cycle {cycle}: {count} single-level files, want {SINGLE_LEVEL_FILES}; rerun later or pass --allow-incomplete")
    diagnostics = select(listing, args.variables, args.include_levels, args.limit)
    total_files = sum(min(len(d.files), args.leads or sys.maxsize) for d in diagnostics)
    log(f"cycle {cycle}: {len(diagnostics)} diagnostics selected, {total_files} files")
    # spawn, not fork. By this point `list_cycle` has built the obstore client and its tokio runtime;
    # forking a process with live runtime threads deadlocks the workers. macOS spawns by default so
    # this only bites on Linux, which is where the scheduled job runs.
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        inspections = list(pool.map(inspect_first, (d.files[0].key for d in diagnostics)))
        placements = place(diagnostics, inspections)
        print_plan(placements, log)
        if args.plan:
            return 0
        if args.fresh and not args.dry_run:
            reset_to_root(repo)
        # ORIGIN_CYCLE is read in the parent only; spawned workers never look at it.
        origin = cycle_index(ORIGIN_CYCLE)
        # Seed when there are no arrays, or when the root predates this layout. The second case is what
        # a `--fresh` reset leaves behind: the root-attributes snapshot survives the reset, so a repo
        # built under the old ring layout still carries its attrs and must be re-stamped. A brand new
        # repo has no root group at all, so its attrs cannot be read to make that comparison.
        present = existing_arrays(repo)
        if present is None:
            seeding = True
        else:
            root_now = zarr.open_group(repo.readonly_session("main").store, mode="r").attrs
            seeding = not present or "origin_cycle_index" not in root_now
        if not seeding:
            # The stored origin rules. A different ORIGIN_CYCLE would shift every slot silently.
            stored = archive_origin(repo)
            if stored != origin:
                raise SystemExit(f"repo origin_cycle_index is {stored}, but ORIGIN_CYCLE {ORIGIN_CYCLE} gives {origin}")
            origin = stored
        slot = slot_of(cycle, origin)
        if seeding:
            if slot != 0:
                raise SystemExit(f"repo is empty, so the first ingest must be the origin cycle {ORIGIN_CYCLE}, not {cycle}")
            if not args.dry_run and (args.limit or args.leads or args.variables):
                # The cycle that seeds a repo fixes the shape of every shared coordinate, so seeding
                # with a truncated selection bakes in the wrong forecast_period and every later full
                # cycle collides with it. Cheap mistake to make, expensive to notice.
                raise SystemExit(
                    "refusing to seed a real repo from a truncated cycle: drop --limit/--leads/--variables, "
                    "or seed against --dry-run first"
                )
            write_root(repo, cycle, origin, window_slots(origin, args.horizon_hours))
        slots = int(zarr.open_group(repo.readonly_session("main").store, mode="r").attrs["initial_window_slots"])

        if written_cycle(repo, cycle):
            log(f"cycle {cycle} already committed at slot {slot}; nothing to do")
            return 0
        todo = sorted(placements, key=lambda p: (p.group, p.diagnostic.slug))
        log(f"slot {slot} of {slots}: building {len(todo)} variables with {args.workers} workers")

        # One session for the whole cycle, committed once at the end: an init time lands all-or-nothing,
        # so no reader ever sees a slot with half its variables.
        session = repo.writable_session("main")
        members_by_group = {g: list(ms) for g, ms in groupby(sorted(placements, key=lambda p: p.group), key=lambda p: p.group)}
        # Grow the axis *before* writing when a cycle lands past the pre-populated horizon. A region
        # write into a slot the array does not have yet resolves to a zero-length region and fails
        # with a dimension-size mismatch rather than anything that names the real problem. Growth has
        # to wait until after the bootstrap write on the seeding cycle, where the arrays do not exist
        # yet -- hence the second call further down, which also lays out the full window.
        if not seeding:
            for group_name in members_by_group:
                ensure_capacity(session, group_name, slot, origin)
        for index, placement in enumerate(todo, 1):
            t0 = time.perf_counter()
            ds, phases = build_variable(pool, placement, args.leads)
            t_write = time.perf_counter()
            write_variable(session, placement, members_by_group[placement.group], ds, cycle, slot)
            n = min(len(placement.diagnostic.files), args.leads or sys.maxsize)
            log(
                f"[{index}/{len(todo)}] {placement.group}/{placement.diagnostic.name}: {n} files "
                f"in {time.perf_counter() - t0:.0f}s (parse {phases['parse']:.0f}s, concat {phases['concat']:.0f}s, "
                f"write {time.perf_counter() - t_write:.0f}s)",
            )
        # Lay the axis out only after the arrays exist; on the seeding cycle they are created at size 1
        # by the bootstrap write above, so this is what stretches them to the full pre-populated window.
        for group_name in members_by_group:
            ensure_capacity(session, group_name, max(slot, slots - 1), origin)
        t_commit = time.perf_counter()
        snapshot = session.commit(
            f"MOGREPS-G {cycle}: {len(todo)} variables -> slot {slot}",
            metadata={"cycle": cycle, "slot": slot, "variables": len(todo)},
        )
        log(f"committed slot {slot} as {snapshot} in {time.perf_counter() - t_commit:.0f}s")
    if todo:
        log(read_probe(repo, todo[0].group, todo[0].diagnostic.name, slot))
    log(f"finished in {(time.perf_counter() - started) / 60:.1f} min")
    return 0


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cycle", default=None, help="YYYY/MM/DD/THHMMZ, e.g. 2026/09/10/T0000Z; default is the newest complete cycle")
    parser.add_argument("--org", default="metoffice")
    parser.add_argument("--repo", default="mogreps-g")
    parser.add_argument("--bucket-nickname", default="met-office-global-ensemble", help="bucket config for the virtual chunk source")
    parser.add_argument("--storage-nickname", default=None, help="bucket config the repo itself is stored in; default is the org default")
    parser.add_argument("--allow-incomplete", action="store_true", help="ingest a cycle whose files are still landing")
    parser.add_argument("--horizon-hours", type=int, default=40, help="pre-populate the init axis this far past now, at repo creation")
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--variables", type=lambda s: s.split(","), default=[], help="comma-separated diagnostic slugs; default all")
    parser.add_argument("--include-levels", action="store_true", help="also build the pressure/height/soil level diagnostics")
    parser.add_argument("--limit", type=int, default=None, help="build at most this many diagnostics")
    parser.add_argument("--leads", type=int, default=None, help="use at most this many lead times per diagnostic")
    parser.add_argument("--plan", action="store_true", help="print the group assignment and exit")
    parser.add_argument("--dry-run", action="store_true", help="write to a local repo instead of Arraylake")
    parser.add_argument("--dry-run-path", default=None, help="persist the dry-run repo here so several cycles can be ingested in sequence")
    parser.add_argument("--fresh", action="store_true", help="reset main to the root-attributes snapshot before building")
    return parser.parse_args(argv)

