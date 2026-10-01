"""Array layouts for the two native Icechunk repos: `ts` (GEFS pattern) and `dl` (Anemoi pattern).

Both are xarray templates: an empty init axis written once, then grown one slab per cycle and
filled by region writes. Both are pcodec-encoded and sharded; decision 011 holds the sizes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import dask.array as dsa
import numpy as np
import xarray as xr
from zarr.codecs.numcodecs import PCodec

# 171 lead times: hourly to T+132, then 3-hourly to T+246. Minutes, as the source keys give them.
LEAD_MINUTES: tuple[int, ...] = tuple(range(0, 7921, 60)) + tuple(range(8100, 14761, 180))
PCODEC_LEVEL = 8

# `ts`: one array per variable, dims (init, member, lead, lat, lon).
# The extents divide the full grid (171 = 3 x 57, 960 = 60 x 16, 1280 = 80 x 16), so a shard write
# is always a complete-shard write and Zarr never does a read-modify-write.
TS_CHUNK = (1, 18, 57, 16, 16)
TS_SHARD = (1, 18, 171, 320, 320)
TS_DIMS = ("init", "member", "lead", "lat", "lon")
# `dl`: one `data` array, dims (init, lead, variable, member, lat, lon); one shard per sample.
# At 81 variables a shard is ~7.2 GB of 1296 inner chunks, and a sample write is a whole shard.
DL_CHUNK = (1, 1, 1, 18, 240, 320)
DL_SHARD = (1, 1, 0, 18, 960, 1280)  # the variable entry is replaced by the variable count
DL_DIMS = ("init", "lead", "variable", "member", "lat", "lon")
# Both layouts keep Zarr's default Morton sub-chunk order (decision 011): it is the only order
# Zarr persists, and an aligned power-of-two variable group stays contiguous inside a shard.
# Stats arrays, dims (init, lead, variable): one chunk per sample, so each lead unit writes alone.
STATS_CHUNK = (1, 1, 0)
STATS_DIMS = ("init", "lead", "variable")
STATS = {
    "count": "int64",
    "sums": "float64",
    "squares": "float64",
    "minimum": "float32",
    "maximum": "float32",
}

# The Q3 benchmark variables. Not the `dl` variable set: `dl` holds every study slug, like `ts`.
# `dl_variable_order` puts them first on the variable axis, so a Q3 read is one span in the shard.
Q3_VARIABLES = (
    "temperature_at_screen_level",
    "wind_speed_at_10m",
    "pressure_at_mean_sea_level",
    "relative_humidity_at_screen_level",
)

# One contiguous chunk for the append-only init coordinate, sized past any plausible archive.
COORD_INIT_CHUNK = 4096
CF_UNITS = "seconds since 1970-01-01"
CF_CALENDAR = "proleptic_gregorian"
# xarray writes the CF attributes from these; int64 seconds keeps the stored layout unchanged.
TIME_ENCODING = {"units": CF_UNITS, "calendar": CF_CALENDAR, "dtype": "int64"}
LEAD_ENCODING = {"units": "seconds", "dtype": "int64"}
# Icechunk never carries consolidated metadata, and both layouts are Zarr v3.
ZARR_KWARGS: dict[str, Any] = {"zarr_format": 3, "consolidated": False}
SLUGS_ATTR = "wxtco.slugs"
LAYOUT_ATTR = "wxtco.layout"


@dataclass(frozen=True)
class GridSpec:
    """The axes a layout is built on. `leads` is in minutes, as the source keys give them."""

    members: int
    lat: np.ndarray
    lon: np.ndarray
    leads: Sequence[int]


def array_name(slug: str) -> str:
    """Give the Zarr array name of a diagnostic slug. Same rule as `table_schema.column_name`."""
    return slug.replace("-", "_")


def dl_variable_order(slugs: Sequence[str]) -> list[str]:
    """Order the `dl` variable axis: the Q3 variables first, then the rest in study order.

    Only reorders; a Q3 variable that `slugs` leaves out is not added. The four Q3 variables are
    then the first 64 Morton codes of a shard, so a Q3 read is one contiguous span.
    """
    picked = list(slugs)
    first = [s for s in Q3_VARIABLES if s in picked]
    return first + [s for s in picked if s not in first]


def clamp_shapes(
    shape: Sequence[int], chunk: Sequence[int], shard: Sequence[int]
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Fit `chunk` and `shard` to `shape`, keeping the shard a whole number of chunks.

    Axis 0 is the append-only init axis and always stays (1, 1). A shard may round up past a short
    axis, which Zarr allows: the shard then holds the whole axis in fewer chunks.
    """
    if not (len(shape) == len(chunk) == len(shard)):
        raise ValueError(f"rank mismatch: {shape}, {chunk}, {shard}")
    chunks, shards = [1], [1]
    for dim, c, s in zip(shape[1:], chunk[1:], shard[1:], strict=True):
        if dim < 1:
            raise ValueError(f"non-init axis of length {dim} in {tuple(shape)}")
        c2 = max(1, min(c, dim))
        s2 = -(-max(c2, min(s, dim)) // c2) * c2  # round the clamped shard up to whole chunks
        chunks.append(c2)
        shards.append(s2)
    return tuple(chunks), tuple(shards)


def _fill(dtype: Any) -> Any:
    """The fill value of one dtype: NaN, or 0 where NaN does not exist."""
    return 0 if np.issubdtype(dtype, np.integer) else np.nan


def _lazy(shape: Sequence[int], dtype: Any, fill: Any) -> Any:
    """A Dask placeholder. `to_zarr(compute=False)` sizes the array from it and writes no chunk."""
    return dsa.full(tuple(shape), fill, dtype=dtype, chunks=tuple(max(s, 1) for s in shape))


def _encoding(
    shape: Sequence[int], chunk: Sequence[int], shard: Sequence[int] | None, fill: object
) -> dict[str, Any]:
    """Encoding for one pcodec array. `shard` None means plain chunks, for the small stats arrays."""
    chunks, shards = clamp_shapes(shape, chunk, shard or chunk)
    return {
        "chunks": chunks,
        "shards": shards if shard is not None else None,
        "serializer": PCodec(level=PCODEC_LEVEL),
        "compressors": None,
        "filters": None,
        "fill_value": fill,
        "_FillValue": None,  # the Zarr fill value alone; no CF attribute on the array
    }


def _coords(spec: GridSpec) -> tuple[dict[str, Any], dict[str, Any]]:
    """Give the coordinates, init and valid_time empty, and their CF encoding."""
    leads = np.asarray(spec.leads, "int64").astype("timedelta64[m]").astype("timedelta64[s]")
    coords = {
        "init": ("init", np.empty(0, "datetime64[s]")),
        "lead": ("lead", leads),
        "member": ("member", np.arange(spec.members, dtype="int32")),
        "lat": ("lat", np.asarray(spec.lat, "float32")),
        "lon": ("lon", np.asarray(spec.lon, "float32")),
        "valid_time": (("init", "lead"), np.empty((0, leads.size), "datetime64[s]")),
    }
    encoding = {
        "init": {**TIME_ENCODING, "chunks": (COORD_INIT_CHUNK,)},
        "valid_time": {**TIME_ENCODING, "chunks": (1, leads.size)},
        "lead": dict(LEAD_ENCODING),
        # The grid axes keep the Zarr default fill value and carry no CF attribute.
        "lat": {"_FillValue": None, "fill_value": 0.0},
        "lon": {"_FillValue": None, "fill_value": 0.0},
    }
    return coords, encoding


def _group_attrs(layout: str, slugs: Sequence[str]) -> dict[str, str]:
    """The group attributes both layouts carry. Every write must repeat them: `to_zarr` replaces."""
    return {LAYOUT_ATTR: layout, SLUGS_ATTR: json.dumps(list(slugs))}


def _ts_variables(shape: Sequence[int], slugs: Sequence[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Give the `ts` data variables and their encoding: one lazy array per slug."""
    encoding = _encoding(shape, TS_CHUNK, TS_SHARD, np.float32("nan"))
    data = {array_name(s): (TS_DIMS, _lazy(shape, "float32", np.nan), {"wxtco.slug": s}) for s in slugs}
    return data, {array_name(s): dict(encoding) for s in slugs}


def create_ts(store: Any, spec: GridSpec, slugs: Sequence[str]) -> None:
    """Build `ts`: coordinates plus one empty (init, member, lead, lat, lon) array per slug."""
    shape = (0, spec.members, len(spec.leads), spec.lat.size, spec.lon.size)
    data, encoding = _ts_variables(shape, slugs)
    coords, coord_encoding = _coords(spec)
    ds = xr.Dataset(data, coords=coords, attrs=_group_attrs("ts", slugs))
    # Metadata only: every data variable is a Dask placeholder and `compute=False`.
    ds.to_zarr(store, mode="w-", encoding=encoding | coord_encoding, compute=False, **ZARR_KWARGS)


def add_ts_arrays(store: Any, ds: xr.Dataset, slugs: Sequence[str]) -> None:
    """Create any missing `ts` variable array at the stored init length, empty."""
    missing = [s for s in slugs if array_name(s) not in ds.data_vars]
    if not missing:
        return
    shape = (ds.sizes["init"], ds.sizes["member"], ds.sizes["lead"], ds.sizes["lat"], ds.sizes["lon"])
    data, encoding = _ts_variables(shape, missing)
    known = frozen_slugs(ds.attrs)
    merged = known + [s for s in slugs if s not in known]
    # Keep the group slug list in step with the arrays, so readers see every variable.
    new = xr.Dataset(data, attrs=_group_attrs("ts", merged))
    new.to_zarr(store, mode="a", encoding=encoding, compute=False, **ZARR_KWARGS)


def create_dl(store: Any, spec: GridSpec, slugs: Sequence[str]) -> None:
    """Build `dl`: one `data` array, one shard per sample, plus the per-sample stats."""
    slugs = dl_variable_order(slugs)
    names = [array_name(s) for s in slugs]
    v = len(names)
    shape = (0, len(spec.leads), v, spec.members, spec.lat.size, spec.lon.size)
    stats_shape = (0, len(spec.leads), v)
    coords, encoding = _coords(spec)
    # An object array keeps the Zarr variable-length string dtype; a NumPy `str_` array is fixed width.
    coords["variable"] = ("variable", np.array(names, dtype=object))
    data: dict[str, Any] = {"data": (DL_DIMS, _lazy(shape, "float32", np.nan))}
    shard = DL_SHARD[:2] + (v,) + DL_SHARD[3:]
    encoding["data"] = _encoding(shape, DL_CHUNK, shard, np.float32("nan"))
    for name, dtype in STATS.items():
        fill = _fill(dtype) if dtype == "int64" else np.float64("nan")
        data[name] = (STATS_DIMS, _lazy(stats_shape, dtype, fill))
        encoding[name] = _encoding(stats_shape, STATS_CHUNK[:2] + (v,), None, fill)
    ds = xr.Dataset(data, coords=coords, attrs=_group_attrs("dl", slugs))
    ds.to_zarr(store, mode="w-", encoding=encoding, compute=False, **ZARR_KWARGS)


def append_init(store: Any, ds: xr.Dataset, init: np.datetime64) -> None:
    """Append one init slot to every array: real coordinates, lazy data, so no data chunk is stored.

    `to_zarr` replaces the group attributes, so the stored ones ride along.
    """
    valid = (init.astype("datetime64[s]") + ds["lead"].values).astype("datetime64[s]")
    slab = {
        # An unwritten slot reads the array fill value: NaN, or 0 for the integer `count`.
        name: (var.dims, _lazy((1, *var.shape[1:]), var.dtype, _fill(var.dtype)))
        # A repo written before the rewrite stores `valid_time` as a data variable; it is a
        # coordinate here, so skip it rather than write it twice.
        for name, var in ds.data_vars.items()
        if name != "valid_time"
    }
    out = xr.Dataset(
        slab,
        coords={
            "init": ("init", np.asarray([init], "datetime64[s]")),
            "valid_time": (("init", "lead"), valid[np.newaxis]),
        },
        attrs=dict(ds.attrs),
    )
    out.to_zarr(store, append_dim="init", compute=False, **ZARR_KWARGS)


def write_region(store: Any, ds: xr.Dataset, region: dict[str, slice]) -> None:
    """Write one slab into an existing layout. `drop_encoding` keeps the stored encoding."""
    ds.drop_encoding().to_zarr(store, region=region, **ZARR_KWARGS)


def frozen_slugs(attrs: Mapping[str, Any]) -> list[str]:
    """Give the slug list the group was created with."""
    raw = attrs.get(SLUGS_ATTR)
    if raw is None:
        raise ValueError(f"group has no {SLUGS_ATTR} attribute; it was not written by wxtco")
    return list(json.loads(str(raw)))
