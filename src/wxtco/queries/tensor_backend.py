"""xarray over an Icechunk group. Same code for virtual and (stage B) native repos."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any

import icechunk as ic
import numpy as np
import xarray as xr
import zarr

from wxtco.ingest.virtual import open_virtual_dataset_group
from wxtco.queries.base import Box, Point
from wxtco.source import cycle_time

# Many small virtual chunks per query: raise zarr fetch concurrency (Ryan, 2026-09-19).
# Peak memory of a sharded read is (shards in flight) x (decoded bytes of the chunks touched per
# shard), so `WXTCO_ZARR_CONCURRENCY` lowers it for layouts with large chunks.
zarr.config.set(
    {
        "async.concurrency": int(os.environ.get("WXTCO_ZARR_CONCURRENCY", "256")),
        "threading.max_workers": 16,
    }
)

RENAME = {
    "forecast_reference_time": "init",
    "forecast_period": "lead",
    "realization": "member",
    "latitude": "lat",
    "longitude": "lon",
}


def rename_dims[T: (xr.Dataset, xr.DataArray)](obj: T) -> T:
    """Rename the CF dims and coords to init, lead, member, lat, lon."""
    return obj.rename({k: v for k, v in RENAME.items() if k in obj.dims or k in obj.coords})


def init_time(cycle: str) -> np.datetime64:
    """Give the cycle init as naive datetime64[ns], the stored `init` dtype."""
    return np.datetime64(cycle_time(cycle).replace(tzinfo=None), "ns")


def lead_hours(lead: xr.DataArray) -> xr.DataArray:
    """Give `lead` as whole hours."""
    # xarray decodes forecast_period to timedelta64; without units it stays seconds.
    hours = lead / np.timedelta64(1, "h") if np.issubdtype(lead.dtype, np.timedelta64) else lead // 3600
    return hours.astype(int)


def _missing_cycle(cycle: str, obj: xr.Dataset | xr.DataArray) -> ValueError:
    """Give the error for a cycle that the repo does not hold."""
    inits = obj["init"].values
    span = f"{inits[0]} to {inits[-1]}" if inits.size else "none"
    return ValueError(f"cycle {cycle} not in repo; available: {span}")


def positions(obj: xr.Dataset | xr.DataArray, dim: str, labels: Sequence[Any]) -> slice | list[int]:
    """Give the positions of `labels` on `dim`: a slice when they are consecutive, else a list.

    Zarr turns an orthogonal selection with two or more array-indexed axes, or with an integer
    axis, into per-shard coordinate arrays the size of the selection (`OrthogonalIndexer`,
    `ix_`); on a 7 GB shard that is an OOM. Slices keep the selection on the cheap path.
    """
    pos = obj.indexes[dim].get_indexer(list(labels))
    if (pos < 0).any():
        raise KeyError([lab for lab, p in zip(labels, pos, strict=True) if p < 0])
    ints = [int(p) for p in pos]
    if ints == list(range(ints[0], ints[0] + len(ints))):
        return slice(ints[0], ints[0] + len(ints))
    return ints


def select_cycles[T: (xr.Dataset, xr.DataArray)](obj: T, cycles: Sequence[str]) -> T:
    """Select the inits of `cycles` in order, lazily, and give `lead` as whole hours."""
    wanted = [init_time(c) for c in cycles]
    try:
        out = obj.isel(init=positions(obj, "init", wanted))
    except KeyError:
        present = np.isin(np.array(wanted, dtype="datetime64[ns]"), obj["init"].values)
        missing = [c for c, ok in zip(cycles, present, strict=True) if not ok]
        if not missing:
            raise
        raise _missing_cycle(missing[0], obj) from None
    return out.assign_coords(lead=lead_hours(out["lead"]))


def select_leads[T: (xr.Dataset, xr.DataArray)](obj: T, leads: Sequence[int], cycles: Sequence[str]) -> T:
    """Select `leads` (whole hours) in order. A missing lead is a ValueError that names it."""
    try:
        return obj.isel(lead=positions(obj, "lead", leads))
    except KeyError:
        have = obj["lead"].values.tolist()
        missing = [x for x in leads if x not in have]
        if not missing:
            raise
        raise ValueError(
            f"lead {missing[0]} h not in cycles {', '.join(cycles)}; available: {have}"
        ) from None


def _stack_lead(ds: xr.Dataset, vars: Sequence[str]) -> np.ndarray:
    """Stack one lead of `vars` as float32 (var, member, lat, lon)."""
    return np.stack([ds[v].transpose("member", "lat", "lon").values.astype("float32") for v in vars])


class TensorBackend:
    """Read one Icechunk group with xarray. Dims are renamed to init, lead, member, lat, lon."""

    def __init__(self, ds: xr.Dataset, name: str = "virtual") -> None:
        self.ds = rename_dims(ds)
        self.name = name

    @classmethod
    def from_repo(cls, repo: ic.Repository, group: str = "surface", name: str = "virtual") -> TensorBackend:
        """Give a backend on one group of an Icechunk repo. The read-only session pins one snapshot,
        so a long-lived process does not see later commits; make a new backend to pick them up."""
        return cls(open_virtual_dataset_group(repo, group), name=name)

    def _cycle(self, cycle: str) -> xr.Dataset:
        """Select one init time and give `lead` as whole hours."""
        try:
            out = self.ds.sel(init=init_time(cycle))
        except KeyError:
            raise _missing_cycle(cycle, self.ds) from None
        return out.assign_coords(lead=lead_hours(out["lead"]))

    def _select(self, cycles: Sequence[str]) -> xr.Dataset:
        """Select the inits of `cycles` in order and give `lead` as whole hours."""
        return select_cycles(self.ds, cycles)

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        """Give one variable at the nearest cell, dims (member, lead), with a `valid_time` coord.
        `method="nearest"` snaps lat and lon independently and breaks ties to the lower index, so
        bench points must be grid centers for the table and tensor backends to agree."""
        da = self._cycle(cycle)[var].sel(lat=point.lat, lon=point.lon, method="nearest")
        out = da.transpose("member", "lead").load().astype("float32")
        if "time" not in out.coords:
            raise ValueError("group has no time coordinate; cannot build valid_time")
        out = out.rename({"time": "valid_time"})
        return out.drop_vars([c for c in out.coords if c not in ("member", "lead", "valid_time")])

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        """Give `vars` inside `box`, dims (member, lead, lat, lon)."""
        ds = self._cycle(cycle)[list(vars)]
        # Slice direction must follow the stored latitude order.
        lat_slice = (
            slice(box.lat_min, box.lat_max)
            if ds["lat"][0] < ds["lat"][-1]
            else slice(box.lat_max, box.lat_min)
        )
        sel = ds.sel(lat=lat_slice, lon=slice(box.lon_min, box.lon_max))
        return sel.transpose("member", "lead", "lat", "lon").load().astype("float32")

    def _lead_selection(self, vars: Sequence[str], cycle: str, lead_hours: int) -> xr.Dataset:
        """Give `vars` at one cycle and one lead, lazily. A missing lead is a ValueError that names it."""
        cyc = self._cycle(cycle)
        try:
            ds = cyc.sel(lead=lead_hours)
        except KeyError:
            raise ValueError(
                f"lead {lead_hours} h not in cycle {cycle}; available: {cyc['lead'].values.tolist()}"
            ) from None
        return ds[list(vars)]

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        """Give one lead as float32 of shape (len(vars), member, ny, nx)."""
        return _stack_lead(self._lead_selection(vars, cycle, lead_hours), vars)

    def batch_fields(self, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]) -> np.ndarray:
        """Give all samples as float32 (cycle, lead, var, member, ny, nx): one lazy selection, one load."""
        ds = select_leads(self._select(cycles), leads, cycles)[list(vars)].load()
        dims = ("init", "lead", "member", "lat", "lon")
        return np.stack([ds[v].transpose(*dims).values for v in vars], axis=2).astype("float32")

    def bytes_read(self) -> int | None:
        """Give None. Stage A reads S3 request metrics from CloudWatch instead."""
        return None
