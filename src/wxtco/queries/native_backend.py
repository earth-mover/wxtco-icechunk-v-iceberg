"""xarray views of the two native layouts, shaped for `TensorBackend`."""

from __future__ import annotations

from collections.abc import Sequence

import icechunk as ic
import numpy as np
import xarray as xr

from wxtco.ingest.native import NativeGroup
from wxtco.ingest.native_layout import ZARR_KWARGS
from wxtco.queries.tensor_backend import (
    TensorBackend,
    positions,
    rename_dims,
    select_cycles,
    select_leads,
)


def _open(repo: ic.Repository) -> xr.Dataset:
    """Open the repo root lazily, with `valid_time` as a coordinate."""
    ds = xr.open_zarr(repo.readonly_session("main").store, chunks=None, **ZARR_KWARGS)
    return promote_valid_time(ds)


def _split(ds: xr.Dataset, group: NativeGroup) -> xr.Dataset:
    """Give one variable per diagnostic, with `valid_time` renamed to `time`."""
    if group == "dl":
        # `valid_time` rides along: xarray stored it in the `coordinates` attribute of `data`.
        ds = ds["data"].to_dataset(dim="variable")
    return ds.rename({"valid_time": "time"})


def open_native_dataset(repo: ic.Repository, group: NativeGroup) -> xr.Dataset:
    """Give one variable per diagnostic, in the dimension order the layout stores.

    `ts` gives (init, member, lead, lat, lon); `dl` gives (init, lead, member, lat, lon), because
    `dl` stores the variables stacked in one array and is split on the `variable` coordinate.
    `valid_time` is renamed to `time`, the CF name `TensorBackend.point_series` looks for. xarray
    decodes `init`, `lead` and `valid_time` from the stored CF attributes; nothing else is needed.
    """
    return _split(_open(repo), group)


def promote_valid_time(ds: xr.Dataset) -> xr.Dataset:
    """Make `valid_time` a coordinate. Repos written before the xarray rewrite store it as a data variable."""
    return ds.set_coords("valid_time") if "valid_time" in ds.data_vars else ds


class NativeDlBackend(TensorBackend):
    """`TensorBackend` on the `dl` layout that also keeps the stacked `data` array.

    `batch_fields` reads the stacked array in one selection, thus zarr batches all chunk fetches.
    The split dataset serves the other queries.
    """

    def __init__(self, ds: xr.Dataset, stacked: xr.DataArray, name: str = "native_dl") -> None:
        super().__init__(ds, name=name)
        # Dims (init, lead, variable, member, lat, lon); `variable` holds the slug names.
        self.stacked = rename_dims(stacked)

    def batch_fields(self, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]) -> np.ndarray:
        """Give all samples as float32 (cycle, lead, var, member, ny, nx) from one array read."""
        da = select_leads(select_cycles(self.stacked, cycles), leads, cycles)
        # Q3 variables are adjacent in the stored order, so this is a slice, not an array index.
        out = da.isel(variable=positions(da, "variable", vars)).load()
        return out.transpose("init", "lead", "variable", "member", "lat", "lon").values.astype("float32")


def open_native_backend(repo: ic.Repository, group: NativeGroup, name: str | None = None) -> TensorBackend:
    """Give a `TensorBackend` on one native layout; `NativeDlBackend` for `dl`."""
    ds = _open(repo)
    name = name or f"native_{group}"
    if group == "dl":
        return NativeDlBackend(_split(ds, group), ds["data"].rename({"valid_time": "time"}), name=name)
    return TensorBackend(_split(ds, group), name=name)
