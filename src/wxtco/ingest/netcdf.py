"""Read one Met Office NetCDF file from an object store into xarray."""

from __future__ import annotations

import io

import numpy as np
import obstore as obs
import xarray as xr
from obstore.store import ObjectStore

BOUNDS_SUFFIXES = ("_bnds", "_bounds")


def open_netcdf(store: ObjectStore, key: str) -> xr.Dataset:
    """Fetch the whole object and open it in memory. Files are ~35 MB; one GET is cheapest."""
    payload = io.BytesIO(obs.get(store, key).bytes())
    # Load eagerly and close the handle, so many parallel files do not hold HDF5 state open.
    with xr.open_dataset(payload, engine="h5netcdf", decode_times=True) as ds:
        return ds.load()


def _bounds_names(ds: xr.Dataset) -> set[str]:
    """Names of bounds variables: by name suffix, or named in the bounds attribute of a coordinate."""
    named = {str(c.attrs["bounds"]) for c in ds.coords.values() if "bounds" in c.attrs}
    return named | {n for n in ds.variables if str(n).endswith(BOUNDS_SUFFIXES)}


def data_variable(ds: xr.Dataset) -> str:
    """Give the name of the one 3-D data variable, bounds variables not counted."""
    skip = _bounds_names(ds)
    names = [n for n, v in ds.data_vars.items() if v.ndim == 3 and n not in skip]
    if len(names) != 1:
        raise ValueError(f"expected one 3-D variable, found {names}")
    return names[0]


def grid_values(ds: xr.Dataset) -> np.ndarray:
    """Give the (member, ny, nx) grid as a contiguous float32 array, dim order enforced."""
    da = ds[data_variable(ds)].transpose("realization", "latitude", "longitude")
    return np.ascontiguousarray(da.values, dtype="float32")
