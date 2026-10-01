import numpy as np

from wxtco.config import store_for
from wxtco.ingest.netcdf import data_variable, grid_values, open_netcdf
from wxtco.source import list_cycle


def test_open_netcdf_from_store(fixture_root):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    d = next(d for d in list_cycle(store, cycle) if d.slug == "temperature_at_screen_level")
    ds = open_netcdf(store, d.files[2].key)  # lead 120 min
    assert data_variable(ds) == "air_temperature"
    v = grid_values(ds)
    assert v.shape == (2, 8, 8) and v.dtype == "float32"
    assert v[1, 3, 5] == 1000 + 20 + 3 + 0.05
    assert int(ds["forecast_period"].values) == 120 * 60


def _surface_key(store, cycle: str) -> str:
    d = next(d for d in list_cycle(store, cycle) if d.slug == "temperature_at_screen_level")
    return d.files[2].key  # lead 120 min


def test_data_variable_ignores_bounds(fixture_root):
    """Bounds variables can be 3-D. They must not count as the data variable."""
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    ds = open_netcdf(store, _surface_key(store, cycle))
    shape = ds["air_temperature"].shape
    dims = ("realization", "latitude", "longitude")
    ds["latitude_bnds"] = (dims, np.zeros(shape, dtype="float32"))
    ds["lat_edges"] = (dims, np.zeros(shape, dtype="float32"))
    ds["latitude"].attrs["bounds"] = "lat_edges"
    ds.to_netcdf(root / "with_bounds.nc", engine="h5netcdf")
    out = open_netcdf(store, "with_bounds.nc")
    assert data_variable(out) == "air_temperature"
    assert grid_values(out).shape == (2, 8, 8)


def test_grid_values_transposed_file(fixture_root):
    """A file stored in another dim order still gives (member, ny, nx)."""
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    ds = open_netcdf(store, _surface_key(store, cycle))
    ds.transpose("latitude", "longitude", "realization").to_netcdf(root / "transposed.nc", engine="h5netcdf")
    v = grid_values(open_netcdf(store, "transposed.nc"))
    assert v.shape == (2, 8, 8) and v.dtype == "float32"
    assert v[1, 3, 5] == 1000 + 20 + 3 + 0.05
