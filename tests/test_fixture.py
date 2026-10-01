import xarray as xr

from wxtco.config import store_for
from wxtco.fixture import FIXTURE_LEADS_MIN, FIXTURE_SLUGS, build_cycle
from wxtco.source import list_cycle


def test_build_cycle_layout(tmp_path):
    cycle = "2026/09/16/T0000Z"
    paths = build_cycle(tmp_path, cycle)
    # 3 slugs x 4 leads, minus the PT01H statistic at lead 0
    assert len(paths) == len(FIXTURE_SLUGS) * len(FIXTURE_LEADS_MIN) - 1
    diags = list_cycle(store_for(f"file://{tmp_path}"), cycle)
    assert [d.slug for d in diags] == sorted(FIXTURE_SLUGS)
    ds = xr.open_dataset(paths[0], engine="h5netcdf")
    (name,) = [v for v in ds.data_vars if ds[v].ndim == 3]
    assert ds[name].shape == (2, 8, 8)
    assert ds[name].encoding["chunksizes"] == (1, 4, 4)
    assert ds[name].dtype == "float32"
    assert "forecast_reference_time" in ds.coords and "forecast_period" in ds.coords
    raw = xr.open_dataset(paths[0], engine="h5netcdf", decode_times=False)
    assert raw["time"].attrs["units"] == "seconds since 1970-01-01"


def test_values_deterministic(tmp_path):
    paths = build_cycle(tmp_path, "2026/09/16/T0000Z")
    p = next(p for p in paths if "PT0002H00M-temperature" in p.name)
    ds = xr.open_dataset(p, engine="h5netcdf")
    v = ds["air_temperature"].values
    assert v[1, 3, 5] == 1000 + 20 + 3 + 0.05
