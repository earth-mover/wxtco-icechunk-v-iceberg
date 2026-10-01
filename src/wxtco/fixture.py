"""Synthetic MOGREPS-like cycle for tests. Small grid, real encoding and key layout."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import numpy as np
import xarray as xr

from wxtco.source import cycle_time

FIXTURE_SLUGS = ("temperature_at_screen_level", "wind_speed_at_10m", "precipitation_accumulation-PT01H")
FIXTURE_LEADS_MIN = (0, 60, 120, 180)
CF_NAMES = {
    "temperature_at_screen_level": ("air_temperature", "K", 1.5),
    "wind_speed_at_10m": ("wind_speed", "m s-1", 10.0),
    "precipitation_accumulation-PT01H": ("lwe_thickness_of_precipitation_amount", "m", None),
}


TIME_ENCODING = {"units": "seconds since 1970-01-01", "dtype": "int64"}


def _values(members: int, ny: int, nx: int, lead_hours: int) -> np.ndarray:
    """Deterministic data: member*1000 + lead_hours*10 + lat_index + lon_index/100."""
    m = np.arange(members)[:, None, None] * 1000.0
    y = np.arange(ny)[None, :, None] * 1.0
    x = np.arange(nx)[None, None, :] / 100.0
    return (m + lead_hours * 10 + y + x).astype("float32")


def build_cycle(
    root: Path,
    cycle: str,
    members: int = 2,
    ny: int = 8,
    nx: int = 8,
    slugs: Sequence[str] = FIXTURE_SLUGS,
    leads: Sequence[int] = FIXTURE_LEADS_MIN,
) -> list[Path]:
    """Write one synthetic cycle under `root/<cycle>/`. Return the file paths written."""
    init = cycle_time(cycle)
    out: list[Path] = []
    lat = np.linspace(-89.9, 89.9, ny, dtype="float32")
    lon = np.linspace(-179.9, 179.9, nx, dtype="float32")
    for slug in slugs:
        cf, units, height = CF_NAMES[slug]
        for lead_min in leads:
            if slug.endswith("-PT01H") and lead_min == 0:
                continue
            valid = init + timedelta(minutes=lead_min)
            data = _values(members, ny, nx, lead_min // 60)
            coords = {
                "realization": ("realization", np.arange(members, dtype="int32")),
                "latitude": ("latitude", lat),
                "longitude": ("longitude", lon),
                "forecast_period": ((), np.int32(lead_min * 60), {"units": "seconds"}),
                "forecast_reference_time": ((), np.datetime64(init.replace(tzinfo=None), "ns")),
                "time": ((), np.datetime64(valid.replace(tzinfo=None), "ns")),
            }
            if height is not None:
                coords["height"] = ((), np.float32(height), {"units": "m"})
            ds = xr.Dataset(
                {cf: (("realization", "latitude", "longitude"), data, {"units": units})}, coords=coords
            )
            ds.attrs.update(
                {
                    "institution": "Met Office",
                    "title": "wxtco synthetic fixture",
                    "mosg__model_configuration": "gl_ens",
                }
            )
            name = f"{valid.strftime('%Y%m%dT%H%MZ')}-PT{lead_min // 60:04d}H{lead_min % 60:02d}M-{slug}.nc"
            path = root / cycle / name
            path.parent.mkdir(parents=True, exist_ok=True)
            # One epoch for all files, and no _FillValue on the coords.
            encoding = {
                cf: {
                    "chunksizes": (1, min(4, ny), min(4, nx)),
                    "zlib": True,
                    "complevel": 1,
                    "dtype": "float32",
                },
                "time": dict(TIME_ENCODING),
                "forecast_reference_time": dict(TIME_ENCODING),
                "latitude": {"_FillValue": None},
                "longitude": {"_FillValue": None},
            }
            if height is not None:
                encoding["height"] = {"_FillValue": None}
            ds.to_netcdf(path, engine="h5netcdf", encoding=encoding)
            out.append(path)
    return out
