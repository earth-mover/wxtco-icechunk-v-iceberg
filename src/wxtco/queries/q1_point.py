"""Q1: point forecast timeseries and heating degree days."""

from __future__ import annotations

from dataclasses import dataclass

import xarray as xr

from wxtco.queries.base import Backend, Point

K = 273.15


def heating_degree_days(temp_k: xr.DataArray, base_c: float = 18.0) -> xr.DataArray:
    """Daily mean by valid-time date, then max(base - mean, 0). Dims (member, day).

    The daily mean skips NaN leads (xarray default skipna); a day is NaN only if all its leads are.
    Partial days keep the mean of the leads present; see `n_leads`.
    """
    day = temp_k["valid_time"].dt.floor("D").rename("day")
    # Leads per day, from valid_time only: one count for all members, NaN temperatures included.
    n_leads = temp_k["valid_time"].groupby(day).count()
    daily_c = (temp_k - K).groupby(day).mean("lead")
    hdd = (base_c - daily_c).clip(min=0).transpose("member", "day")
    return hdd.assign_coords(n_leads=n_leads)


@dataclass(frozen=True)
class Q1Result:
    """The point temperature series and the heating degree days made from it."""

    series: xr.DataArray
    hdd: xr.DataArray


def q1(backend: Backend, point: Point, cycle: str, var: str = "temperature_at_screen_level") -> Q1Result:
    """Read the point series for one cycle and give it with its heating degree days."""
    series = backend.point_series(var, point, cycle)
    return Q1Result(series, heating_degree_days(series))
