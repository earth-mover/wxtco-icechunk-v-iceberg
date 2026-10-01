"""Q2: regional ensemble mean, spread, and CRPS against a proxy analysis."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

import numpy as np
import xarray as xr

from wxtco.queries.base import Backend, Box
from wxtco.source import cycle_time

CYCLE_HOURS = 6


def crps_ensemble(ens: xr.DataArray, obs: xr.DataArray, member_dim: str = "member") -> xr.DataArray:
    """Energy form of CRPS for a finite ensemble: mean_i |x_i - y| - 0.5 mean_ij |x_i - x_j|.

    The second term builds an (i, j) pairwise array, so peak memory is M times the field size.
    That is acceptable at M = 18 members over one box, but not for a full global grid.
    The estimator is biased low at small M, so compare only across equal ensemble sizes.
    """
    term1 = np.abs(ens - obs).mean(member_dim)
    a = ens.rename({member_dim: "i"})
    b = ens.rename({member_dim: "j"})
    term2 = 0.5 * np.abs(a - b).mean(("i", "j"))
    return term1 - term2


def next_cycle(cycle: str) -> str:
    """Give the cycle key 6 h after `cycle`."""
    t = cycle_time(cycle) + timedelta(hours=CYCLE_HOURS)
    return t.strftime("%Y/%m/%d/T%H%MZ")


def q2(
    backend: Backend,
    box: Box,
    cycle: str,
    vars: Sequence[str] = ("temperature_at_screen_level", "wind_speed_at_10m"),
) -> xr.Dataset:
    """Give box-mean ensemble mean, spread and CRPS per lead, dims (lead,).

    Proxy obs is the next cycle's ensemble mean at the same valid time; leads with no proxy drop out.
    """
    fc = backend.box_fields(vars, box, cycle)
    proxy_all = backend.box_fields(vars, box, next_cycle(cycle)).mean("member")
    # Same valid time: lead L in this cycle is lead L-6 in the next one.
    proxy = proxy_all.assign_coords(lead=proxy_all["lead"] + CYCLE_HOURS)
    common = np.intersect1d(fc["lead"].values, proxy["lead"].values)
    # An un-ingested next cycle gives zero rows, which must fail loudly, not give an empty result.
    if common.size == 0:
        raise ValueError(f"no proxy leads for {cycle}; is {next_cycle(cycle)} ingested?")
    fc, proxy = fc.sel(lead=common), proxy.sel(lead=common)
    out = {}
    for v in vars:
        out[f"{v}_mean"] = fc[v].mean(("member", "lat", "lon"))
        out[f"{v}_spread"] = fc[v].std("member").mean(("lat", "lon"))
        out[f"{v}_crps"] = crps_ensemble(fc[v], proxy[v]).mean(("lat", "lon"))
    return xr.Dataset(out)
