"""Tests for Q1 heating degree days."""

import numpy as np
import xarray as xr

from wxtco.queries.q1_point import heating_degree_days


def test_hdd_two_days():
    # 2 members, 4 leads: two on day 1 at 10C and 20C (mean 15C -> 3 HDD), two on day 2 at 25C (0 HDD)
    k = 273.15
    vals = np.array([[10 + k, 20 + k, 25 + k, 25 + k], [0 + k, 0 + k, 30 + k, 30 + k]], dtype="float32")
    valid = np.array(
        ["2026-09-16T12", "2026-09-16T18", "2026-09-17T00", "2026-09-17T06"], dtype="datetime64[ns]"
    )
    da = xr.DataArray(
        vals,
        dims=("member", "lead"),
        coords={"member": [0, 1], "lead": [0, 6, 12, 18], "valid_time": ("lead", valid)},
    )
    hdd = heating_degree_days(da)
    assert hdd.dims == ("member", "day")
    np.testing.assert_allclose(hdd.values, [[3.0, 0.0], [18.0, 0.0]], atol=1e-3)
    assert list(hdd["n_leads"].values) == [2, 2]


def test_hdd_skips_nan_lead():
    # Member 0 loses the 10C lead to NaN, so day 1 mean is 20C alone -> 0 HDD, not NaN.
    k = 273.15
    vals = np.array([[np.nan, 20 + k, 25 + k, 25 + k], [0 + k, 0 + k, 30 + k, 30 + k]], dtype="float32")
    valid = np.array(
        ["2026-09-16T12", "2026-09-16T18", "2026-09-17T00", "2026-09-17T06"], dtype="datetime64[ns]"
    )
    da = xr.DataArray(
        vals,
        dims=("member", "lead"),
        coords={"member": [0, 1], "lead": [0, 6, 12, 18], "valid_time": ("lead", valid)},
    )
    hdd = heating_degree_days(da)
    assert hdd.dims == ("member", "day")
    assert not np.isnan(hdd.values).any()
    np.testing.assert_allclose(hdd.values, [[0.0, 0.0], [18.0, 0.0]], atol=1e-3)


def test_hdd_partial_day_counts_leads():
    # Day 1 has 3 leads, day 2 has 1: the short day still gets a mean, and n_leads shows the gap.
    k = 273.15
    vals = np.array([[10 + k, 20 + k, 12 + k, 25 + k]], dtype="float32")
    valid = np.array(
        ["2026-09-16T06", "2026-09-16T12", "2026-09-16T18", "2026-09-17T00"], dtype="datetime64[ns]"
    )
    da = xr.DataArray(
        vals,
        dims=("member", "lead"),
        coords={"member": [0], "lead": [0, 6, 12, 18], "valid_time": ("lead", valid)},
    )
    hdd = heating_degree_days(da)
    assert list(hdd["n_leads"].values) == [3, 1]
    # Day 1 mean is 14C -> 4 HDD; day 2 is the single 25C lead -> 0 HDD.
    np.testing.assert_allclose(hdd.values, [[4.0, 0.0]], atol=1e-3)
