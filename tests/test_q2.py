import numpy as np
import pytest
import xarray as xr

from wxtco.queries.base import Box
from wxtco.queries.q2_regional import crps_ensemble, next_cycle, q2


def test_crps_degenerate_ensemble_equals_abs_error():
    ens = xr.DataArray(np.full((3, 2), 5.0), dims=("member", "x"))
    obs = xr.DataArray([3.0, 5.0], dims=("x",))
    np.testing.assert_allclose(crps_ensemble(ens, obs).values, [2.0, 0.0])


def test_crps_two_member():
    # members 0 and 2, obs 1: mean|x-y| = 1, 0.5*mean|xi-xj| = 0.5*(0+2+2+0)/4 = 0.5 -> 0.5
    ens = xr.DataArray([[0.0], [2.0]], dims=("member", "x"))
    obs = xr.DataArray([1.0], dims=("x",))
    np.testing.assert_allclose(crps_ensemble(ens, obs).values, [0.5])


def test_next_cycle():
    assert next_cycle("2026/09/16/T1800Z") == "2026/09/17/T0000Z"


LEADS = (0, 6, 12)
VARS = ("temperature_at_screen_level", "wind_speed_at_10m")


class FakeBackend:
    """Deterministic backend. Values depend on member, lead and a per-cycle offset."""

    name = "fake"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def box_fields(self, vars, box, cycle):
        self.calls.append(cycle)
        offset = float(sum(cycle.encode()) % 7)
        member = np.arange(3.0)
        lead = np.array(LEADS, dtype="int64")
        lat = np.array([0.0, 1.0])
        lon = np.array([10.0, 11.0])
        # Separable field so the box mean is easy to compute by hand.
        base = (
            member[:, None, None, None]
            + lead[None, :, None, None]
            + lat[None, None, :, None]
            + lon[None, None, None, :]
        )
        dims = ("member", "lead", "lat", "lon")
        coords = {"member": member, "lead": lead, "lat": lat, "lon": lon}
        return xr.Dataset({v: (dims, base + offset + i) for i, v in enumerate(vars)}, coords=coords)

    def bytes_read(self):
        return None


def test_q2_drops_leads_without_proxy_and_computes_mean():
    backend = FakeBackend()
    out = q2(backend, Box(0.0, 1.0, 10.0, 11.0), "2026/09/16/T1800Z", vars=VARS)

    # Lead 0 has no proxy: the next cycle has no lead -6.
    np.testing.assert_array_equal(out["lead"].values, [6, 12])
    assert backend.calls == ["2026/09/16/T1800Z", "2026/09/17/T0000Z"]

    offset = float(sum(b"2026/09/16/T1800Z") % 7)
    # mean over member (0,1,2)=1, lat (0,1)=0.5, lon (10,11)=10.5 -> lead + 12.0
    for i, v in enumerate(VARS):
        expect = np.array([6.0, 12.0]) + 12.0 + offset + i
        np.testing.assert_allclose(out[f"{v}_mean"].values, expect)
        # Spread is the member std, which is the same at every cell.
        np.testing.assert_allclose(out[f"{v}_spread"].values, np.full(2, np.std([0.0, 1.0, 2.0])))


def test_q2_crps_against_hand_value():
    backend = FakeBackend()
    cycle = "2026/09/16/T1800Z"
    out = q2(backend, Box(0.0, 1.0, 10.0, 11.0), cycle, vars=("temperature_at_screen_level",))

    # Proxy at lead L is the next cycle's member mean at lead L-6, so obs - ens_mean
    # is (next_offset - offset) - 6 at every cell; CRPS depends only on that gap.
    gap = float(sum(next_cycle(cycle).encode()) % 7) - float(sum(cycle.encode()) % 7) - 6.0
    ens = np.array([0.0, 1.0, 2.0])
    expect = np.abs(ens - (1.0 + gap)).mean() - 0.5 * np.abs(ens[:, None] - ens[None, :]).mean()
    np.testing.assert_allclose(out["temperature_at_screen_level_crps"].values, np.full(2, expect))


class MissingProxyBackend(FakeBackend):
    """The next cycle is not ingested, so its query gives zero rows."""

    def box_fields(self, vars, box, cycle):
        ds = super().box_fields(vars, box, cycle)
        return ds.isel(lead=slice(0, 0)) if cycle != "2026/09/16/T1800Z" else ds


def test_q2_raises_when_proxy_cycle_is_missing():
    backend = MissingProxyBackend()
    with pytest.raises(ValueError, match="no proxy leads for 2026/09/16/T1800Z"):
        q2(backend, Box(0.0, 1.0, 10.0, 11.0), "2026/09/16/T1800Z", vars=VARS)
