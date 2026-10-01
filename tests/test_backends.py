import importlib

import numpy as np
import pytest
import xarray as xr

import wxtco.ingest.mogreps_virtual as mv
from wxtco.catalog import ensure_table, local_catalog
from wxtco.config import Settings, store_for
from wxtco.fixture import FIXTURE_SLUGS, build_cycle
from wxtco.ingest.table import ingest_cycle_table
from wxtco.ingest.table_schema import column_name
from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_repo
from wxtco.queries.base import Box, Point
from wxtco.queries.table_backend import TableBackend
from wxtco.queries.tensor_backend import TensorBackend


@pytest.fixture
def table_backend(fixture_root, tmp_path):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    ingest_cycle_table(cat, store, store, cycle, list(FIXTURE_SLUGS), log=lambda *_: None)
    return TableBackend.local(ensure_table(cat, list(FIXTURE_SLUGS)).metadata_location), cycle


def test_point_series(table_backend):
    b, cycle = table_backend
    # nearest grid cell to (lat index 3, lon index 5) on the 8x8 fixture grid
    lat = np.linspace(-89.9, 89.9, 8)[3]
    lon = np.linspace(-179.9, 179.9, 8)[5]
    s = b.point_series("temperature_at_screen_level", Point(lat, lon), cycle)
    assert s.dims == ("member", "lead")
    assert list(s["lead"].values) == [0, 1, 2, 3]
    assert s.sel(member=1, lead=2).item() == np.float32(1000 + 20 + 3 + 0.05)


def test_box_fields(table_backend):
    b, cycle = table_backend
    lats = np.linspace(-89.9, 89.9, 8)
    lons = np.linspace(-179.9, 179.9, 8)
    box = Box(lats[2] - 0.01, lats[4] + 0.01, lons[1] - 0.01, lons[3] + 0.01)
    ds = b.box_fields(["temperature_at_screen_level", "wind_speed_at_10m"], box, cycle)
    assert ds["temperature_at_screen_level"].dims == ("member", "lead", "lat", "lon")
    assert ds.sizes == {"member": 2, "lead": 4, "lat": 3, "lon": 3}
    assert ds["temperature_at_screen_level"].sel(member=0, lead=1).isel(lat=0, lon=0).item() == np.float32(
        10 + 2 + 0.01
    )


def test_lead_fields(table_backend):
    b, cycle = table_backend
    arr = b.lead_fields(["temperature_at_screen_level", "wind_speed_at_10m"], cycle, 3)
    assert arr.shape == (2, 2, 8, 8) and arr.dtype == np.float32
    assert arr[0, 1, 3, 5] == np.float32(1000 + 30 + 3 + 0.05)


@pytest.fixture
def tensor_backend(tmp_path, monkeypatch):
    """Ingest one cycle into a local virtual repo, then give (backend, cycle)."""
    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'src'}")
    importlib.reload(mv)
    try:
        repo = open_virtual_repo(Settings.from_env(), "local", local_path=tmp_path / "repo")
        ingest_cycle_virtual(
            repo,
            store_for(f"file://{tmp_path / 'p'}"),
            cycle,
            cycle,
            workers=2,
            allow_incomplete=True,
            log=lambda *_: None,
        )
        yield TensorBackend.from_repo(repo), cycle
    finally:
        # Leave the virtualization module pointed at the real environment, not a deleted tmp_path.
        monkeypatch.delenv("WXTCO_COPY_URL", raising=False)
        importlib.reload(mv)


def test_tensor_matches_table(table_backend, tensor_backend):
    tb, cycle = table_backend
    xb, _ = tensor_backend
    lat = np.linspace(-89.9, 89.9, 8)[3]
    lon = np.linspace(-179.9, 179.9, 8)[5]
    a = tb.point_series("temperature_at_screen_level", Point(lat, lon), cycle)
    b = xb.point_series("temperature_at_screen_level", Point(lat, lon), cycle)
    np.testing.assert_array_equal(a.values, b.values)
    assert list(b["lead"].values) == [0, 1, 2, 3]
    assert set(b.coords) == {"member", "lead", "valid_time"}
    assert b.dtype == np.float32
    lats = np.linspace(-89.9, 89.9, 8)
    lons = np.linspace(-179.9, 179.9, 8)
    box = Box(lats[2] - 0.01, lats[4] + 0.01, lons[1] - 0.01, lons[3] + 0.01)
    da = tb.box_fields(["temperature_at_screen_level"], box, cycle)["temperature_at_screen_level"]
    db = xb.box_fields(["temperature_at_screen_level"], box, cycle)["temperature_at_screen_level"]
    np.testing.assert_array_equal(
        da.transpose("member", "lead", "lat", "lon").values,
        db.transpose("member", "lead", "lat", "lon").values,
    )
    np.testing.assert_array_equal(
        tb.lead_fields(["wind_speed_at_10m"], cycle, 2), xb.lead_fields(["wind_speed_at_10m"], cycle, 2)
    )


BATCH_VARS = ["temperature_at_screen_level", "wind_speed_at_10m"]


def _check_batch(b, cycle, leads):
    arr = b.batch_fields(BATCH_VARS, [cycle], leads)
    assert arr.shape == (1, len(leads), 2, 2, 8, 8) and arr.dtype == np.float32
    np.testing.assert_array_equal(arr[0], np.stack([b.lead_fields(BATCH_VARS, cycle, x) for x in leads]))


def test_table_batch_fields(table_backend):
    b, cycle = table_backend
    _check_batch(b, cycle, [0, 1, 2, 3])
    # Caller lead order, not SQL sort order.
    _check_batch(b, cycle, [3, 1])
    with pytest.raises(ValueError, match="got 128 rows, expected 256"):
        b.batch_fields(BATCH_VARS, [cycle], [0, 7])
    with pytest.raises(ValueError, match="expected 0"):
        b.batch_fields(BATCH_VARS, [cycle], [7, 0])


CYCLE_2 = "2026/09/16/T0600Z"


def test_table_batch_fields_two_cycles(fixture_root, tmp_path):
    root, cycle = fixture_root
    build_cycle(root, CYCLE_2)
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    for c in (cycle, CYCLE_2):
        ingest_cycle_table(cat, store, store, c, list(FIXTURE_SLUGS), log=lambda *_: None)
    base = TableBackend.local(ensure_table(cat, list(FIXTURE_SLUGS)).metadata_location)
    # Fixture values do not depend on the cycle. Add the init hour, thus the cycle axis order is visible.
    shift = ", ".join(f"{column_name(v)} + hour(init_time) as {column_name(v)}" for v in BATCH_VARS)
    b = TableBackend(base.con, f"(select * replace ({shift}) from {base.table})")
    for cycles, leads in (([cycle, CYCLE_2], [1, 2]), ([CYCLE_2, cycle], [3, 0, 2])):
        arr = b.batch_fields(BATCH_VARS, cycles, leads)
        assert arr.shape == (2, len(leads), 2, 2, 8, 8) and arr.dtype == np.float32
        for ci, c in enumerate(cycles):
            for li, lead in enumerate(leads):
                np.testing.assert_array_equal(arr[ci, li], b.lead_fields(BATCH_VARS, c, lead))
    np.testing.assert_allclose(arr[0] - arr[1], 6, atol=1e-3)
    with pytest.raises(ValueError, match="got 256 rows, expected 512"):
        b.batch_fields(BATCH_VARS, [cycle, "2020/01/01/T0000Z"], [1, 2])


def test_tensor_batch_fields(tensor_backend):
    b, cycle = tensor_backend
    _check_batch(b, cycle, [3, 0, 2])
    with pytest.raises(ValueError, match="lead 7"):
        b.batch_fields(BATCH_VARS, [cycle], [0, 7])
    with pytest.raises(ValueError, match="cycle 2020/01/01/T0000Z not in repo"):
        b.batch_fields(BATCH_VARS, [cycle, "2020/01/01/T0000Z"], [0])


def test_lead_fields_rejects_missing_lead(table_backend):
    b, cycle = table_backend
    with pytest.raises(ValueError, match="lead 7"):
        b.lead_fields(["temperature_at_screen_level"], cycle, 7)


def test_nearest_cell_grid_is_cached(table_backend):
    b, cycle = table_backend
    point = Point(np.linspace(-89.9, 89.9, 8)[3], np.linspace(-179.9, 179.9, 8)[5])
    assert b._grids == {}
    b.point_series("temperature_at_screen_level", point, cycle)
    cached = b._grids[cycle]
    assert [len(a) for a in cached] == [8, 8]
    b.point_series("temperature_at_screen_level", point, cycle)
    # Same tuple object means the second call did not re-run the distinct queries.
    assert b._grids[cycle] is cached


def test_table_backend_file_cache_switch(monkeypatch):
    import duckdb

    from wxtco.queries.table_backend import TableBackend

    monkeypatch.setenv("WXTCO_DUCKDB_FILE_CACHE", "0")
    b = TableBackend(duckdb.connect(), "t")
    assert b.con.execute("select current_setting('enable_external_file_cache')").fetchone()[0] is False
    monkeypatch.delenv("WXTCO_DUCKDB_FILE_CACHE")
    b2 = TableBackend(duckdb.connect(), "t")
    assert b2.con.execute("select current_setting('enable_external_file_cache')").fetchone()[0] is True


def test_positions_gives_slice_for_consecutive_labels():
    from wxtco.queries.tensor_backend import positions

    da = xr.DataArray(np.zeros(6), dims=("lead",), coords={"lead": [0, 1, 2, 3, 4, 5]})
    assert positions(da, "lead", [1, 2, 3]) == slice(1, 4)
    assert positions(da, "lead", [0, 2, 5]) == [0, 2, 5]
    with pytest.raises(KeyError):
        positions(da, "lead", [1, 9])
