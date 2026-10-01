import csv
import importlib
import json
from pathlib import Path

import numpy as np
import pytest
from typer.testing import CliRunner

import wxtco.ingest.mogreps_virtual as mv
import wxtco.queries.netcdf_backend as nb
from wxtco.catalog import ensure_table, local_catalog
from wxtco.cli import app
from wxtco.config import Settings, store_for
from wxtco.fixture import FIXTURE_SLUGS, build_cycle
from wxtco.ingest.table import ingest_cycle_table
from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_repo
from wxtco.queries.base import Box, Point
from wxtco.queries.netcdf_backend import DownloadBackend, FuseBackend
from wxtco.queries.table_backend import TableBackend
from wxtco.queries.tensor_backend import TensorBackend

LATS = np.linspace(-89.9, 89.9, 8)
LONS = np.linspace(-179.9, 179.9, 8)
VARS = ["temperature_at_screen_level", "wind_speed_at_10m"]
# Different lead schedules: the accumulation has no lead 0.
MIXED_VARS = ["temperature_at_screen_level", "precipitation_accumulation-PT01H"]
BOX = Box(LATS[2] - 0.01, LATS[4] + 0.01, LONS[1] - 0.01, LONS[3] + 0.01)
runner = CliRunner()


@pytest.fixture
def backends(fixture_root, tmp_path):
    """Give (download, fuse, table, cycle) on the same synthetic cycle."""
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    ingest_cycle_table(cat, store, store, cycle, list(FIXTURE_SLUGS), log=lambda *_: None)
    table = TableBackend.local(ensure_table(cat, list(FIXTURE_SLUGS)).metadata_location)
    return DownloadBackend(store), FuseBackend(store, mount_root=root), table, cycle


def test_point_series_matches_table(backends):
    down, fuse, table, cycle = backends
    point = Point(LATS[3], LONS[5])
    want = table.point_series(VARS[0], point, cycle)
    for b in (down, fuse):
        got = b.point_series(VARS[0], point, cycle)
        assert got.dims == ("member", "lead")
        assert set(got.coords) == {"member", "lead", "valid_time"}
        assert got.dtype == np.float32
        assert list(got["lead"].values) == [0, 1, 2, 3]
        assert got.sel(member=1, lead=2).item() == np.float32(1000 + 20 + 3 + 0.05)
        np.testing.assert_array_equal(got.values, want.values)
        np.testing.assert_array_equal(
            got["valid_time"].values.astype("datetime64[ns]"), want["valid_time"].values
        )


def test_box_fields_matches_table(backends):
    down, fuse, table, cycle = backends
    box = Box(LATS[2] - 0.01, LATS[4] + 0.01, LONS[1] - 0.01, LONS[3] + 0.01)
    want = table.box_fields(VARS, box, cycle)
    for b in (down, fuse):
        got = b.box_fields(VARS, box, cycle)
        assert got.sizes == {"member": 2, "lead": 4, "lat": 3, "lon": 3}
        assert got[VARS[0]].dims == ("member", "lead", "lat", "lon")
        assert got[VARS[0]].sel(member=0, lead=1).isel(lat=0, lon=0).item() == np.float32(10 + 2 + 0.01)
        for v in VARS:
            np.testing.assert_array_equal(
                got[v].values, want[v].transpose("member", "lead", "lat", "lon").values
            )


def test_lead_fields_matches_table(backends):
    down, fuse, table, cycle = backends
    want = table.lead_fields(VARS, cycle, 3)
    for b in (down, fuse):
        got = b.lead_fields(VARS, cycle, 3)
        assert got.shape == (2, 2, 8, 8) and got.dtype == np.float32
        assert got[0, 1, 3, 5] == np.float32(1000 + 30 + 3 + 0.05)
        np.testing.assert_array_equal(got, want)


def test_download_releases_its_temp_dir(backends):
    down, _, _, cycle = backends
    for call in (
        lambda: down.point_series(VARS[0], Point(LATS[3], LONS[5]), cycle),
        lambda: down.lead_fields(VARS, cycle, 1),
    ):
        call()
        # Nothing is cached between queries: the dir is gone and the bytes are of the last query only.
        assert down._dir is None
        assert down.bytes_read() > 0


def test_missing_lead_and_var_raise(backends):
    down, fuse, _, cycle = backends
    for b in (down, fuse):
        with pytest.raises(ValueError, match="lead 7"):
            b.lead_fields([VARS[0]], cycle, 7)
        with pytest.raises(ValueError, match="nope"):
            b.point_series("nope", Point(LATS[3], LONS[5]), cycle)
        with pytest.raises(ValueError, match="2026/09/17/T0000Z"):
            b.point_series(VARS[0], Point(LATS[3], LONS[5]), "2026/09/17/T0000Z")


def test_fuse_reports_a_missing_path(fixture_root, tmp_path):
    root, cycle = fixture_root
    b = FuseBackend(store_for(f"file://{root}"), mount_root=tmp_path / "not-mounted")
    with pytest.raises(FileNotFoundError, match="not-mounted"):
        b.point_series(VARS[0], Point(LATS[3], LONS[5]), cycle)


def test_bench_q1_download_local(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    # Settings reads `.env` from the CWD, so keep the CWD off the repo.
    monkeypatch.chdir(tmp_path)
    params = {"lat": float(LATS[3]), "lon": float(LONS[5]), "cycle": cycle}
    out = tmp_path / "bench" / "q1.csv"
    r = runner.invoke(
        app,
        [
            "bench",
            "--method",
            "download",
            "--query",
            "q1",
            "--local",
            str(root),
            "--params",
            json.dumps(params),
            "--runs",
            "2",
            "--out",
            str(out),
        ],
    )
    assert r.exit_code == 0, r.output
    with Path(out).open() as f:
        recs = list(csv.DictReader(f))
    assert [rec["method"] for rec in recs] == ["download", "download"]
    assert all(int(rec["bytes_read"]) > 0 for rec in recs)


def test_download_backend_transfer_validation(fixture_root):
    root, _ = fixture_root
    store = store_for(f"file://{root}")
    with pytest.raises(ValueError, match="expected obstore or crt"):
        DownloadBackend(store, transfer="rsync")
    with pytest.raises(ValueError, match="needs an S3 bucket"):
        DownloadBackend(store, transfer="crt")
    b = DownloadBackend(store, transfer="crt", bucket="b", prefix="/netcdf/", region="us-east-1")
    assert b.name == "download_crt" and b.prefix == "netcdf"
    assert DownloadBackend(store).name == "download"


@pytest.fixture
def tensor(fixture_root, tmp_path, monkeypatch):
    """Ingest the same synthetic cycle into a local virtual repo; give (TensorBackend, cycle)."""
    root, cycle = fixture_root
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{root}")
    importlib.reload(mv)
    try:
        repo = open_virtual_repo(Settings.from_env(), "local", local_path=tmp_path / "repo")
        ingest_cycle_virtual(
            repo,
            store_for(f"file://{tmp_path / 'progress'}"),
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


def test_matches_tensor_backend(backends, tensor):
    """The no-ETL backends must give the tensor backend's answer for all three queries."""
    down, fuse, _, cycle = backends
    xb, _ = tensor
    point = Point(LATS[3], LONS[5])
    want_point = xb.point_series(VARS[0], point, cycle)
    want_box = xb.box_fields(VARS, BOX, cycle)
    want_lead = xb.lead_fields(VARS, cycle, 3)
    for b in (down, fuse):
        got = b.point_series(VARS[0], point, cycle)
        assert got.dims == want_point.dims and set(got.coords) == set(want_point.coords)
        np.testing.assert_array_equal(got.values, want_point.values)
        np.testing.assert_array_equal(got["valid_time"].values, want_point["valid_time"].values)
        got_box = b.box_fields(VARS, BOX, cycle)
        assert got_box.sizes == want_box.sizes
        # Same time coord name as the tensor backend: `time` in a box, `valid_time` at a point.
        assert "time" in got_box.coords
        np.testing.assert_array_equal(got_box["time"].values, want_box["time"].values)
        for v in VARS:
            np.testing.assert_array_equal(got_box[v].values, want_box[v].values)
        np.testing.assert_array_equal(b.lead_fields(VARS, cycle, 3), want_lead)


def test_failed_download_leaves_no_temp_dir(backends, tmp_path, monkeypatch):
    """A fetch that raises must still release the temp dir it made."""
    down, _, _, cycle = backends
    temp = tmp_path / "scratch"
    temp.mkdir()
    monkeypatch.setattr(nb.tempfile, "tempdir", str(temp))

    async def boom(*args, **kwargs):
        raise RuntimeError("fetch failed")

    monkeypatch.setattr(nb.obs, "get_async", boom)
    with pytest.raises(RuntimeError, match="fetch failed"):
        down.point_series(VARS[0], Point(LATS[3], LONS[5]), cycle)
    assert down._dir is None
    assert list(temp.glob("wxtco-download-*")) == []


def test_box_fields_over_mixed_lead_schedules(backends):
    """Variables with unequal lead schedules must intersect, not raise a merge error."""
    down, fuse, _, cycle = backends
    for b in (down, fuse):
        ds = b.box_fields(MIXED_VARS, BOX, cycle)
        # The hourly accumulation starts at lead 1, so leads 1-3 are shared.
        assert list(ds["lead"].values) == [1, 2, 3]
        assert ds.sizes == {"member": 2, "lead": 3, "lat": 3, "lon": 3}
        assert set(ds.data_vars) == set(MIXED_VARS)


def test_box_fields_without_shared_leads_raises(tmp_path):
    """No lead in common is an error, not an empty result."""
    root = tmp_path / "disjoint"
    cycle = "2026/09/16/T0000Z"
    build_cycle(root, cycle, slugs=("temperature_at_screen_level",), leads=(0,))
    build_cycle(root, cycle, slugs=("precipitation_accumulation-PT01H",), leads=(60,))
    b = DownloadBackend(store_for(f"file://{root}"))
    # Drop the one shared lead by asking for the two disjoint single-lead diagnostics.
    with pytest.raises(ValueError, match="share no lead"):
        b.box_fields(MIXED_VARS, BOX, cycle)


def test_sub_hour_lead_raises(tmp_path):
    """A 30-minute lead would truncate to a duplicate lead index, so it must raise."""
    root = tmp_path / "half-hour"
    cycle = "2026/09/16/T0000Z"
    build_cycle(root, cycle, slugs=("temperature_at_screen_level",), leads=(0, 30))
    b = DownloadBackend(store_for(f"file://{root}"))
    with pytest.raises(ValueError, match="whole number of hours"):
        b.point_series(VARS[0], Point(LATS[3], LONS[5]), cycle)


def test_cli_fuse_needs_a_fuse_root(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("WXTCO_FUSE_ROOT", raising=False)
    params = {"lat": float(LATS[3]), "lon": float(LONS[5]), "cycle": cycle}
    r = runner.invoke(
        app,
        [
            "bench",
            "--method",
            "fuse",
            "--query",
            "q1",
            "--local",
            str(root),
            "--params",
            json.dumps(params),
            "--runs",
            "1",
            "--out",
            str(tmp_path / "fuse.csv"),
        ],
    )
    assert r.exit_code != 0
    assert "--fuse-root" in r.output


def test_bytes_read_accumulates_then_resets(backends):
    """A query that materializes twice must report both fetches, and reading must reset."""
    down, _, _, cycle = backends
    down.box_fields(VARS, BOX, cycle)
    one = down.bytes_read()
    assert one > 0
    down.box_fields(VARS, BOX, cycle)
    down.box_fields(VARS, BOX, cycle)
    assert down.bytes_read() == 2 * one
    assert down.bytes_read() == 0


def test_list_seconds_accumulates_then_resets(backends):
    """The LIST cost is recorded per query, outside the bench CSV columns."""
    down, _, _, cycle = backends
    assert down.list_seconds() == 0.0
    down.point_series(VARS[0], Point(LATS[3], LONS[5]), cycle)
    assert down.last_list_seconds > 0
    assert down.list_seconds() > 0
    assert down.list_seconds() == 0.0


def test_batch_fields_one_transfer_matches_lead_fields(tmp_path):
    """The batch path lists once per cycle and matches a stack of `lead_fields`."""
    import numpy as np

    from wxtco.config import store_for
    from wxtco.fixture import build_cycle
    from wxtco.queries.netcdf_backend import DownloadBackend

    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    b = DownloadBackend(store_for(f"file://{tmp_path / 'src'}"), concurrency=4)
    vars = ["temperature_at_screen_level", "wind_speed_at_10m"]
    calls = []
    orig = b._materialize
    b._materialize = lambda files: (calls.append(len(files)), orig(files))[1]
    arr = b.batch_fields(vars, [cycle], [3, 1])
    assert calls == [4] and arr.shape[:3] == (1, 2, 2) and arr.dtype == np.float32
    for li, lead in enumerate([3, 1]):
        np.testing.assert_array_equal(arr[0, li], b.lead_fields(vars, cycle, lead))
