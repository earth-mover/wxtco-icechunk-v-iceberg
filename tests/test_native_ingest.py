"""Native Zarr ingest into the `ts` and `dl` Icechunk layouts, on the synthetic fixture."""

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr
import zarr
from typer.testing import CliRunner
from zarr.storage import WrapperStore

from wxtco.catalog import ensure_table, local_catalog
from wxtco.cli import app
from wxtco.config import Settings, store_for
from wxtco.fixture import FIXTURE_LEADS_MIN, FIXTURE_SLUGS, build_cycle
from wxtco.ingest import native_layout
from wxtco.ingest.native import _init_slot, ingest_cycle_native, open_native_repo
from wxtco.ingest.native_layout import (
    DL_CHUNK,
    DL_DIMS,
    DL_SHARD,
    Q3_VARIABLES,
    STATS,
    STATS_DIMS,
    TS_CHUNK,
    TS_DIMS,
    TS_SHARD,
    ZARR_KWARGS,
    GridSpec,
    append_init,
    array_name,
    clamp_shapes,
    create_dl,
    create_ts,
    dl_variable_order,
    write_region,
)
from wxtco.ingest.table import ingest_cycle_table
from wxtco.queries.base import Box, Point
from wxtco.queries.native_backend import NativeDlBackend, open_native_backend, open_native_dataset
from wxtco.queries.table_backend import TableBackend
from wxtco.slugs import SURFACE_SLUGS, study_slugs

CYCLES = ("2026/09/16/T0000Z", "2026/09/16/T0600Z")
GROUPS = ("ts", "dl")
LATS = np.linspace(-89.9, 89.9, 8)
LONS = np.linspace(-179.9, 179.9, 8)
NAMES = [array_name(s) for s in FIXTURE_SLUGS]
# Production shapes: (init, member, lead, lat, lon) and (init, lead, variable, member, lat, lon).
# Both layouts hold every study slug, so Q1-Q3 compare like for like.
TS_FULL = (7, 18, 171, 960, 1280)
DL_FULL = (7, 171, len(SURFACE_SLUGS), 18, 960, 1280)
# Array metadata captured from the pre-xarray implementation; the rewrite must reproduce it.
GOLDEN = Path(__file__).parent / "native_layout_golden.json"
runner = CliRunner()


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """Two fixture cycles ingested into a local repo per layout. Gives (root, repos, reports, store)."""
    root = tmp_path_factory.mktemp("native")
    for cycle in CYCLES:
        build_cycle(root / "src", cycle)
    store = store_for(f"file://{root / 'src'}")
    progress = store_for(f"file://{root / 'prog'}")
    repos, reports = {}, {}
    for group in GROUPS:
        repo = open_native_repo(Settings.from_env(), group, "local", local_path=root / f"repo_{group}")
        repos[group] = repo
        reports[group] = [
            ingest_cycle_native(
                repo,
                store,
                progress,
                cycle,
                group,
                slugs=list(FIXTURE_SLUGS),
                leads=list(FIXTURE_LEADS_MIN),
                workers=2,
                file_workers=2,
                log=lambda *_: None,
            )
            for cycle in CYCLES
        ]
    return root, repos, reports, store, progress


@pytest.fixture(scope="module")
def table_backend(world):
    """The same two cycles in a local Iceberg table, as the equality reference."""
    root, _, _, store, _ = world
    catalog = local_catalog(root / "cat")
    progress = store_for(f"file://{root / 'tprog'}")
    for cycle in CYCLES:
        ingest_cycle_table(catalog, store, progress, cycle, list(FIXTURE_SLUGS), log=lambda *_: None)
    return TableBackend.local(ensure_table(catalog, list(FIXTURE_SLUGS)).metadata_location)


def test_clamp_shapes_fits_the_fixture_grid():
    shape = (2, 2, 4, 8, 8)
    chunk, shard = clamp_shapes(shape, TS_CHUNK, TS_SHARD)
    assert chunk == (1, 2, 4, 8, 8) and shard == chunk
    assert all(s % c == 0 for s, c in zip(shard, chunk))
    assert all(s <= d for s, d in zip(shard[1:], shape[1:]))


def test_clamp_shapes_keeps_the_full_grid_spec():
    chunk, shard = clamp_shapes(TS_FULL, TS_CHUNK, TS_SHARD)
    assert chunk == TS_CHUNK and shard == TS_SHARD
    # The lead shard is the whole 171-lead axis, 3 inner chunks of 57.
    assert shard[2] == 171 and shard[2] // chunk[2] == 3


def test_clamp_shapes_keeps_the_dl_full_grid_spec():
    v = DL_FULL[2]
    spec_shard = DL_SHARD[:2] + (v,) + DL_SHARD[3:]
    chunk, shard = clamp_shapes(DL_FULL, DL_CHUNK, spec_shard)
    assert chunk == DL_CHUNK and shard == spec_shard
    # One shard per sample: all 81 variables, 81 x 4 x 4 = 1296 inner chunks of 5.5 MB.
    assert [s // c for s, c in zip(shard, chunk)] == [1, 1, v, 1, 4, 4]
    assert v == 81 and np.prod(shard) * 4 == 7_166_361_600
    # A sample write covers whole shards, so Zarr never read-modify-writes a stored shard.
    assert all(d % s == 0 for d, s in zip(DL_FULL[1:], shard[1:]))
    assert all(d % c == 0 for d, c in zip(DL_FULL[1:], chunk[1:]))


def test_ts_full_grid_writes_complete_shards():
    """Every shard write must be a whole shard, or Zarr read-modify-writes the stored shard."""
    chunk, shard = clamp_shapes(TS_FULL, TS_CHUNK, TS_SHARD)
    assert all(d % c == 0 for d, c in zip(TS_FULL[1:], chunk[1:]))
    assert all(d % s == 0 for d, s in zip(TS_FULL[1:], shard[1:]))
    assert all(s % c == 0 for s, c in zip(shard, chunk))


def test_clamp_shapes_rejects_a_rank_mismatch():
    with pytest.raises(ValueError, match="rank mismatch"):
        clamp_shapes((1, 2), TS_CHUNK, TS_SHARD)


@pytest.mark.parametrize("group", GROUPS)
def test_two_cycles_append_and_decode(world, group):
    _, repos, reports, _, _ = world
    ds = open_native_dataset(repos[group], group)
    assert ds.sizes["init"] == 2 and ds.sizes["lead"] == 4
    assert ds["init"].dtype == np.dtype("datetime64[ns]")
    assert np.issubdtype(ds["lead"].dtype, np.timedelta64)
    assert ds["time"].dtype == np.dtype("datetime64[ns]")
    assert set(NAMES) <= set(ds.data_vars)
    assert ds["temperature_at_screen_level"].isel(init=1).max().item() == np.float32(1000 + 30 + 7 + 0.07)
    # The fixture writes no hourly precipitation at lead 0, so that sample is NaN.
    precip = ds["precipitation_accumulation_PT01H"].isel(init=0).sel(lead=np.timedelta64(0, "s"))
    assert bool(np.isnan(precip).all())
    # One commit for the layout, then one per cycle: 3 on top of the empty repo.
    assert [r.commits for r in reports[group]] == [2, 1]
    assert len(list(repos[group].ancestry(branch="main"))) == 4
    assert all(r.units_done and not r.units_skipped for r in reports[group])


@pytest.mark.parametrize("group", GROUPS)
def test_rerunning_a_cycle_is_a_no_op(world, group):
    _, repos, reports, store, progress = world
    before = len(list(repos[group].ancestry(branch="main")))
    again = ingest_cycle_native(
        repos[group],
        store,
        progress,
        CYCLES[1],
        group,
        slugs=list(FIXTURE_SLUGS),
        leads=list(FIXTURE_LEADS_MIN),
        workers=2,
        file_workers=2,
        log=lambda *_: None,
    )
    assert again.units_done == 0 and again.units_skipped == reports[group][1].units_done
    assert again.commits == 0
    assert len(list(repos[group].ancestry(branch="main"))) == before


@pytest.mark.parametrize("group", GROUPS)
def test_native_matches_the_table(world, table_backend, group):
    _, repos, _, _, _ = world
    native = open_native_backend(repos[group], group)
    cycle = CYCLES[1]
    point = Point(LATS[3], LONS[5])
    a = table_backend.point_series("temperature_at_screen_level", point, cycle)
    b = native.point_series("temperature_at_screen_level", point, cycle)
    np.testing.assert_array_equal(a.values, b.values)
    assert list(b["lead"].values) == [0, 1, 2, 3] and set(b.coords) == {"member", "lead", "valid_time"}
    np.testing.assert_array_equal(a["valid_time"].values, b["valid_time"].values)

    box = Box(LATS[2] - 0.01, LATS[4] + 0.01, LONS[1] - 0.01, LONS[3] + 0.01)
    variables = ["temperature_at_screen_level", "wind_speed_at_10m"]
    da = table_backend.box_fields(variables, box, cycle)
    db = native.box_fields(variables, box, cycle)
    for name in variables:
        np.testing.assert_array_equal(
            da[name].transpose("member", "lead", "lat", "lon").values,
            db[name].transpose("member", "lead", "lat", "lon").values,
        )
    np.testing.assert_array_equal(
        table_backend.lead_fields(variables, cycle, 2), native.lead_fields(variables, cycle, 2)
    )


@pytest.mark.parametrize("group", GROUPS)
def test_native_batch_fields_match_lead_fields(world, table_backend, group):
    """The bulk read gives the per-sample values, for both cycles, in the order the caller gives."""
    _, repos, _, _, _ = world
    native = open_native_backend(repos[group], group)
    assert isinstance(native, NativeDlBackend) == (group == "dl")
    variables = ["wind_speed_at_10m", "temperature_at_screen_level"]
    cycles, leads = [CYCLES[1], CYCLES[0]], [2, 1]
    arr = native.batch_fields(variables, cycles, leads)
    assert arr.shape == (2, 2, 2, 2, 8, 8) and arr.dtype == np.float32
    for ci, cycle in enumerate(cycles):
        for li, lead in enumerate(leads):
            np.testing.assert_array_equal(arr[ci, li], table_backend.lead_fields(variables, cycle, lead))
    with pytest.raises(ValueError, match="lead 9"):
        native.batch_fields(variables, cycles, [1, 9])


def test_dl_statistics_match_numpy(world, table_backend):
    _, repos, _, _, _ = world
    session = repos["dl"].readonly_session("main")
    stats = {name: zarr.open_array(session.store, path=name)[0, 1] for name in STATS}
    # Lead index 1 is +60 min, where every fixture variable has a file.
    block = table_backend.lead_fields(NAMES, CYCLES[0], 1).astype("float64")
    flat = block.reshape(len(NAMES), -1)
    np.testing.assert_array_equal(stats["count"], np.full(len(NAMES), flat.shape[1]))
    np.testing.assert_allclose(stats["sums"], flat.sum(axis=1))
    np.testing.assert_allclose(stats["squares"], (flat * flat).sum(axis=1))
    np.testing.assert_array_equal(stats["minimum"], flat.min(axis=1).astype("float32"))
    np.testing.assert_array_equal(stats["maximum"], flat.max(axis=1).astype("float32"))
    # Lead 0 has no hourly precipitation: no values counted, no extremes.
    lead0 = {name: zarr.open_array(session.store, path=name)[0, 0] for name in STATS}
    i = NAMES.index("precipitation_accumulation_PT01H")
    assert lead0["count"][i] == 0 and lead0["sums"][i] == 0.0
    assert np.isnan(lead0["minimum"][i]) and np.isnan(lead0["maximum"][i])


@pytest.mark.parametrize(("group", "path"), [("ts", "temperature_at_screen_level"), ("dl", "data")])
def test_arrays_are_sharded_pcodec(world, group, path):
    _, repos, _, _, _ = world
    root = zarr.open_group(repos[group].readonly_session("main").store, mode="r")
    codecs = root[path].metadata.to_dict()["codecs"]
    assert codecs[0]["name"] == "sharding_indexed"
    assert [c["name"] for c in codecs[0]["configuration"]["codecs"]] == ["numcodecs.pcodec"]
    assert json.loads(str(root.attrs["wxtco.slugs"])) == list(FIXTURE_SLUGS)


class _CountingStore(WrapperStore):
    """Record every key a read asks for and every key written, to count the objects touched."""

    def __init__(self, store):
        super().__init__(store)
        self.keys: list[str] = []
        self.puts: list[str] = []

    async def set(self, key, value):
        self.puts.append(key)
        return await self._store.set(key, value)

    async def get(self, key, prototype, byte_range=None):
        self.keys.append(key)
        return await self._store.get(key, prototype, byte_range)

    async def get_partial_values(self, prototype, key_ranges):
        pairs = list(key_ranges)
        self.keys.extend(k for k, _ in pairs)
        return await self._store.get_partial_values(prototype, pairs)


def _built(build, slugs=FIXTURE_SLUGS, members=18):
    """Write one layout template to a fresh in-memory store and give the opened group."""
    store = zarr.storage.MemoryStore()
    build(store, GridSpec(members, LATS, LONS, FIXTURE_LEADS_MIN), slugs)
    return zarr.open_group(store, mode="a")


def test_both_layouts_keep_the_morton_sub_chunk_order():
    """Decision 011: no write-order override, because Zarr never stores one."""
    dl, ts = _built(create_dl), _built(create_ts)
    data = dl["data"]
    # One shard per sample holding every variable.
    assert data.shards == (1, 1, len(FIXTURE_SLUGS), 18, LATS.size, LONS.size)
    assert data.chunks == (1, 1, 1, 18, LATS.size, LONS.size)
    assert data.metadata.codecs[0].subchunk_write_order == "morton"
    assert ts["wind_speed_at_10m"].metadata.codecs[0].subchunk_write_order == "morton"
    # The write-order machinery is gone: no override constant and no reopen helper.
    assert not [n for n in dir(native_layout) if "WRITE_ORDER" in n]
    assert not hasattr(native_layout, "open_dl_data")


def _plain(value):
    """JSON-ready copy of a metadata value, as the golden capture stored it."""
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def _stored_metadata(group):
    """The metadata of every array, normalised the way the golden capture was taken."""
    out = {}
    for name in sorted(group.array_keys()):
        meta = json.loads(json.dumps(group[name].metadata.to_dict(), default=_plain))
        # xarray names the non-index coordinate on each data variable; the old writer did not.
        meta["attributes"] = {k: v for k, v in meta["attributes"].items() if k != "coordinates"}
        out[name] = meta
    return out


@pytest.mark.parametrize(("kind", "build"), [("ts", create_ts), ("dl", create_dl)])
def test_layout_metadata_matches_the_golden_capture(kind, build):
    """The xarray templates must write the same codecs, dims, fill values and shapes as before."""
    golden = json.loads(GOLDEN.read_text())[kind]
    written = _built(build)
    assert _stored_metadata(written) == golden["arrays"]
    assert dict(written.attrs) == golden["attrs"]


def test_ts_region_write_reads_no_chunk(world):
    """A slab spans whole shards, so Zarr encodes it without reading the stored shard back."""
    _, repos, _, _, _ = world
    session = repos["ts"].writable_session("main")
    spy = _CountingStore(session.store)
    sizes = open_native_dataset(repos["ts"], "ts").sizes
    block = np.zeros((1, sizes["member"], sizes["lead"], sizes["lat"], sizes["lon"]), "float32")
    spy.keys.clear()
    write_region(spy, xr.Dataset({"wind_speed_at_10m": (TS_DIMS, block)}), {"init": slice(0, 1)})
    assert not [k for k in spy.keys if k.startswith("wind_speed_at_10m/c")]


def test_dl_region_write_reads_no_chunk(world):
    """The `dl` sample goes through the same xarray region write: one whole shard, no read-back."""
    _, repos, _, _, _ = world
    session = repos["dl"].writable_session("main")
    spy = _CountingStore(session.store)
    shape = zarr.open_group(repos["dl"].readonly_session("main").store, mode="r")["data"].shape
    v = shape[2]
    block = np.zeros((1, 1, *shape[2:]), "float32")
    rows = {n: (STATS_DIMS, np.zeros((1, 1, v), STATS[n])) for n in STATS}
    spy.keys.clear()
    slab = xr.Dataset({"data": (DL_DIMS, block)} | rows)
    write_region(spy, slab, {"init": slice(0, 1), "lead": slice(1, 2)})
    assert not [k for k in spy.keys if k.startswith("data/c")]


def test_append_init_grows_every_array_without_writing_data(world):
    """The per-cycle append is a Dask `compute=False` write: metadata only, the new slot reads NaN."""
    _, repos, _, _, _ = world
    spy = _CountingStore(repos["ts"].writable_session("main").store)
    ds = xr.open_zarr(spy, chunks=None, **ZARR_KWARGS)
    before = ds.sizes["init"]
    spy.puts.clear()
    append_init(spy, ds, np.datetime64("2026-09-17T00:00:00", "s"))
    grown = xr.open_zarr(spy, chunks=None, **ZARR_KWARGS)
    assert grown.sizes["init"] == before + 1
    assert all(v.shape[0] == before + 1 for v in grown.data_vars.values())
    # Only the two coordinates store a chunk; no data array does.
    written = {k.split("/c/")[0] for k in spy.puts if "/c/" in k}
    assert written == {"init", "valid_time"}
    assert bool(np.isnan(grown["wind_speed_at_10m"].isel(init=-1).values).all())


def test_dl_one_variable_read_touches_one_shard(world):
    """A variable subset of one sample stays inside the sample shard: byte ranges, one object."""
    _, repos, _, _, _ = world
    spy = _CountingStore(repos["dl"].readonly_session("main").store)
    fake_repo = SimpleNamespace(readonly_session=lambda *_: SimpleNamespace(store=spy))
    ds = open_native_dataset(fake_repo, "dl")
    spy.keys.clear()
    values = ds["wind_speed_at_10m"].isel(init=0, lead=1).values
    assert np.isfinite(values).all()
    touched = {k for k in spy.keys if k.startswith("data/c")}
    assert touched and len(touched) == 1


def test_dl_variable_coord_puts_q3_first():
    """The Q3 variables lead the variable axis, so a Q3 read is one span inside a sample shard."""
    full = _built(create_dl, study_slugs())
    q3 = [array_name(s) for s in Q3_VARIABLES]
    assert list(full["variable"][:4]) == q3
    assert full["data"].shape[2] == len(SURFACE_SLUGS)
    assert sorted(full["variable"][:]) == sorted(array_name(s) for s in SURFACE_SLUGS)
    # A narrowed `WXTCO_SLUGS` list keeps the rule and adds no variable it does not name.
    picked = ["landsea_mask", "relative_humidity_at_screen_level", "wind_speed_at_10m"]
    narrow = _built(create_dl, picked)
    assert list(narrow["variable"][:]) == [
        "wind_speed_at_10m",
        "relative_humidity_at_screen_level",
        "landsea_mask",
    ]


def test_dl_reorders_the_ingest_block(world, table_backend, tmp_path):
    """The block variable axis follows the stored coordinate, whatever order the caller gives."""
    _, _, _, store, _ = world
    scrambled = list(reversed(FIXTURE_SLUGS))
    repo = open_native_repo(Settings.from_env(), "dl", "local", local_path=tmp_path / "repo_dl_order")
    ingest_cycle_native(
        repo,
        store,
        store_for(f"file://{tmp_path / 'prog'}"),
        CYCLES[0],
        "dl",
        slugs=scrambled,
        leads=list(FIXTURE_LEADS_MIN),
        workers=2,
        file_workers=2,
        log=lambda *_: None,
    )
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert list(root["variable"][:]) == [array_name(s) for s in dl_variable_order(scrambled)]
    assert list(root["variable"][:2]) == ["temperature_at_screen_level", "wind_speed_at_10m"]
    # Reordering the axis must not move the values: every variable still matches the table.
    ds = open_native_dataset(repo, "dl")
    expected = table_backend.lead_fields(NAMES, CYCLES[0], 1)
    for i, name in enumerate(NAMES):
        np.testing.assert_array_equal(ds[name].isel(init=0, lead=1).values, expected[i])


def test_dl_variable_list_is_frozen(world):
    _, repos, _, store, progress = world
    with pytest.raises(ValueError, match="frozen"):
        ingest_cycle_native(
            repos["dl"],
            store,
            progress,
            CYCLES[0],
            "dl",
            slugs=["wind_speed_at_10m"],
            leads=list(FIXTURE_LEADS_MIN),
            log=lambda *_: None,
        )


@pytest.mark.parametrize("group", GROUPS)
def test_cli_ingest_and_bench(tmp_path, monkeypatch, group):
    cycle = CYCLES[0]
    build_cycle(tmp_path / "src", cycle)
    # Settings reads `.env` from the CWD, so keep the CWD off the repo.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path / 'src'}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    repo = tmp_path / f"repo_{group}"
    args = ["ingest", "native", "--group", group, "--cycle", cycle, "--local", str(repo), "--workers", "2"]
    r = runner.invoke(app, args)
    assert r.exit_code == 0, r.output
    # The layout commit plus the cycle commit.
    assert "units done" in r.output and "(2 commits)" in r.output
    again = runner.invoke(app, args)
    assert again.exit_code == 0, again.output
    assert "0 units done" in again.output

    params = json.dumps({"lat": float(LATS[3]), "lon": float(LONS[5]), "cycle": cycle})
    out = tmp_path / "bench" / f"{group}.csv"
    b = runner.invoke(
        app,
        [
            "bench",
            "--method",
            f"native_{group}",
            "--query",
            "q1",
            "--local",
            str(repo),
            "--params",
            params,
            "--runs",
            "1",
            "--out",
            str(out),
        ],
    )
    assert b.exit_code == 0, b.output
    assert out.exists() and f"native_{group} q1" in b.output


def _init_axis(values):
    """The stored init axis, as `_init_slot` reads it off an opened Dataset."""
    return np.asarray(values, dtype="int64")


def test_init_slot_reuses_an_older_present_cycle():
    axis = _init_axis([100, 200, 300])
    assert _init_slot(axis, 100) == (0, False)
    assert _init_slot(axis, 200) == (1, False)
    assert _init_slot(axis, 400) == (3, True)


def test_init_slot_rejects_an_absent_older_cycle():
    with pytest.raises(ValueError, match="chronological order"):
        _init_slot(_init_axis([100, 300]), 200)


def test_dl_rejects_a_variable_subset(world):
    _, repos, _, store, progress = world
    with pytest.raises(ValueError, match="one shard holds all variables"):
        ingest_cycle_native(
            repos["dl"],
            store,
            progress,
            CYCLES[0],
            "dl",
            slugs=list(FIXTURE_SLUGS),
            variables=["wind_speed_at_10m"],
            leads=list(FIXTURE_LEADS_MIN),
            log=lambda *_: None,
        )


def test_reports_carry_coverage_counts(world):
    _, _, reports, _, _ = world
    for group in GROUPS:
        # The fixture leads are all on the axis; only hourly precipitation at lead 0 has no file.
        assert [r.leads_dropped for r in reports[group]] == [0, 0]
        assert [r.missing_files for r in reports[group]] == [1, 1]


def _ts_cli(tmp_path, cycle, repo, slugs, variables=None):
    """Run one `ingest native --group ts` with an explicit study slug list."""
    args = ["ingest", "native", "--group", "ts", "--cycle", cycle, "--local", str(repo), "--workers", "2"]
    if variables:
        args += ["--variables", variables]
    return runner.invoke(app, args, env={"WXTCO_SLUGS": ",".join(slugs)})


def test_ts_variables_adds_an_array(tmp_path, monkeypatch):
    cycle = CYCLES[0]
    build_cycle(tmp_path / "src", cycle)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path / 'src'}")
    repo = tmp_path / "repo_ts"
    first, late = list(FIXTURE_SLUGS[:2]), FIXTURE_SLUGS[2]
    r = _ts_cli(tmp_path, cycle, repo, first)
    assert r.exit_code == 0, r.output
    # The late slug joins the repo and only its unit runs; the other arrays keep their data.
    again = _ts_cli(tmp_path, cycle, repo, [*first, late], variables=late)
    assert again.exit_code == 0, again.output
    assert "1 units done" in again.output

    root = zarr.open_group(
        open_native_repo(Settings.from_env(), "ts", "local", repo).readonly_session("main").store, mode="r"
    )
    assert json.loads(str(root.attrs["wxtco.slugs"])) == [*first, late]
    added = root[array_name(late)]
    assert added.shape[0] == 1 and bool(np.isfinite(added[0, :, 1]).all())
    assert bool(np.isfinite(root["temperature_at_screen_level"][0, :, 0]).all())


def test_cli_rejects_variables_for_dl(tmp_path, monkeypatch):
    cycle = CYCLES[0]
    build_cycle(tmp_path / "src", cycle)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path / 'src'}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    r = runner.invoke(
        app,
        [
            "ingest",
            "native",
            "--group",
            "dl",
            "--cycle",
            cycle,
            "--local",
            str(tmp_path / "r"),
            "--variables",
            "wind_speed_at_10m",
        ],
    )
    assert r.exit_code != 0
    assert "would blank the rest" in r.output


def test_promote_valid_time_handles_pre_rewrite_repos():
    import numpy as np
    import xarray as xr

    from wxtco.queries.native_backend import promote_valid_time

    ds = xr.Dataset(
        {
            "x": (("init", "lead"), np.zeros((1, 2))),
            "valid_time": (("init", "lead"), np.zeros((1, 2), "int64")),
        }
    )
    out = promote_valid_time(ds)
    assert "valid_time" in out.coords and "valid_time" not in out.data_vars
    assert promote_valid_time(out) is out or "valid_time" in promote_valid_time(out).coords


def test_stale_progress_marks_are_ignored_for_a_new_repo(tmp_path, monkeypatch):
    """A recreated repo must re-ingest cycles whose manifests survived in the progress store."""
    import icechunk as ic

    from wxtco.config import store_for
    from wxtco.fixture import FIXTURE_SLUGS, build_cycle
    from wxtco.ingest.native import ingest_cycle_native
    from wxtco.progress import Progress

    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    store = store_for(f"file://{tmp_path / 'src'}")
    prog = store_for(f"file://{tmp_path / 'prog'}")
    slugs = list(FIXTURE_SLUGS)
    leads = [0, 60, 120, 180]
    repo1 = ic.Repository.open_or_create(ic.local_filesystem_storage(str(tmp_path / "repo1")))
    r1 = ingest_cycle_native(
        repo1, store, prog, cycle, "ts", slugs=slugs, leads=leads, workers=1, log=lambda *_: None
    )
    assert r1.units_done == len(slugs)
    assert Progress(prog, "native_ts", cycle).units()
    # A brand-new repo with the old manifests still in place: the marks are stale and must not skip work.
    repo2 = ic.Repository.open_or_create(ic.local_filesystem_storage(str(tmp_path / "repo2")))
    r2 = ingest_cycle_native(
        repo2, store, prog, cycle, "ts", slugs=slugs, leads=leads, workers=1, log=lambda *_: None
    )
    assert r2.units_done == len(slugs) and r2.units_skipped == 0
    # And a plain rerun on the same repo is still a no-op.
    r3 = ingest_cycle_native(
        repo2, store, prog, cycle, "ts", slugs=slugs, leads=leads, workers=1, log=lambda *_: None
    )
    assert r3.units_done == 0 and r3.units_skipped == len(slugs)
