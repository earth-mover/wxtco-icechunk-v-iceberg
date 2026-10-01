import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from wxtco.catalog import ensure_table, local_catalog
from wxtco.config import store_for
from wxtco.duck import connect_iceberg
from wxtco.fixture import FIXTURE_SLUGS
from wxtco.ingest.table import files_by_lead, ingest_cycle_table, lead_table
from wxtco.progress import Progress
from wxtco.source import list_cycle, surface_only


def test_files_by_lead(fixture_root):
    root, cycle = fixture_root
    diags = surface_only(list_cycle(store_for(f"file://{root}"), cycle))
    by_lead = files_by_lead(diags)
    assert sorted(by_lead) == [0, 60, 120, 180]
    assert "precipitation_accumulation-PT01H" not in by_lead[0]
    assert set(by_lead[60]) == set(FIXTURE_SLUGS)


def test_lead_table_values_and_nulls(fixture_root):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    by_lead = files_by_lead(surface_only(list_cycle(store, cycle)))
    t = lead_table(store, cycle, 0, by_lead[0], list(FIXTURE_SLUGS))
    assert t.num_rows == 2 * 8 * 8
    assert t.schema.names[:6] == ["init_time", "lead_hours", "valid_time", "member", "lat", "lon"]
    assert pc.all(pc.equal(t["lead_hours"], 0)).as_py()
    assert t["precipitation_accumulation_PT01H"].null_count == t.num_rows
    # row order is member, lat, lon; row for member 1, lat index 3, lon index 5
    row = 1 * 64 + 3 * 8 + 5
    assert t["temperature_at_screen_level"][row].as_py() == np.float32(1000 + 0 + 3 + 0.05)
    assert t["member"][row].as_py() == 1
    assert str(t["init_time"][0].as_py()) == "2026-09-16 00:00:00"


def test_lead_table_rejects_empty_and_subhour(fixture_root):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    by_lead = files_by_lead(surface_only(list_cycle(store, cycle)))
    with pytest.raises(ValueError, match="no files"):
        lead_table(store, cycle, 0, {}, list(FIXTURE_SLUGS))
    with pytest.raises(ValueError, match="whole hour"):
        lead_table(store, cycle, 30, by_lead[0], list(FIXTURE_SLUGS))


def test_ingest_cycle_table_resumable(fixture_root, tmp_path):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    slugs = list(FIXTURE_SLUGS)
    r = ingest_cycle_table(cat, store, store, cycle, slugs, log=lambda *_: None)
    assert r.units_done == 4 and r.units_skipped == 0
    assert r.rows == 4 * 2 * 8 * 8
    again = ingest_cycle_table(cat, store, store, cycle, slugs, log=lambda *_: None)
    assert again.units_done == 0 and again.units_skipped == 4
    assert Progress(store, "table", cycle).units() == {"lead=0", "lead=60", "lead=120", "lead=180"}

    t = ensure_table(cat, slugs)
    con = connect_iceberg()
    n, leads = con.execute(
        f"select count(*), count(distinct lead_hours) from iceberg_scan('{t.metadata_location}')"
    ).fetchone()
    assert n == r.rows and leads == 4
    # one data file per lead
    assert len(list(t.scan().plan_files())) == 4


def test_ingest_cycle_table_snapshot_guard(fixture_root, tmp_path):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    slugs = list(FIXTURE_SLUGS)
    first = ingest_cycle_table(cat, store, store, cycle, slugs, leads=[0], log=lambda *_: None)
    assert first.units_done == 1 and first.units_skipped == 0
    # A lost manifest must not duplicate the data file; the snapshot summary is the truth.
    Progress(store, "table", cycle).clear()
    again = ingest_cycle_table(cat, store, store, cycle, slugs, leads=[0], log=lambda *_: None)
    assert again.units_done == 0 and again.units_skipped == 1
    assert Progress(store, "table", cycle).units() == {"lead=0"}
    assert len(list(ensure_table(cat, slugs).scan().plan_files())) == 1


def test_ingest_cycle_table_rejects_unknown_lead(fixture_root, tmp_path):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    cat = local_catalog(tmp_path / "cat")
    with pytest.raises(ValueError, match="lead minutes"):
        ingest_cycle_table(cat, store, store, cycle, list(FIXTURE_SLUGS), leads=[7], log=lambda *_: None)


def test_lead_table_hilbert_is_a_row_permutation(fixture_root, tmp_path, monkeypatch):
    import importlib

    import pyarrow.compute as pc

    import wxtco.catalog as catalog_mod
    import wxtco.ingest.table as table_mod
    import wxtco.ingest.table_schema as schema_mod

    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    by_lead = files_by_lead(surface_only(list_cycle(store, cycle)))
    base = lead_table(store, cycle, 0, by_lead[0], list(FIXTURE_SLUGS))
    member_table = ensure_table(local_catalog(tmp_path / "member"), list(FIXTURE_SLUGS))
    monkeypatch.setenv("WXTCO_TABLE_SORT", "hilbert")
    mods = (schema_mod, table_mod, catalog_mod)
    for m in mods:
        importlib.reload(m)
    try:
        assert schema_mod.SORT == "hilbert"
        assert schema_mod.TABLE_PROPERTIES["wxtco.sort"] == "hilbert"
        assert catalog_mod.sort_order().fields == []
        # A member-sorted table refuses a Hilbert writer.
        with pytest.raises(ValueError, match="sorted 'member'"):
            catalog_mod.ensure_table(local_catalog(tmp_path / "member"), list(FIXTURE_SLUGS))
        assert member_table.properties["wxtco.sort"] == "member"
        hil = table_mod.lead_table(store, cycle, 0, by_lead[0], list(FIXTURE_SLUGS))
    finally:
        monkeypatch.delenv("WXTCO_TABLE_SORT")
        for m in mods:
            importlib.reload(m)
    assert hil.num_rows == base.num_rows
    assert hil.schema.equals(base.schema)
    # Same rows, different order: sorting both by the key gives identical tables.
    keys = [("member", "ascending"), ("lat", "ascending"), ("lon", "ascending")]
    assert hil.sort_by(keys).equals(base.sort_by(keys))
    assert not hil.equals(base)
    # Member is innermost.
    assert pc.equal(hil["member"].slice(0, 2), pa.array([0, 1], pa.int32())).to_pylist() == [True, True]
    assert hil["lat"][0].as_py() == hil["lat"][1].as_py()


def test_progress_method_for_scratch_table(monkeypatch):
    import importlib

    import wxtco.ingest.table_schema as schema_mod

    assert schema_mod.progress_method() == "table"
    monkeypatch.setenv("WXTCO_TABLE_ID", "mogreps.surface_hilbert")
    importlib.reload(schema_mod)
    try:
        assert schema_mod.progress_method() == "table_surface_hilbert"
    finally:
        monkeypatch.delenv("WXTCO_TABLE_ID")
        importlib.reload(schema_mod)
