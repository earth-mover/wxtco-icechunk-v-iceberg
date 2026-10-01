import pytest

from wxtco.catalog import arraylake_catalog, ensure_table, local_catalog
from wxtco.config import Settings
from wxtco.ingest.table_schema import TABLE_ID


def test_local_catalog_and_ensure_table(tmp_path):
    cat = local_catalog(tmp_path)
    t = ensure_table(cat, ["a_var", "b_var"])
    assert t.name() == tuple(TABLE_ID.split("."))
    assert [f.name for f in t.spec().fields] == ["init_time", "lead_hours"]
    assert [f.source_id for f in t.sort_order().fields] == [4, 5, 6]
    assert t.properties["write.parquet.compression-codec"] == "zstd"
    again = ensure_table(cat, ["a_var", "b_var"])
    assert again.metadata_location == t.metadata_location


def test_ensure_table_rejects_other_slugs(tmp_path):
    cat = local_catalog(tmp_path)
    ensure_table(cat, ["a_var", "b_var"])
    with pytest.raises(ValueError, match="schema mismatch"):
        ensure_table(cat, ["z_var"])


def test_arraylake_catalog_requires_token():
    s = Settings("s", "r", "c", "r", "org", None, "https://api.earthmover.io/iceberg", None)
    with pytest.raises(ValueError):
        arraylake_catalog(s)
