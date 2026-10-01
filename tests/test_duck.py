from wxtco.duck import connect_iceberg


def test_connect_iceberg_loads_extension():
    con = connect_iceberg()
    loaded = {
        name
        for name, installed in con.execute(
            "select extension_name, loaded from duckdb_extensions()"
        ).fetchall()
        if installed
    }
    assert "iceberg" in loaded
