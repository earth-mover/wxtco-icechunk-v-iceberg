import pyarrow as pa

from wxtco.ingest.table_schema import KEY_COLUMNS, arrow_schema, column_name, iceberg_schema, partition_spec


def test_column_name():
    assert column_name("precipitation_accumulation-PT01H") == "precipitation_accumulation_PT01H"


def test_schema_shapes():
    slugs = ["b_var", "a_var-PT01H"]
    s = iceberg_schema(slugs)
    names = [f.name for f in s.fields]
    assert names[:6] == list(KEY_COLUMNS)
    assert names[6:] == ["b_var", "a_var_PT01H"]  # slug order preserved
    assert all(f.required for f in s.fields[:6]) and not any(f.required for f in s.fields[6:])
    a = arrow_schema(slugs)
    assert a.field("lead_hours").type == pa.int32()
    assert a.field("lat").type == pa.float32()
    assert a.field("b_var").type == pa.float32()
    assert a.field("init_time").type == pa.timestamp("us")


def test_partition_spec():
    spec = partition_spec()
    assert [f.name for f in spec.fields] == ["init_time", "lead_hours"]
