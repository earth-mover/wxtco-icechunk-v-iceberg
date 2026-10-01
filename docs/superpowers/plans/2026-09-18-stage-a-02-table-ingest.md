# Stage A, Plan 02: Table Ingest (NetCDF to Iceberg/Parquet) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest one MOGREPS-G cycle into a wide Iceberg table `mogreps.surface`, one Parquet data file per lead, resumable per lead, with the same code running against a local SQL catalog (tests) and the Arraylake REST catalog (production).

**Architecture:** For each lead, open the surface files for that lead from the copy store, stack the `(member, lat, lon)` grids into Arrow columns, and append one Arrow table to Iceberg. The table is partitioned by identity on `init_time` and `lead_hours`. A `Progress` manifest records finished leads. The catalog is injected, so tests use PyIceberg's `SqlCatalog` over sqlite and a local warehouse.

**Tech Stack:** pyiceberg 0.12 (`sql` extra + `sqlalchemy` for tests), pyarrow, xarray + h5netcdf, obstore, duckdb (verification).

**Spec:** `docs/superpowers/specs/2026-09-18-tco-study-design.md` (sections 4.1, 5).

## Global Constraints

- Same as Plan 01. Depends on Plan 01 interfaces: `Settings`, `store_for`, `list_cycle`, `surface_only`, `Diagnostic`, `SourceFile`, `cycle_time`, `Progress`, `fixture_root`.
- Table identifier: `mogreps.surface`. Namespace `mogreps`.
- Key columns: `init_time timestamp(us) not null`, `lead_hours int32 not null`, `valid_time timestamp(us) not null`, `member int32 not null`, `lat float not null`, `lon float not null`. One nullable `float` column per diagnostic slug, column name = slug with `-` replaced by `_`.
- Partition spec: identity(`init_time`), identity(`lead_hours`). Sort order: `member, lat, lon` (rows are produced already in that order).
- Parquet writer properties: `write.parquet.compression-codec=zstd`, `write.parquet.row-group-limit=380000` (about 128 MiB per row group at 356 B/row; pyiceberg 0.12 ignores `row-group-size-bytes`, session 003).
- Arraylake REST catalog: uri `https://api.earthmover.io/iceberg`, `warehouse=<org>`, `token=<ARRAYLAKE_TOKEN>`, header `X-Iceberg-Access-Delegation: vended-credentials`.

---

## File map

| file | responsibility |
|---|---|
| `src/wxtco/ingest/__init__.py` | empty |
| `src/wxtco/ingest/netcdf.py` | open one NetCDF from any store as an xarray Dataset; pick the data variable |
| `src/wxtco/ingest/table_schema.py` | slug list to Iceberg schema, Arrow schema, partition spec; column naming |
| `src/wxtco/ingest/table.py` | `lead_table()` builds Arrow for one lead; `ingest_cycle_table()` loops leads with progress |
| `src/wxtco/catalog.py` | `local_catalog(path)` and `arraylake_catalog(settings)` |
| `src/wxtco/cli.py` | add `ingest table` command |
| `tests/test_netcdf.py`, `tests/test_table_schema.py`, `tests/test_table_ingest.py` | unit tests on the fixture |

---

### Task 1: Open NetCDF from a store — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/ingest/__init__.py`, `src/wxtco/ingest/netcdf.py`
- Test: `tests/test_netcdf.py`

**Interfaces:**
- Produces: `open_netcdf(store, key: str) -> xr.Dataset` (reads full bytes, opens with h5netcdf, `decode_times=True`); `data_variable(ds) -> str` returning the single variable with `ndim == 3`; `grid_values(ds) -> np.ndarray` float32 of shape `(member, ny, nx)`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_netcdf.py
from wxtco.config import store_for
from wxtco.ingest.netcdf import data_variable, grid_values, open_netcdf
from wxtco.source import list_cycle


def test_open_netcdf_from_store(fixture_root):
    root, cycle = fixture_root
    store = store_for(f"file://{root}")
    d = [d for d in list_cycle(store, cycle) if d.slug == "temperature_at_screen_level"][0]
    ds = open_netcdf(store, d.files[2].key)  # lead 120 min
    assert data_variable(ds) == "air_temperature"
    v = grid_values(ds)
    assert v.shape == (2, 8, 8) and v.dtype == "float32"
    assert v[1, 3, 5] == 1000 + 20 + 3 + 0.05
    assert int(ds["forecast_period"].values) == 120 * 60
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_netcdf.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/ingest/netcdf.py
"""Read one Met Office NetCDF file from an object store into xarray."""

from __future__ import annotations

import io

import numpy as np
import obstore as obs
import xarray as xr
from obstore.store import ObjectStore


def open_netcdf(store: ObjectStore, key: str) -> xr.Dataset:
    """Fetch the whole object and open it in memory. Files are ~35 MB; one GET is cheapest."""
    payload = bytes(obs.get(store, key).bytes())
    return xr.open_dataset(io.BytesIO(payload), engine="h5netcdf")


def data_variable(ds: xr.Dataset) -> str:
    names = [n for n, v in ds.data_vars.items() if v.ndim == 3]
    if len(names) != 1:
        raise ValueError(f"expected one 3-D variable, found {names}")
    return names[0]


def grid_values(ds: xr.Dataset) -> np.ndarray:
    return np.ascontiguousarray(ds[data_variable(ds)].values, dtype="float32")
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_netcdf.py -v`
Expected: 1 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/ingest tests/test_netcdf.py
git commit -m "feat: open NetCDF from object store"
```

---

### Task 2: Table schema — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/ingest/table_schema.py`
- Test: `tests/test_table_schema.py`

**Interfaces:**
- Produces: `KEY_COLUMNS = ("init_time", "lead_hours", "valid_time", "member", "lat", "lon")`; `column_name(slug) -> str`; `iceberg_schema(slugs: Sequence[str]) -> pyiceberg.schema.Schema`; `partition_spec() -> PartitionSpec`; `arrow_schema(slugs) -> pa.Schema` (derived from the Iceberg schema so field ids match); `TABLE_ID = "mogreps.surface"`, `NAMESPACE = "mogreps"`; `TABLE_PROPERTIES: dict[str, str]`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_table_schema.py
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
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_table_schema.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/ingest/table_schema.py
"""Wide table schema: six key columns, one float column per diagnostic."""

from __future__ import annotations

from typing import Sequence

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FloatType, IntegerType, NestedField, TimestampType

NAMESPACE = "mogreps"
TABLE_ID = f"{NAMESPACE}.surface"
KEY_COLUMNS = ("init_time", "lead_hours", "valid_time", "member", "lat", "lon")
TABLE_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    "write.parquet.row-group-limit": "380000",
    "write.target-file-size-bytes": str(2 * 1024 * 1024 * 1024),
}


def column_name(slug: str) -> str:
    return slug.replace("-", "_")


def iceberg_schema(slugs: Sequence[str]) -> Schema:
    keys = [
        NestedField(1, "init_time", TimestampType(), required=True),
        NestedField(2, "lead_hours", IntegerType(), required=True),
        NestedField(3, "valid_time", TimestampType(), required=True),
        NestedField(4, "member", IntegerType(), required=True),
        NestedField(5, "lat", FloatType(), required=True),
        NestedField(6, "lon", FloatType(), required=True),
    ]
    values = [NestedField(7 + i, column_name(s), FloatType(), required=False) for i, s in enumerate(slugs)]
    return Schema(*keys, *values)


def partition_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="init_time"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="lead_hours"),
    )


def arrow_schema(slugs: Sequence[str]) -> pa.Schema:
    return iceberg_schema(slugs).as_arrow()
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_table_schema.py -v`
Expected: 3 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/ingest/table_schema.py tests/test_table_schema.py
git commit -m "feat: wide Iceberg table schema"
```

---

### Task 3: Catalog factories — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/catalog.py`
- Modify: `pyproject.toml` (add `pyiceberg[sql-sqlite]` or `sqlalchemy` to dev group)
- Test: `tests/test_catalog.py`

**Interfaces:**
- Produces: `local_catalog(path: Path) -> Catalog` (SqlCatalog, sqlite at `path/catalog.db`, warehouse `file://path/warehouse`); `arraylake_catalog(settings: Settings) -> Catalog` (RestCatalog per Global Constraints; raises if token or org missing); `ensure_table(catalog, slugs) -> Table` creating namespace and table if absent, with `partition_spec()` and `TABLE_PROPERTIES`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_catalog.py
import pytest

from wxtco.catalog import arraylake_catalog, ensure_table, local_catalog
from wxtco.config import Settings
from wxtco.ingest.table_schema import TABLE_ID


def test_local_catalog_and_ensure_table(tmp_path):
    cat = local_catalog(tmp_path)
    t = ensure_table(cat, ["a_var", "b_var"])
    assert t.name() == tuple(TABLE_ID.split("."))
    assert [f.name for f in t.spec().fields] == ["init_time", "lead_hours"]
    assert t.properties["write.parquet.compression-codec"] == "zstd"
    again = ensure_table(cat, ["a_var", "b_var"])
    assert again.metadata_location == t.metadata_location


def test_arraylake_catalog_requires_token():
    s = Settings("s", "r", "c", "r", "org", None, "https://api.earthmover.io/iceberg", None)
    with pytest.raises(ValueError):
        arraylake_catalog(s)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_catalog.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Add dependency and write implementation**

Run: `uv add --group dev sqlalchemy`

```python
# src/wxtco/catalog.py
"""Iceberg catalog factories: local sqlite for tests, Arraylake REST for production."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table import Table

from wxtco.config import Settings
from wxtco.ingest.table_schema import NAMESPACE, TABLE_ID, TABLE_PROPERTIES, iceberg_schema, partition_spec


def local_catalog(path: Path) -> Catalog:
    path.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("local", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=f"file://{path / 'warehouse'}")


def arraylake_catalog(settings: Settings) -> Catalog:
    if not settings.arraylake_token or not settings.arraylake_org:
        raise ValueError("ARRAYLAKE_TOKEN and ARRAYLAKE_ORG are required for the Arraylake catalog")
    return RestCatalog(
        name=settings.arraylake_org,
        uri=settings.iceberg_uri,
        warehouse=settings.arraylake_org,
        token=settings.arraylake_token,
        **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
    )


def ensure_table(catalog: Catalog, slugs: Sequence[str]) -> Table:
    """Create namespace and table if absent. The slug list fixes the schema for the study."""
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(TABLE_ID)
    except NoSuchTableError:
        return catalog.create_table(TABLE_ID, schema=iceberg_schema(slugs), partition_spec=partition_spec(), properties=TABLE_PROPERTIES)
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_catalog.py -v`
Expected: 2 PASS

- [x] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/wxtco/catalog.py tests/test_catalog.py
git commit -m "feat: Iceberg catalog factories"
```

---

### Task 4: Build the Arrow table for one lead — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/ingest/table.py`
- Test: `tests/test_table_ingest.py`

**Interfaces:**
- Produces: `files_by_lead(diagnostics: Sequence[Diagnostic]) -> dict[int, dict[str, SourceFile]]` mapping `lead_minutes -> {slug: file}`; `lead_table(store, cycle: str, lead_minutes: int, files: dict[str, SourceFile], slugs: Sequence[str], workers: int = 8) -> pa.Table` with rows ordered `member, lat, lon` and NULL columns for slugs without a file at this lead. Files are read in a thread pool (`workers`).

- [x] **Step 1: Write the failing test**

```python
# tests/test_table_ingest.py
import numpy as np
import pyarrow.compute as pc

from wxtco.config import store_for
from wxtco.fixture import FIXTURE_SLUGS
from wxtco.ingest.table import files_by_lead, lead_table
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
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_table_ingest.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/ingest/table.py
"""NetCDF to wide Arrow rows, one lead at a time, then Iceberg append."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Sequence

import numpy as np
import pyarrow as pa
from obstore.store import ObjectStore

from wxtco.ingest.netcdf import grid_values, open_netcdf
from wxtco.ingest.table_schema import arrow_schema, column_name
from wxtco.source import Diagnostic, SourceFile, cycle_time


def files_by_lead(diagnostics: Sequence[Diagnostic]) -> dict[int, dict[str, SourceFile]]:
    out: dict[int, dict[str, SourceFile]] = {}
    for d in diagnostics:
        for f in d.files:
            out.setdefault(f.lead_minutes, {})[d.slug] = f
    return dict(sorted(out.items()))


def _grid(store: ObjectStore, f: SourceFile) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    ds = open_netcdf(store, f.key)
    return grid_values(ds), ds["latitude"].values.astype("float32"), ds["longitude"].values.astype("float32")


def lead_table(
    store: ObjectStore, cycle: str, lead_minutes: int, files: dict[str, SourceFile], slugs: Sequence[str], workers: int = 8
) -> pa.Table:
    """One Arrow table for one lead. Rows ordered member, lat, lon. Missing diagnostics are NULL."""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        grids = dict(zip(files, pool.map(lambda f: _grid(store, f), files.values())))
    first, lat, lon = next(iter(grids.values()))
    members, ny, nx = first.shape
    n = members * ny * nx
    init = np.datetime64(cycle_time(cycle).replace(tzinfo=None), "us")
    valid = np.datetime64((cycle_time(cycle) + timedelta(minutes=lead_minutes)).replace(tzinfo=None), "us")
    # Broadcast the three coordinates to the grid shape in C order. ravel() then matches grid.reshape(-1).
    member, lat2d, lon2d = np.meshgrid(np.arange(members, dtype="int32"), lat, lon, indexing="ij")
    schema = arrow_schema(slugs)
    columns: dict[str, pa.Array] = {
        "init_time": pa.array(np.full(n, init), pa.timestamp("us")),
        "lead_hours": pa.array(np.full(n, lead_minutes // 60, dtype="int32")),
        "valid_time": pa.array(np.full(n, valid), pa.timestamp("us")),
        "member": pa.array(member.ravel()),
        "lat": pa.array(lat2d.ravel()),
        "lon": pa.array(lon2d.ravel()),
    }
    for slug in slugs:
        name = column_name(slug)
        if slug in grids:
            # NaN in the grid stays NaN. NULL means the diagnostic has no file at this lead.
            columns[name] = pa.array(grids[slug][0].reshape(-1))
        else:
            columns[name] = pa.nulls(n, pa.float32())
    return pa.table(columns, schema=schema)
```

**Design note (2026-09-18, session 003).** Ryan asked whether `xr.Dataset.to_dataframe()` (then
`reset_index()` to drop the MultiIndex) should replace the manual coordinate broadcast. Measured at
the real grid (18 x 960 x 1280 = 22.1 M rows, `scripts/bench_flatten.py <numpy|meshgrid|to_dataframe> <nvars>`):

| path | 8 vars | 24 vars |
|---|---|---|
| numpy broadcast + `pa.array` | 0.1 s, 1.1 GB peak RSS | 0.1 s, 2.4 GB |
| `to_dataframe().reset_index()` + `pa.Table.from_pandas` | 2.1 s, 3.3 GB | 4.4 s, 8.9 GB |

The pandas path copies the whole lead table into a DataFrame block, then Arrow copies it again, so
peak memory is about 3.7x the data. At 81 variables that is ~27 GB per lead versus ~7 GB, and it is
40x slower. It also changes semantics: `from_pandas` converts every NaN data value to NULL (the
fixture run showed 10 NULLs from 10 NaN cells), which conflates "value missing in the source" with
"diagnostic absent at this lead"; it emits the scalar coords (`forecast_period`, `height`, `time`)
as extra columns; and merging 81 files into one Dataset first fails on the per-variable `height`
coordinate. Decision: keep the direct Arrow path, but use `np.meshgrid(..., indexing="ij")` so the
coordinate broadcast is one self-evident line instead of nested `tile`/`repeat`.

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_table_ingest.py -v`
Expected: 2 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/ingest/table.py tests/test_table_ingest.py
git commit -m "feat: Arrow table for one lead"
```

---

### Task 5: Ingest a cycle with progress and verify with DuckDB — DONE 2026-09-18 by session 003

**Files:**
- Modify: `src/wxtco/ingest/table.py`
- Test: `tests/test_table_ingest.py`

**Interfaces:**
- Produces: `ingest_cycle_table(catalog, store, progress_store, cycle, slugs, workers=8, leads: Sequence[int] | None = None, log=print) -> IngestReport(cycle, units_done: int, units_skipped: int, rows: int, seconds: float)`. Unit id is `f"lead={lead_minutes}"`. One `table.append` per lead. Skips leads already in `Progress(progress_store, "table", cycle)`.

- [x] **Step 1: Write the failing test**

Append to `tests/test_table_ingest.py`:

```python
import duckdb

from wxtco.catalog import ensure_table, local_catalog
from wxtco.ingest.table import ingest_cycle_table
from wxtco.progress import Progress


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
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    n, leads = con.execute(
        f"select count(*), count(distinct lead_hours) from iceberg_scan('{t.metadata_location}')"
    ).fetchone()
    assert n == r.rows and leads == 4
    # one data file per lead
    assert len(list(t.scan().plan_files())) == 4
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_table_ingest.py::test_ingest_cycle_table_resumable -v`
Expected: FAIL with `ImportError: cannot import name 'ingest_cycle_table'`

- [x] **Step 3: Write implementation**

Append to `src/wxtco/ingest/table.py`:

```python
import time
from dataclasses import dataclass
from typing import Callable

from pyiceberg.catalog import Catalog

from wxtco.catalog import ensure_table
from wxtco.progress import Progress
from wxtco.source import list_cycle, surface_only


@dataclass(frozen=True)
class IngestReport:
    cycle: str
    units_done: int
    units_skipped: int
    rows: int
    seconds: float


def ingest_cycle_table(
    catalog: Catalog,
    store: ObjectStore,
    progress_store: ObjectStore,
    cycle: str,
    slugs: Sequence[str],
    workers: int = 8,
    leads: Sequence[int] | None = None,
    log: Callable[[str], None] = print,
) -> IngestReport:
    """Append one Parquet data file per lead. Reruns skip leads already marked done."""
    started = time.perf_counter()
    table = ensure_table(catalog, slugs)
    progress = Progress(progress_store, "table", cycle)
    by_lead = files_by_lead([d for d in surface_only(list_cycle(store, cycle)) if d.slug in set(slugs)])
    done = skipped = rows = 0
    for lead_minutes, files in by_lead.items():
        if leads is not None and lead_minutes not in leads:
            continue
        unit = f"lead={lead_minutes}"
        if progress.done(unit):
            skipped += 1
            continue
        t0 = time.perf_counter()
        arrow = lead_table(store, cycle, lead_minutes, files, slugs, workers=workers)
        table.append(arrow, snapshot_properties={"wxtco.cycle": cycle, "wxtco.unit": unit})
        progress.mark(unit)
        done += 1
        rows += arrow.num_rows
        log(f"{cycle} {unit}: {arrow.num_rows} rows in {time.perf_counter() - t0:.1f}s")
    return IngestReport(cycle, done, skipped, rows, time.perf_counter() - started)
```

Move the new `import` lines to the top of the file with the others.

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_table_ingest.py -v`
Expected: 3 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/ingest/table.py tests/test_table_ingest.py
git commit -m "feat: resumable table ingest per cycle"
```

---

### Task 6: `wxtco ingest table` CLI — DONE 2026-09-18 by session 003

**Files:**
- Modify: `src/wxtco/cli.py`
- Create: `src/wxtco/slugs.py`
- Test: `tests/test_cli_ingest.py`

**Interfaces:**
- Produces: `wxtco ingest table --cycle C [--local PATH] [--workers N] [--slugs a,b] [--leads 0,60]`. Without `--local`, uses `arraylake_catalog(Settings.from_env())` and the copy store; with `--local PATH`, uses `local_catalog(PATH)` and the source store from `WXTCO_SOURCE_URL` (so a fixture directory works). Progress store is the copy store in production, the local path in local mode.
- `wxtco.slugs.SURFACE_SLUGS: tuple[str, ...]`: the 81 surface diagnostic slugs, taken from `docs/findings/mogreps-g-cycle-inventory.md` (all rows whose slug does not end in `_levels`). `study_slugs(settings) -> list[str]` returns `SURFACE_SLUGS` unless `WXTCO_SLUGS` is set.

- [x] **Step 1: Generate the slug list**

Run:

```bash
awk -F'|' 'NR>5 && $4 !~ /_levels/ {gsub(/ /,"",$4); if ($4!="") print "    \"" $4 "\","}' docs/findings/mogreps-g-cycle-inventory.md | sort
```

Paste the 81 lines into `src/wxtco/slugs.py`:

```python
# src/wxtco/slugs.py
"""The 81 MOGREPS-G surface diagnostics in the study. Source: cycle inventory 2026-09-16 T0000Z."""

import os

SURFACE_SLUGS: tuple[str, ...] = (
    # ... 81 slugs, alphabetical ...
)


def study_slugs() -> list[str]:
    """Override with WXTCO_SLUGS=a,b,c for small runs."""
    env = os.environ.get("WXTCO_SLUGS")
    return env.split(",") if env else list(SURFACE_SLUGS)
```

Add a test that asserts `len(SURFACE_SLUGS) == 81` and none ends with `_levels`.

- [x] **Step 2: Write the failing CLI test**

```python
# tests/test_cli_ingest.py
from typer.testing import CliRunner

from wxtco.cli import app
from wxtco.fixture import FIXTURE_SLUGS

runner = CliRunner()


def test_ingest_table_local(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{root}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    r = runner.invoke(app, ["ingest", "table", "--cycle", cycle, "--local", str(tmp_path / "cat")])
    assert r.exit_code == 0, r.output
    assert "4 leads done" in r.output
```

- [x] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_cli_ingest.py -v`
Expected: FAIL (`No such command 'ingest'`)

- [x] **Step 4: Implement the command**

Append to `src/wxtco/cli.py`:

```python
ingest_app = typer.Typer(help="Ingest cycles into one of the storage methods")
app.add_typer(ingest_app, name="ingest")


@ingest_app.command("table")
def ingest_table(
    cycle: str = typer.Option(...),
    local: Path | None = typer.Option(None, help="Use a local sqlite catalog at this path"),
    workers: int = 8,
    leads: str | None = typer.Option(None, help="Comma-separated lead minutes to restrict to"),
) -> None:
    """Ingest one cycle into the wide Iceberg table."""
    from wxtco.catalog import arraylake_catalog, local_catalog
    from wxtco.ingest.table import ingest_cycle_table
    from wxtco.slugs import study_slugs

    s = Settings.from_env()
    if local is not None:
        catalog, store, progress_store = local_catalog(local), _store(s, False), store_for(f"file://{local}")
    else:
        catalog, store = arraylake_catalog(s), _store(s, True)
        progress_store = store
    lead_list = [int(x) for x in leads.split(",")] if leads else None
    r = ingest_cycle_table(catalog, store, progress_store, cycle, study_slugs(), workers=workers, leads=lead_list, log=typer.echo)
    typer.echo(f"{r.cycle}: {r.units_done} leads done, {r.units_skipped} skipped, {r.rows} rows, {r.seconds:.0f}s")
```

- [x] **Step 5: Run all tests and commit**

Run: `uv run pytest -q`
Expected: all PASS

```bash
git add src/wxtco/cli.py src/wxtco/slugs.py tests/test_cli_ingest.py tests/test_slugs.py
git commit -m "feat: ingest table CLI and surface slug list"
```

---

### Task 7: Production smoke test recipe (manual, documented) — DONE 2026-09-18 by session 003

**Files:**
- Create: `docs/infra/runbook-table-ingest.md`

No code. Document the exact commands a session runs once the org, token, and copy exist:

```markdown
# Runbook: table ingest, one cycle

Prereqs: `.env` has `ARRAYLAKE_TOKEN`, `ARRAYLAKE_ORG`; `study_cycles.txt` exists; copy verified.

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>
    CYCLE=$(head -1 study_cycles.txt)
    uv run wxtco copy verify --cycle "$CYCLE"
    # small first: 2 leads, 3 diagnostics
    WXTCO_SLUGS=temperature_at_screen_level,wind_speed_at_10m,pressure_at_mean_sea_level \
      uv run wxtco ingest table --cycle "$CYCLE" --leads 0,60 --workers 16
    # verify with DuckDB
    uv run python -c "
    import duckdb, os
    c = duckdb.connect(); c.execute('INSTALL iceberg; LOAD iceberg;')
    c.execute(f\"CREATE SECRET al (TYPE ICEBERG, TOKEN '{os.environ['ARRAYLAKE_TOKEN']}', ENDPOINT 'https://api.earthmover.io/iceberg')\")
    c.execute(f\"ATTACH '{os.environ['WXTCO_ORG']}' AS wh (TYPE iceberg, SECRET al)\")
    print(c.execute('select lead_hours, count(*) from wh.mogreps.surface group by 1 order by 1').fetchall())"

If the small run passes, drop the table (`al iceberg table delete` or via PyIceberg
`catalog.drop_table('mogreps.surface')`) because its schema has only 3 columns, then run the
full 81-slug cycle on EC2 (see Plan 04, EC2 scripts). Record wall clock and instance type in
`docs/findings/etl/table.csv`.
```

- [x] **Step 1: Commit**

```bash
git add docs/infra/runbook-table-ingest.md
git commit -m "chore: table ingest runbook"
```

---

## Self-review notes

- Spec §4.1: schema, partitioning, Parquet settings (Tasks 2, 3). §5 table row: one data file per lead, one commit per lead here rather than per cycle; per-lead commits make resume simple and the spec's per-cycle commit added no benefit for a study. Documented deviation.
- Memory: a real lead is 22.1M rows x (6 keys + 81 floats) ≈ 8 GB Arrow before write. Acceptable on the 128 GB ingest instance. If it is not, Task 4 can yield one `RecordBatch` per member and `table.append` accepts a `RecordBatchReader`; the interface stays the same.
- Interfaces used by Plan 04: `ensure_table`, `TABLE_ID`, `column_name`, `arraylake_catalog`, `local_catalog`.
