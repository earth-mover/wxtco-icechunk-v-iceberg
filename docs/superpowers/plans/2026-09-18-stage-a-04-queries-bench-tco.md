# Stage A, Plan 04: Queries, Benchmarks, Cost Model, EC2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Q1 (point timeseries + heating degree days), Q2 (regional ensemble stats + CRPS), Q3 (ML dataloader throughput) against a `TableBackend` (DuckDB over Iceberg) and a `TensorBackend` (xarray over Icechunk), prove they agree on the fixture, and add the benchmark runner, cost model, and EC2 job scripts.

**Architecture:** Backends return plain xarray objects with dims `(member, lead)` or `(member, lead, lat, lon)`; query functions compute derived quantities from those, so derived math is shared and only data access differs. `bench.py` times each query N times and appends CSV rows to `docs/findings/bench/`. `tco.py` combines bench CSVs, ETL CSVs, storage CSVs and `prices.toml` into a monthly TCO table.

**Tech Stack:** duckdb 1.5 (iceberg extension), xarray, zarr 3, icechunk, numpy, pyarrow, tomllib.

**Spec:** `docs/superpowers/specs/2026-09-18-tco-study-design.md` (sections 6, 7, 8).

## Global Constraints

- Same as Plans 01 to 03. Depends on: `local_catalog`, `ensure_table`, `ingest_cycle_table`, `TABLE_ID`, `column_name` (Plan 02); `open_virtual_repo`, `ingest_cycle_virtual`, `open_virtual_dataset_group` (Plan 03); `fixture_root`, `FIXTURE_SLUGS`, `build_cycle` (Plan 01).
- Tensor dims from the virtual repo: `forecast_reference_time, forecast_period, realization, latitude, longitude`. Backends rename to `init, lead, member, lat, lon` before returning.
- Workload for the cost model (D010): Q1 10,000/day, Q2 4/day, Q3 1/day, ETL 4 cycles/day.
- Bench CSV columns: `timestamp, git_sha, method, query, params, run, seconds, bytes_read, instance_type, region`.
- Session 003 notes: DuckDB extensions load from PyPI wheels (`wxtco.duck`); the Arraylake secret is parameter-bound; `tco` prices each run on its own instance before the median; `launch.sh` passes user data as a file (the CLI base64-encodes it itself); the EC2 job prologue exports `HOME` and runs under `set -e` until the job starts.

---

## File map

| file | responsibility |
|---|---|
| `src/wxtco/queries/__init__.py` | empty |
| `src/wxtco/queries/base.py` | `Backend` protocol, `Box`, `Point`, timing helper |
| `src/wxtco/queries/table_backend.py` | DuckDB over Iceberg |
| `src/wxtco/queries/tensor_backend.py` | xarray over Icechunk repo group |
| `src/wxtco/queries/q1_point.py` | timeseries + heating degree days |
| `src/wxtco/queries/q2_regional.py` | ensemble mean, spread, CRPS |
| `src/wxtco/queries/q3_dataloader.py` | batch iterator and throughput |
| `src/wxtco/bench.py` | run queries, write CSV |
| `src/wxtco/tco.py` | cost model |
| `prices.toml` | unit prices |
| `scripts/ec2/launch.sh`, `scripts/ec2/run_job.sh`, `scripts/ec2/terminate.sh`, `scripts/ec2/user_data.sh` | job machine lifecycle; code delivered as an S3 tarball |
| `src/wxtco/jobs.py` | `wxtco jobs status` / `jobs log` from `_progress/` and `_logs/` |
| tests: `tests/test_backends.py`, `tests/test_q1.py`, `tests/test_q2.py`, `tests/test_q3.py`, `tests/test_bench.py`, `tests/test_tco.py` | |

---

### Task 1: Backend protocol and TableBackend — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/queries/__init__.py`, `src/wxtco/queries/base.py`, `src/wxtco/queries/table_backend.py`
- Test: `tests/test_backends.py`

**Interfaces:**
- `base.py`:
  - `@dataclass(frozen=True) Point(lat: float, lon: float)`; `@dataclass(frozen=True) Box(lat_min, lat_max, lon_min, lon_max)`
  - `class Backend(Protocol)`: `name: str`; `point_series(var: str, point: Point, cycle: str) -> xr.DataArray` dims `(member, lead)` with coords `lead` (hours, int) and `valid_time`; `box_fields(vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset` dims `(member, lead, lat, lon)`; `lead_fields(vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray` shape `(len(vars), member, ny, nx)` float32; `bytes_read() -> int | None`.
  - `timed(fn, *a, **k) -> tuple[result, seconds]`.
- `table_backend.py`: `TableBackend(con: duckdb.DuckDBPyConnection, table: str)`; `TableBackend.local(metadata_location: str)` classmethod (uses `iceberg_scan`); `TableBackend.arraylake(settings)` (creates ICEBERG secret, attaches warehouse, table `wh.mogreps.surface`). `bytes_read()` returns `None` (DuckDB does not expose it per query; noted).

- [x] **Step 1: Write the failing test**

```python
# tests/test_backends.py
import numpy as np
import pytest

from wxtco.catalog import ensure_table, local_catalog
from wxtco.config import store_for
from wxtco.fixture import FIXTURE_SLUGS
from wxtco.ingest.table import ingest_cycle_table
from wxtco.queries.base import Box, Point
from wxtco.queries.table_backend import TableBackend


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
    assert ds["temperature_at_screen_level"].sel(member=0, lead=1).isel(lat=0, lon=0).item() == np.float32(10 + 2 + 0.01)


def test_lead_fields(table_backend):
    b, cycle = table_backend
    arr = b.lead_fields(["temperature_at_screen_level", "wind_speed_at_10m"], cycle, 3)
    assert arr.shape == (2, 2, 8, 8) and arr.dtype == np.float32
    assert arr[0, 1, 3, 5] == np.float32(1000 + 30 + 3 + 0.05)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backends.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write base.py**

```python
# src/wxtco/queries/base.py
"""Backend protocol. Backends fetch; query modules compute."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence

import numpy as np
import xarray as xr


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float


@dataclass(frozen=True)
class Box:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


class Backend(Protocol):
    name: str

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray: ...

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset: ...

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray: ...

    def bytes_read(self) -> int | None: ...


def timed(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[Any, float]:
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - t0
```

- [x] **Step 4: Write table_backend.py**

```python
# src/wxtco/queries/table_backend.py
"""DuckDB over the wide Iceberg table."""

from __future__ import annotations

from typing import Sequence

import duckdb
import numpy as np
import pandas as pd
import xarray as xr

from wxtco.config import Settings
from wxtco.ingest.table_schema import column_name
from wxtco.queries.base import Box, Point
from wxtco.source import cycle_time


class TableBackend:
    name = "table"

    def __init__(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        self.con = con
        self.table = table

    @classmethod
    def local(cls, metadata_location: str) -> TableBackend:
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg;")
        return cls(con, f"iceberg_scan('{metadata_location}')")

    @classmethod
    def arraylake(cls, settings: Settings) -> TableBackend:
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE SECRET al (TYPE ICEBERG, TOKEN '{settings.arraylake_token}', ENDPOINT '{settings.iceberg_uri}')")
        con.execute(f"ATTACH '{settings.arraylake_org}' AS wh (TYPE iceberg, SECRET al)")
        return cls(con, "wh.mogreps.surface")

    @staticmethod
    def _init(cycle: str) -> str:
        return cycle_time(cycle).strftime("%Y-%m-%d %H:%M:%S")

    def _nearest_cell(self, point: Point, cycle: str) -> tuple[float, float]:
        """Snap to the grid cell nearest the point using one lead's coordinates."""
        row = self.con.execute(
            f"select lat, lon from {self.table} where init_time = TIMESTAMP '{self._init(cycle)}' and lead_hours = 0 and member = 0 "
            f"order by (lat - {point.lat})*(lat - {point.lat}) + (lon - {point.lon})*(lon - {point.lon}) limit 1"
        ).fetchone()
        return float(row[0]), float(row[1])

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        lat, lon = self._nearest_cell(point, cycle)
        col = column_name(var)
        df = self.con.execute(
            f"select member, lead_hours, valid_time, {col} as value from {self.table} "
            f"where init_time = TIMESTAMP '{self._init(cycle)}' and lat = {lat}::FLOAT and lon = {lon}::FLOAT order by member, lead_hours"
        ).df()
        wide = df.pivot(index="member", columns="lead_hours", values="value")
        valid = df.drop_duplicates("lead_hours").set_index("lead_hours")["valid_time"].loc[wide.columns]
        return xr.DataArray(
            wide.to_numpy(dtype="float32"), dims=("member", "lead"),
            coords={"member": wide.index.to_numpy(), "lead": wide.columns.to_numpy(), "valid_time": ("lead", valid.to_numpy())},
            name=var,
        )

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        cols = ", ".join(f"{column_name(v)} as {column_name(v)}" for v in vars)
        df = self.con.execute(
            f"select member, lead_hours, lat, lon, {cols} from {self.table} "
            f"where init_time = TIMESTAMP '{self._init(cycle)}' and lat between {box.lat_min} and {box.lat_max} "
            f"and lon between {box.lon_min} and {box.lon_max}"
        ).df()
        idx = df.set_index(["member", "lead_hours", "lat", "lon"]).sort_index()
        ds = xr.Dataset.from_dataframe(idx).rename({"lead_hours": "lead"})
        return ds.rename({column_name(v): v for v in vars if column_name(v) != v}).astype("float32")

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        cols = ", ".join(column_name(v) for v in vars)
        tbl = self.con.execute(
            f"select {cols} from {self.table} where init_time = TIMESTAMP '{self._init(cycle)}' and lead_hours = {lead_hours} "
            f"order by member, lat, lon"
        ).fetch_arrow_table()
        n_member = self.con.execute(f"select count(distinct member) from {self.table} where init_time = TIMESTAMP '{self._init(cycle)}' and lead_hours = {lead_hours}").fetchone()[0]
        ny = self.con.execute(f"select count(distinct lat) from {self.table} where init_time = TIMESTAMP '{self._init(cycle)}' and lead_hours = {lead_hours}").fetchone()[0]
        nx = tbl.num_rows // (n_member * ny)
        out = np.empty((len(vars), n_member, ny, nx), dtype="float32")
        for i, v in enumerate(vars):
            out[i] = tbl[column_name(v)].to_numpy(zero_copy_only=False).reshape(n_member, ny, nx)
        return out

    def bytes_read(self) -> int | None:
        return None
```

Add `pandas` to `pyproject.toml` (`uv add pandas`).

- [x] **Step 5: Run tests; fix until green**

Run: `uv run pytest tests/test_backends.py -v`
Expected: 3 PASS. If `xr.Dataset.from_dataframe` produces `lat` unsorted, sort with `.sortby(["lat", "lon"])`.

- [x] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/wxtco/queries tests/test_backends.py
git commit -m "feat: query backend protocol and DuckDB table backend"
```

---

### Task 2: TensorBackend — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/queries/tensor_backend.py`
- Test: `tests/test_backends.py`

**Interfaces:**
- `TensorBackend(ds: xr.Dataset, name: str = "virtual")` where `ds` is `open_virtual_dataset_group(repo, "surface")`; `TensorBackend.from_repo(repo, group="surface", name="virtual")`. Implements the protocol. `bytes_read()` returns `None` in stage A (a byte-counting store wrapper is a stage B nicety; S3 request metrics from CloudWatch are the primary source).

- [x] **Step 1: Write the failing test**

Append to `tests/test_backends.py`:

```python
from wxtco.fixture import build_cycle
from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_repo
from wxtco.queries.tensor_backend import TensorBackend
from wxtco.config import Settings


@pytest.fixture
def tensor_backend(tmp_path, monkeypatch):
    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'src'}")
    import importlib, wxtco.ingest.mogreps_virtual as mv
    importlib.reload(mv)
    repo = open_virtual_repo(Settings.from_env(), "local", local_path=tmp_path / "repo")
    ingest_cycle_virtual(repo, store_for(f"file://{tmp_path / 'p'}"), cycle, cycle, workers=2, allow_incomplete=True, log=lambda *_: None)
    return TensorBackend.from_repo(repo), cycle


def test_tensor_matches_table(table_backend, tensor_backend):
    tb, cycle = table_backend
    xb, _ = tensor_backend
    lat = np.linspace(-89.9, 89.9, 8)[3]
    lon = np.linspace(-179.9, 179.9, 8)[5]
    a = tb.point_series("temperature_at_screen_level", Point(lat, lon), cycle)
    b = xb.point_series("temperature_at_screen_level", Point(lat, lon), cycle)
    np.testing.assert_array_equal(a.values, b.values)
    assert list(b["lead"].values) == [0, 1, 2, 3]
    lats = np.linspace(-89.9, 89.9, 8)
    lons = np.linspace(-179.9, 179.9, 8)
    box = Box(lats[2] - 0.01, lats[4] + 0.01, lons[1] - 0.01, lons[3] + 0.01)
    da = tb.box_fields(["temperature_at_screen_level"], box, cycle)["temperature_at_screen_level"]
    db = xb.box_fields(["temperature_at_screen_level"], box, cycle)["temperature_at_screen_level"]
    np.testing.assert_array_equal(da.transpose("member", "lead", "lat", "lon").values, db.transpose("member", "lead", "lat", "lon").values)
    np.testing.assert_array_equal(tb.lead_fields(["wind_speed_at_10m"], cycle, 2), xb.lead_fields(["wind_speed_at_10m"], cycle, 2))
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backends.py::test_tensor_matches_table -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/queries/tensor_backend.py
"""xarray over an Icechunk group. Same code for virtual and (stage B) native repos."""

from __future__ import annotations

from typing import Sequence

import icechunk as ic
import numpy as np
import xarray as xr

from wxtco.ingest.virtual import open_virtual_dataset_group
from wxtco.queries.base import Box, Point
from wxtco.source import cycle_time

RENAME = {"forecast_reference_time": "init", "forecast_period": "lead", "realization": "member", "latitude": "lat", "longitude": "lon"}


class TensorBackend:
    def __init__(self, ds: xr.Dataset, name: str = "virtual") -> None:
        self.ds = ds.rename({k: v for k, v in RENAME.items() if k in ds.dims or k in ds.coords})
        self.name = name

    @classmethod
    def from_repo(cls, repo: ic.Repository, group: str = "surface", name: str = "virtual") -> TensorBackend:
        return cls(open_virtual_dataset_group(repo, group), name=name)

    def _cycle(self, cycle: str) -> xr.Dataset:
        init = np.datetime64(cycle_time(cycle).replace(tzinfo=None), "ns")
        out = self.ds.sel(init=init)
        # lead is stored in seconds; expose hours as int
        return out.assign_coords(lead=(out["lead"] / np.timedelta64(1, "h")).astype(int)) if np.issubdtype(out["lead"].dtype, np.timedelta64) else out.assign_coords(lead=(out["lead"] // 3600).astype(int))

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        da = self._cycle(cycle)[var].sel(lat=point.lat, lon=point.lon, method="nearest")
        out = da.transpose("member", "lead").load().astype("float32")
        if "time" in out.coords:
            out = out.rename({"time": "valid_time"})
        return out.drop_vars([c for c in out.coords if c not in ("member", "lead", "valid_time")])

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        ds = self._cycle(cycle)[list(vars)]
        lat_slice = slice(box.lat_min, box.lat_max) if ds["lat"][0] < ds["lat"][-1] else slice(box.lat_max, box.lat_min)
        return ds.sel(lat=lat_slice, lon=slice(box.lon_min, box.lon_max)).transpose("member", "lead", "lat", "lon").load().astype("float32")

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        ds = self._cycle(cycle).sel(lead=lead_hours)
        return np.stack([ds[v].transpose("member", "lat", "lon").values.astype("float32") for v in vars])

    def bytes_read(self) -> int | None:
        return None
```

- [x] **Step 4: Run tests; fix until green**

Run: `uv run pytest tests/test_backends.py -v`
Expected: 4 PASS. Watch for: `forecast_period` decoded as `timedelta64` by xarray (handled), latitude ascending in the fixture (handled by the slice direction check).

- [x] **Step 5: Commit**

```bash
git add src/wxtco/queries/tensor_backend.py tests/test_backends.py
git commit -m "feat: xarray tensor backend; table and tensor agree on fixture"
```

---

### Task 3: Q1 point timeseries and heating degree days — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/queries/q1_point.py`
- Test: `tests/test_q1.py`

**Interfaces:**
- `heating_degree_days(temp_k: xr.DataArray, base_c: float = 18.0) -> xr.DataArray`: input dims `(member, lead)` with `valid_time` coord; groups leads by calendar day of `valid_time`, daily mean temperature in Celsius, `max(base - mean, 0)`, returns dims `(member, day)`.
- `q1(backend, point: Point, cycle: str, var="temperature_at_screen_level") -> Q1Result(series: xr.DataArray, hdd: xr.DataArray)`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_q1.py
import numpy as np
import xarray as xr

from wxtco.queries.q1_point import heating_degree_days


def test_hdd_two_days():
    # 2 members, 4 leads: two on day 1 at 10C and 20C (mean 15C -> 3 HDD), two on day 2 at 25C (0 HDD)
    k = 273.15
    vals = np.array([[10 + k, 20 + k, 25 + k, 25 + k], [0 + k, 0 + k, 30 + k, 30 + k]], dtype="float32")
    valid = np.array(["2026-09-16T12", "2026-09-16T18", "2026-09-17T00", "2026-09-17T06"], dtype="datetime64[ns]")
    da = xr.DataArray(vals, dims=("member", "lead"), coords={"member": [0, 1], "lead": [0, 6, 12, 18], "valid_time": ("lead", valid)})
    hdd = heating_degree_days(da)
    assert hdd.dims == ("member", "day")
    np.testing.assert_allclose(hdd.values, [[3.0, 0.0], [18.0, 0.0]], atol=1e-3)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_q1.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/queries/q1_point.py
"""Q1: point forecast timeseries and heating degree days."""

from __future__ import annotations

from dataclasses import dataclass

import xarray as xr

from wxtco.queries.base import Backend, Point

K = 273.15


def heating_degree_days(temp_k: xr.DataArray, base_c: float = 18.0) -> xr.DataArray:
    """Daily mean by valid-time date, then max(base - mean, 0). Dims (member, day)."""
    day = temp_k["valid_time"].dt.floor("D").rename("day")
    daily_c = (temp_k - K).groupby(day).mean("lead")
    return (base_c - daily_c).clip(min=0).transpose("member", "day")


@dataclass(frozen=True)
class Q1Result:
    series: xr.DataArray
    hdd: xr.DataArray


def q1(backend: Backend, point: Point, cycle: str, var: str = "temperature_at_screen_level") -> Q1Result:
    series = backend.point_series(var, point, cycle)
    return Q1Result(series, heating_degree_days(series))
```

- [x] **Step 4: Run tests and commit**

Run: `uv run pytest tests/test_q1.py -v`

```bash
git add src/wxtco/queries/q1_point.py tests/test_q1.py
git commit -m "feat: Q1 point timeseries and heating degree days"
```

---

### Task 4: Q2 regional ensemble statistics and CRPS — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/queries/q2_regional.py`
- Test: `tests/test_q2.py`

**Interfaces:**
- `crps_ensemble(ens: xr.DataArray, obs: xr.DataArray, member_dim="member") -> xr.DataArray`: `mean_i |x_i - y| - 0.5 * mean_ij |x_i - x_j|`, reduces `member`.
- `next_cycle(cycle: str) -> str` (+6 h).
- `q2(backend, box: Box, cycle: str, vars=("temperature_at_screen_level", "wind_speed_at_10m")) -> xr.Dataset` with data vars `<var>_mean`, `<var>_spread` (std over member, then mean over box) and `<var>_crps` (CRPS per cell vs proxy obs, mean over box), dims `(lead,)`. Proxy obs: ensemble mean of `next_cycle(cycle)` at the same valid time, i.e. at `lead - 6`; leads with no matching proxy are dropped.

- [x] **Step 1: Write the failing test**

```python
# tests/test_q2.py
import numpy as np
import xarray as xr

from wxtco.queries.q2_regional import crps_ensemble, next_cycle


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
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_q2.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/queries/q2_regional.py
"""Q2: regional ensemble mean, spread, and CRPS against a proxy analysis."""

from __future__ import annotations

from datetime import timedelta
from typing import Sequence

import numpy as np
import xarray as xr

from wxtco.queries.base import Backend, Box
from wxtco.source import cycle_time

CYCLE_HOURS = 6


def crps_ensemble(ens: xr.DataArray, obs: xr.DataArray, member_dim: str = "member") -> xr.DataArray:
    """Energy form of CRPS for a finite ensemble."""
    term1 = np.abs(ens - obs).mean(member_dim)
    a = ens.rename({member_dim: "i"})
    b = ens.rename({member_dim: "j"})
    term2 = 0.5 * np.abs(a - b).mean(("i", "j"))
    return term1 - term2


def next_cycle(cycle: str) -> str:
    t = cycle_time(cycle) + timedelta(hours=CYCLE_HOURS)
    return t.strftime("%Y/%m/%d/T%H%MZ")


def q2(backend: Backend, box: Box, cycle: str, vars: Sequence[str] = ("temperature_at_screen_level", "wind_speed_at_10m")) -> xr.Dataset:
    fc = backend.box_fields(vars, box, cycle)
    proxy_all = backend.box_fields(vars, box, next_cycle(cycle)).mean("member")
    # Same valid time: lead L in this cycle is lead L-6 in the next one.
    proxy = proxy_all.assign_coords(lead=proxy_all["lead"] + CYCLE_HOURS)
    common = np.intersect1d(fc["lead"].values, proxy["lead"].values)
    fc, proxy = fc.sel(lead=common), proxy.sel(lead=common)
    out = {}
    for v in vars:
        out[f"{v}_mean"] = fc[v].mean(("member", "lat", "lon"))
        out[f"{v}_spread"] = fc[v].std("member").mean(("lat", "lon"))
        out[f"{v}_crps"] = crps_ensemble(fc[v], proxy[v]).mean(("lat", "lon"))
    return xr.Dataset(out)
```

- [x] **Step 4: Run tests and commit**

Run: `uv run pytest tests/test_q2.py -v`

```bash
git add src/wxtco/queries/q2_regional.py tests/test_q2.py
git commit -m "feat: Q2 regional ensemble statistics and CRPS"
```

---

### Task 5: Q3 dataloader throughput — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/queries/q3_dataloader.py`
- Test: `tests/test_q3.py`

**Interfaces:**
- `iter_batches(backend, vars, cycles: Sequence[str], leads: Sequence[int], seed=0) -> Iterator[tuple[str, int, np.ndarray]]` yields `(cycle, lead_hours, array)` in a seeded random order over the (cycle, lead) product.
- `q3(backend, vars, cycles, leads, seed=0) -> Q3Result(samples: int, bytes: int, seconds: float, samples_per_s: float, gbps: float)`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_q3.py
import numpy as np

from wxtco.queries.q3_dataloader import iter_batches, q3


class FakeBackend:
    name = "fake"

    def lead_fields(self, vars, cycle, lead_hours):
        return np.zeros((len(vars), 2, 4, 4), dtype="float32")

    def bytes_read(self):
        return None


def test_iter_batches_random_order_is_deterministic():
    b = FakeBackend()
    order1 = [(c, l) for c, l, _ in iter_batches(b, ["a"], ["c1", "c2"], [0, 1, 2], seed=1)]
    order2 = [(c, l) for c, l, _ in iter_batches(b, ["a"], ["c1", "c2"], [0, 1, 2], seed=1)]
    assert order1 == order2 and sorted(order1) == [("c1", 0), ("c1", 1), ("c1", 2), ("c2", 0), ("c2", 1), ("c2", 2)]


def test_q3_counts_bytes():
    r = q3(FakeBackend(), ["a", "b"], ["c1"], [0, 1])
    assert r.samples == 2
    assert r.bytes == 2 * (2 * 2 * 4 * 4 * 4)
    assert r.samples_per_s > 0
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_q3.py -v`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/queries/q3_dataloader.py
"""Q3: ML dataloader pattern. Random (cycle, lead) order, all members, several variables per sample."""

from __future__ import annotations

import time
from dataclasses import dataclass
from itertools import product
from typing import Iterator, Sequence

import numpy as np

from wxtco.queries.base import Backend


def iter_batches(backend: Backend, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int], seed: int = 0) -> Iterator[tuple[str, int, np.ndarray]]:
    pairs = list(product(cycles, leads))
    rng = np.random.default_rng(seed)
    for i in rng.permutation(len(pairs)):
        cycle, lead = pairs[i]
        yield cycle, lead, backend.lead_fields(vars, cycle, lead)


@dataclass(frozen=True)
class Q3Result:
    samples: int
    bytes: int
    seconds: float
    samples_per_s: float
    gbps: float


def q3(backend: Backend, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int], seed: int = 0) -> Q3Result:
    t0 = time.perf_counter()
    n = nbytes = 0
    for _, _, arr in iter_batches(backend, vars, cycles, leads, seed):
        n += 1
        nbytes += arr.nbytes
    dt = time.perf_counter() - t0
    return Q3Result(n, nbytes, dt, n / dt, nbytes * 8 / dt / 1e9)
```

- [x] **Step 4: Run tests and commit**

```bash
git add src/wxtco/queries/q3_dataloader.py tests/test_q3.py
git commit -m "feat: Q3 dataloader throughput"
```

---

### Task 6: Benchmark runner — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/bench.py`
- Modify: `src/wxtco/cli.py` (add `bench` command)
- Test: `tests/test_bench.py`

**Interfaces:**
- `BenchRow` dataclass with the CSV columns from Global Constraints; `run_query(backend, query: str, params: dict, runs: int, out_csv: Path, meta: dict) -> list[BenchRow]` where `query in {"q1","q2","q3"}`; `params` for q1: `lat, lon, cycle`; q2: `lat_min, lat_max, lon_min, lon_max, cycle`; q3: `vars (list), cycles (list), leads (list)`. Appends rows; writes header if the file is new. `meta` carries `instance_type`, `region`, `git_sha` (from `git rev-parse --short HEAD`, or env `WXTCO_GIT_SHA`).
- CLI: `wxtco bench --method table|virtual --query q1 --params '{"lat":51.5,"lon":-0.1,"cycle":"2026/09/16/T0000Z"}' --runs 5 --out docs/findings/bench/q1.csv [--local PATH]`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_bench.py
import csv

import numpy as np
import xarray as xr

from wxtco.bench import run_query


class FakeBackend:
    name = "fake"

    def point_series(self, var, point, cycle):
        valid = np.array(["2026-09-16T00", "2026-09-16T06"], dtype="datetime64[ns]")
        return xr.DataArray(np.ones((2, 2), dtype="float32") * 280, dims=("member", "lead"), coords={"member": [0, 1], "lead": [0, 6], "valid_time": ("lead", valid)})

    def bytes_read(self):
        return 123


def test_run_query_writes_csv(tmp_path):
    out = tmp_path / "q1.csv"
    rows = run_query(FakeBackend(), "q1", {"lat": 1.0, "lon": 2.0, "cycle": "2026/09/16/T0000Z"}, runs=3, out_csv=out, meta={"instance_type": "local", "region": "local", "git_sha": "abc"})
    assert len(rows) == 3
    with out.open() as f:
        recs = list(csv.DictReader(f))
    assert len(recs) == 3 and recs[0]["method"] == "fake" and recs[0]["query"] == "q1" and recs[0]["bytes_read"] == "123"
    run_query(FakeBackend(), "q1", {"lat": 1.0, "lon": 2.0, "cycle": "2026/09/16/T0000Z"}, runs=1, out_csv=out, meta={"instance_type": "local", "region": "local", "git_sha": "abc"})
    with out.open() as f:
        assert len(list(csv.DictReader(f))) == 4
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bench.py -v`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/bench.py
"""Time queries and append rows to a findings CSV."""

from __future__ import annotations

import csv
import json
import subprocess
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wxtco.queries.base import Backend, Box, Point, timed
from wxtco.queries.q1_point import q1
from wxtco.queries.q2_regional import q2
from wxtco.queries.q3_dataloader import q3


@dataclass(frozen=True)
class BenchRow:
    timestamp: str
    git_sha: str
    method: str
    query: str
    params: str
    run: int
    seconds: float
    bytes_read: int | None
    instance_type: str
    region: str


def git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def _call(backend: Backend, query: str, p: dict[str, Any]) -> Any:
    if query == "q1":
        return q1(backend, Point(p["lat"], p["lon"]), p["cycle"])
    if query == "q2":
        return q2(backend, Box(p["lat_min"], p["lat_max"], p["lon_min"], p["lon_max"]), p["cycle"])
    if query == "q3":
        return q3(backend, p["vars"], p["cycles"], p["leads"])
    raise ValueError(query)


def run_query(backend: Backend, query: str, params: dict[str, Any], runs: int, out_csv: Path, meta: dict[str, str]) -> list[BenchRow]:
    rows: list[BenchRow] = []
    for i in range(runs):
        _, seconds = timed(_call, backend, query, params)
        rows.append(
            BenchRow(
                datetime.now(UTC).isoformat(timespec="seconds"), meta["git_sha"], backend.name, query, json.dumps(params, sort_keys=True),
                i, round(seconds, 4), backend.bytes_read(), meta["instance_type"], meta["region"],
            )
        )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    new = not out_csv.exists()
    with out_csv.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[x.name for x in fields(BenchRow)])
        if new:
            w.writeheader()
        for r in rows:
            w.writerow(asdict(r))
    return rows
```

CLI, appended to `src/wxtco/cli.py`:

```python
@app.command("bench")
def bench(
    method: str = typer.Option(..., help="table or virtual"),
    query: str = typer.Option(..., help="q1, q2, or q3"),
    params: str = typer.Option(..., help="JSON parameters"),
    runs: int = 5,
    out: Path = typer.Option(..., help="CSV to append to"),
    local: Path | None = typer.Option(None, help="Local catalog or repo path"),
    instance_type: str = typer.Option("local", envvar="WXTCO_INSTANCE_TYPE"),
) -> None:
    """Time one query N times and append to a findings CSV."""
    import json
    import os

    from wxtco.bench import git_sha, run_query

    s = Settings.from_env()
    if method == "table":
        from wxtco.catalog import ensure_table, local_catalog
        from wxtco.queries.table_backend import TableBackend
        from wxtco.slugs import study_slugs

        backend = TableBackend.local(ensure_table(local_catalog(local), study_slugs()).metadata_location) if local else TableBackend.arraylake(s)
    elif method == "virtual":
        from wxtco.ingest.virtual import open_virtual_repo
        from wxtco.queries.tensor_backend import TensorBackend

        repo = open_virtual_repo(s, "local", local_path=local) if local else open_virtual_repo(s, "arraylake")
        backend = TensorBackend.from_repo(repo, name="virtual")
    else:
        raise typer.BadParameter(method)
    meta = {"instance_type": instance_type, "region": os.environ.get("AWS_DEFAULT_REGION", s.copy_region), "git_sha": os.environ.get("WXTCO_GIT_SHA", git_sha())}
    rows = run_query(backend, query, json.loads(params), runs, out, meta)
    typer.echo(f"{method} {query}: " + ", ".join(f"{r.seconds:.2f}s" for r in rows))
```

- [x] **Step 4: Run tests and commit**

Run: `uv run pytest -q`

```bash
git add src/wxtco/bench.py src/wxtco/cli.py tests/test_bench.py
git commit -m "feat: benchmark runner and CLI"
```

---

### Task 7: Cost model — DONE 2026-09-18 by session 003

**Files:**
- Create: `prices.toml`, `src/wxtco/tco.py`
- Modify: `src/wxtco/cli.py` (add `tco` command)
- Test: `tests/test_tco.py`

**Interfaces:**
- `prices.toml`:

```toml
# Unit prices, us-east-1, list price, 2026-09. Update and cite when changed.
[s3]
storage_gb_month = 0.023
put_per_1000 = 0.005
get_per_1000 = 0.0004

[ec2]
# $/hour on-demand
"c7i.16xlarge" = 2.856
"m7i.4xlarge" = 0.8064
local = 0.0

[workload]
# per day, decision 010
q1 = 10000
q2 = 4
q3 = 1
etl_cycles = 4
```

- Inputs read by `tco.py`:
  - `docs/findings/storage.csv`: `method, variant, gb` (variant is `data` or `with_netcdf`)
  - `docs/findings/etl/*.csv`: `method, cycle, instance_type, seconds, s3_put, s3_get` (request counts may be blank)
  - `docs/findings/bench/*.csv`: from Task 6
- `monthly_tco(prices: dict, storage: DataFrame, etl: DataFrame, bench: DataFrame, workload_multiplier: float = 1.0) -> DataFrame` with columns `method, storage_usd, etl_usd, query_usd, total_usd` using: storage = gb x price; ETL = median seconds per cycle x etl_cycles x 30 x hourly price / 3600 (+ request costs when present); query = for each q, median seconds x workload x 30 x hourly price of the bench instance / 3600.
- CLI: `wxtco tco [--multiplier 1.0] [--findings docs/findings]` prints a Markdown table.

- [x] **Step 1: Write the failing test**

```python
# tests/test_tco.py
import pandas as pd

from wxtco.tco import monthly_tco

PRICES = {
    "s3": {"storage_gb_month": 0.02, "put_per_1000": 0.005, "get_per_1000": 0.0004},
    "ec2": {"big": 3.6, "small": 0.36, "local": 0.0},
    "workload": {"q1": 100, "q2": 0, "q3": 0, "etl_cycles": 1},
}


def test_monthly_tco_arithmetic():
    storage = pd.DataFrame({"method": ["table"], "variant": ["data"], "gb": [1000.0]})
    etl = pd.DataFrame({"method": ["table"], "cycle": ["c"], "instance_type": ["big"], "seconds": [3600.0], "s3_put": [None], "s3_get": [None]})
    bench = pd.DataFrame({"method": ["table"] * 2, "query": ["q1"] * 2, "seconds": [1.0, 3.0], "instance_type": ["small"] * 2})
    out = monthly_tco(PRICES, storage, etl, bench).set_index("method")
    assert out.loc["table", "storage_usd"] == 20.0
    assert out.loc["table", "etl_usd"] == 3.6 * 30
    # median 2 s x 100/day x 30 days x 0.36/h / 3600
    assert abs(out.loc["table", "query_usd"] - 2 * 100 * 30 * 0.36 / 3600) < 1e-9
    assert out.loc["table", "total_usd"] == out.loc["table", ["storage_usd", "etl_usd", "query_usd"]].sum()
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tco.py -v`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/tco.py
"""Monthly TCO from findings CSVs and prices.toml."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pandas as pd

DAYS = 30


def load_prices(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def _read_all(folder: Path, pattern: str) -> pd.DataFrame:
    files = sorted(folder.glob(pattern))
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True) if files else pd.DataFrame()


def monthly_tco(prices: dict, storage: pd.DataFrame, etl: pd.DataFrame, bench: pd.DataFrame, workload_multiplier: float = 1.0) -> pd.DataFrame:
    rows = []
    for method in sorted(set(storage["method"]) | set(etl["method"]) | set(bench["method"])):
        gb = storage.loc[(storage["method"] == method) & (storage["variant"] == "data"), "gb"].sum()
        storage_usd = gb * prices["s3"]["storage_gb_month"]
        e = etl[etl["method"] == method]
        etl_usd = 0.0
        if len(e):
            per_cycle = (e["seconds"].median() / 3600) * prices["ec2"][e["instance_type"].iloc[0]]
            reqs = (e["s3_put"].fillna(0).median() * prices["s3"]["put_per_1000"] + e["s3_get"].fillna(0).median() * prices["s3"]["get_per_1000"]) / 1000
            etl_usd = (per_cycle + reqs) * prices["workload"]["etl_cycles"] * DAYS
        query_usd = 0.0
        for q, per_day in prices["workload"].items():
            if q == "etl_cycles":
                continue
            b = bench[(bench["method"] == method) & (bench["query"] == q)]
            if len(b):
                query_usd += b["seconds"].median() * per_day * workload_multiplier * DAYS * prices["ec2"][b["instance_type"].iloc[0]] / 3600
        rows.append({"method": method, "storage_usd": storage_usd, "etl_usd": etl_usd, "query_usd": query_usd, "total_usd": storage_usd + etl_usd + query_usd})
    return pd.DataFrame(rows)


def tco_from_findings(findings: Path, prices_path: Path, multiplier: float = 1.0) -> pd.DataFrame:
    prices = load_prices(prices_path)
    storage = pd.read_csv(findings / "storage.csv") if (findings / "storage.csv").exists() else pd.DataFrame(columns=["method", "variant", "gb"])
    return monthly_tco(prices, storage, _read_all(findings / "etl", "*.csv"), _read_all(findings / "bench", "*.csv"), multiplier)
```

CLI, appended to `src/wxtco/cli.py`:

```python
@app.command("tco")
def tco(multiplier: float = 1.0, findings: Path = Path("docs/findings"), prices: Path = Path("prices.toml")) -> None:
    """Print the monthly TCO table from findings CSVs."""
    from wxtco.tco import tco_from_findings

    df = tco_from_findings(findings, prices, multiplier)
    typer.echo(df.round(2).to_markdown(index=False))
```

Add `tabulate` for `to_markdown`: `uv add tabulate`.

- [x] **Step 4: Run tests and commit**

```bash
git add prices.toml src/wxtco/tco.py src/wxtco/cli.py tests/test_tco.py pyproject.toml uv.lock
git commit -m "feat: monthly TCO model"
```

---

### Task 8: EC2 job scripts — DONE 2026-09-18 by session 003

**Files:**
- Create: `scripts/ec2/user_data.sh`, `scripts/ec2/launch.sh`, `scripts/ec2/run_job.sh`, `scripts/ec2/terminate.sh`, `scripts/ec2/README.md`

No unit tests. Verification is a real launch documented in the runbooks.

- [x] **Step 1: user_data.sh**

```bash
#!/usr/bin/env bash
# Cloud-init for job instances: uv, git, the repo, the Arraylake token from Secrets Manager.
set -euxo pipefail
dnf install -y awscli tar gzip
curl -LsSf https://astral.sh/uv/install.sh | HOME=/root sh
ln -sf /root/.local/bin/uv /usr/local/bin/uv
mkdir -p /opt/wxtco/repo && cd /opt/wxtco/repo
# Code arrives as a tarball the controller uploaded; EC2 never needs GitHub credentials.
aws s3 cp "s3://em-tco-mogreps/_code/${WXTCO_CODE_SHA}.tar.gz" /tmp/code.tar.gz --region us-east-1
tar -xzf /tmp/code.tar.gz -C /opt/wxtco/repo
uv sync --frozen
TOKEN=$(aws secretsmanager get-secret-value --secret-id wxtco/arraylake-token --query SecretString --output text --region us-east-1)
cat > .env <<EOT
ARRAYLAKE_TOKEN=$TOKEN
WXTCO_ORG=${WXTCO_ORG}
WXTCO_INSTANCE_TYPE=$(curl -s http://169.254.169.254/latest/meta-data/instance-type)
EOT
echo ready > /opt/wxtco/READY
```

- [x] **Step 2: launch.sh**

```bash
#!/usr/bin/env bash
# Upload the committed tree as a tarball, then launch one job instance. Prints the instance id.
# Usage: launch.sh <instance-type> <name>
set -euo pipefail
TYPE="${1:?instance type}"; NAME="${2:?name tag}"
SHA=$(git rev-parse --short HEAD)
if [ -n "$(git status --porcelain)" ]; then echo "commit first: working tree is dirty" >&2; exit 1; fi
git archive --format=tar.gz -o "/tmp/wxtco-$SHA.tar.gz" HEAD
aws s3 cp "/tmp/wxtco-$SHA.tar.gz" "s3://em-tco-mogreps/_code/$SHA.tar.gz" --only-show-errors
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
USER_DATA=$(sed -e "s/\${WXTCO_ORG}/${WXTCO_ORG:?set WXTCO_ORG}/" -e "s/\${WXTCO_CODE_SHA}/$SHA/" scripts/ec2/user_data.sh | base64)
aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --iam-instance-profile Name=wxtco-ec2 \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=200,VolumeType=gp3}' \
  --user-data "$USER_DATA" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=wxtco},{Key=Name,Value=$NAME}]" \
  --query 'Instances[0].InstanceId' --output text
```

- [x] **Step 3: run_job.sh**

```bash
#!/usr/bin/env bash
# Run a command on the instance via SSM, detached, logging to S3. Usage: run_job.sh <instance-id> <name> -- <command...>
set -euo pipefail
ID="${1:?instance id}"; NAME="${2:?job name}"; shift 2; [ "$1" = "--" ] && shift
CMD="cd /opt/wxtco/repo && set -a && . ./.env && set +a && export AWS_DEFAULT_REGION=us-east-1 && nohup $* > /opt/wxtco/$NAME.log 2>&1 && aws s3 cp /opt/wxtco/$NAME.log s3://em-tco-mogreps/_logs/$NAME.log"
aws ssm send-command --instance-ids "$ID" --document-name AWS-RunShellScript \
  --parameters "commands=[\"$CMD\"],executionTimeout=[\"172800\"]" \
  --query Command.CommandId --output text
echo "log: s3://em-tco-mogreps/_logs/$NAME.log (uploaded when the job ends)"
```

- [x] **Step 4: terminate.sh**

```bash
#!/usr/bin/env bash
# Terminate a job instance and print its lifetime in hours for the cost log.
set -euo pipefail
ID="${1:?instance id}"
LAUNCH=$(aws ec2 describe-instances --instance-ids "$ID" --query 'Reservations[0].Instances[0].LaunchTime' --output text)
aws ec2 terminate-instances --instance-ids "$ID" >/dev/null
python3 -c "from datetime import datetime,timezone; s='$LAUNCH'; t=datetime.fromisoformat(s.replace('Z','+00:00')); print(f'{(datetime.now(timezone.utc)-t).total_seconds()/3600:.2f} hours')"
```

- [x] **Step 5: `wxtco jobs status`**

Create `src/wxtco/jobs.py` and a `jobs` sub-app. It reads the two control prefixes the
controller key can access and prints one line per job.

```python
# src/wxtco/jobs.py
"""Job status from the control prefixes: _progress/<method>/<cycle>.json and _logs/<name>.log."""

from __future__ import annotations

import json
from dataclasses import dataclass

import obstore as obs
from obstore.store import ObjectStore


@dataclass(frozen=True)
class ProgressLine:
    method: str
    cycle_id: str
    units: int


def progress_lines(store: ObjectStore) -> list[ProgressLine]:
    out = []
    for page in obs.list(store, prefix="_progress/"):
        for item in page:
            _, method, name = item["path"].split("/", 2)
            units = len(json.loads(bytes(obs.get(store, item["path"]).bytes()))["done"])
            out.append(ProgressLine(method, name.removesuffix(".json"), units))
    return sorted(out, key=lambda p: (p.method, p.cycle_id))


def log_tail(store: ObjectStore, name: str, lines: int = 20) -> str:
    text = bytes(obs.get(store, f"_logs/{name}.log").bytes()).decode(errors="replace")
    return "\n".join(text.splitlines()[-lines:])
```

CLI, appended to `src/wxtco/cli.py`:

```python
jobs_app = typer.Typer(help="Status of detached EC2 jobs")
app.add_typer(jobs_app, name="jobs")


@jobs_app.command("status")
def jobs_status() -> None:
    """List progress manifests and finished job logs in the copy bucket."""
    from wxtco.jobs import progress_lines

    store = _store(Settings.from_env(), True)
    for p in progress_lines(store):
        typer.echo(f"{p.method:8s} {p.cycle_id}  {p.units} units done")
    logs = [item["path"] for page in obs.list(store, prefix="_logs/") for item in page]
    typer.echo(f"{len(logs)} finished job logs; `wxtco jobs log <name>` to tail one")


@jobs_app.command("log")
def jobs_log(name: str, lines: int = 20) -> None:
    from wxtco.jobs import log_tail

    typer.echo(log_tail(_store(Settings.from_env(), True), name, lines))
```

Add `import obstore as obs` at the top of `cli.py`. Test `progress_lines` with a `MemoryStore`
holding two manifests written via `Progress` (Plan 01 Task 5) and assert two sorted lines.

- [x] **Step 6: README and commit**

```markdown
# EC2 jobs

Run from a cloud session or a laptop with the `wxtco-controller` key. Commit before launching:
the instance runs the committed tree, uploaded as `_code/<sha>.tar.gz`.

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID> AWS_DEFAULT_REGION=us-east-1 ARRAYLAKE_ORG=wxtco
    ID=$(scripts/ec2/launch.sh c7i.16xlarge wxtco-ingest)
    # wait for /opt/wxtco/READY (about 3 min): aws ssm send-command ... 'cat /opt/wxtco/READY'
    scripts/ec2/run_job.sh "$ID" table-cycle1 -- uv run wxtco ingest table --cycle 2026/09/16/T0000Z --workers 48
    scripts/ec2/terminate.sh "$ID"   # prints hours; record in docs/findings/etl/<method>.csv

Ingest instance: c7i.16xlarge. Benchmark instance: m7i.4xlarge. Always terminate; a forgotten
c7i.16xlarge costs ~$70/day.
```

```bash
chmod +x scripts/ec2/*.sh
git add scripts/ec2 src/wxtco/jobs.py src/wxtco/cli.py tests/test_jobs.py
git commit -m "chore: EC2 launch, run, terminate scripts; jobs status"
```

---

### Task 9: Findings scaffolding and stage A summary template — DONE 2026-09-18 by session 003

**Files:**
- Create: `docs/findings/storage.csv` (header only), `docs/findings/etl/.gitkeep`, `docs/findings/bench/.gitkeep`, `docs/findings/stage-a-summary.md` (template)

```csv
method,variant,gb
```

```markdown
# Stage A summary (template; fill when benchmarks are done)

## Storage
<paste `wxtco tco` storage column and `aws s3 ls --summarize` outputs with commands>

## ETL
<per-method cycle wall clock, instance type, hours for 28 cycles>

## Query performance
<Q1..Q3 medians per method; note bytes read where known>

## Cost at 1x and 10x workload
<`wxtco tco` and `wxtco tco --multiplier 10`>

## What this implies for the native chunking (input to decision 011)
<which queries the virtual layout loses, by how much, and why>
```

Commit: `git commit -m "chore: findings scaffolding"`.

---

## Self-review notes

- Spec §6 Q1..Q4: Tasks 3, 4, 5; Q4 (ETL latency) is recorded by the runbooks into `docs/findings/etl/`, consumed by Task 7. Spec §7 cost model: Task 7 with `prices.toml`. Spec §8 layout: matches (`queries/`, `bench.py`, `tco.py`, `scripts/ec2/`). Spec §9 cross-backend equality: Task 2 test.
- Bytes read is `None` for both backends in stage A. The cost model therefore uses instance time plus CloudWatch S3 request metrics recorded manually into the ETL CSV. A byte-counting store wrapper for the tensor backend is a stage B addition.
- Q2 proxy analysis requires the next cycle to be ingested; for the last cycle in the window Q2 is undefined, which is why the benchmark runbook picks a cycle from the middle of the window.
- Type consistency: `Point`, `Box`, `Backend` from `base.py` are used unchanged in Tasks 1 to 6. `IngestReport` comes from Plan 02 and is reused by Plan 03.
