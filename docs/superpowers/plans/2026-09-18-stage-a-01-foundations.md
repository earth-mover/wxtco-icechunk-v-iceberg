# Stage A, Plan 01: Foundations (config, source listing, fixture, copy, progress) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared layer every ingest and query path depends on: settings, source key parsing and cycle listing over any object store, a synthetic MOGREPS-like fixture, static-copy tooling, and a per-unit progress manifest.

**Architecture:** All object access goes through `obstore` stores so the same code runs against a local directory (tests) and S3 (production). Keys follow the Met Office layout `<root>/<YYYY>/<MM>/<DD>/<THHMMZ>/<valid>-PT<hhhh>H<mm>M-<slug>.nc`. A tiny synthetic NetCDF fixture (2 members, 8x8 grid, 3 diagnostics, 4 leads) stands in for real data in every unit test.

**Tech Stack:** Python 3.12+, uv, obstore, xarray, h5netcdf, numpy, pytest, typer.

**Spec:** `docs/superpowers/specs/2026-09-18-tco-study-design.md` (sections 3, 5, 8, 9).

## Global Constraints

- Python `>=3.12`; dependencies only via `pyproject.toml` and `uv`.
- Comments and docstrings: ASD-STE100 Simplified Technical English, 1-2 lines, expert reader.
- No cloud writes from tests. Tests use `tmp_path` and `obstore.store.LocalStore`.
- Package name `wxtco`; CLI `wxtco`. Source under `src/wxtco/`, tests under `tests/`.
- Static copy bucket: `em-tco-mogreps`, region `us-east-1`, AWS account `<ACCOUNT_ID>`, profile `PowerUserAccess-<ACCOUNT_ID>`.
- Source bucket: `met-office-global-ensemble-model-data`, prefix `global-ensemble`, region `eu-west-2`, anonymous.
- Every cloud resource tagged `project=wxtco`.
- Commit after every task with message prefix `feat:`, `test:`, or `chore:`.

---

## File map

| file | responsibility |
|---|---|
| `src/wxtco/config.py` | `Settings` dataclass loaded from env and `.env`; store factories |
| `src/wxtco/source.py` | key parsing, `SourceFile`, `Diagnostic`, `list_cycle`, `available_cycles` |
| `src/wxtco/fixture.py` | build the synthetic NetCDF cycle used by tests and dry runs |
| `src/wxtco/copy.py` | verify a static copy; produce the `aws s3 cp` command list |
| `src/wxtco/progress.py` | `Progress` manifest: which units of a (method, cycle) are done |
| `src/wxtco/cli.py` | typer app; subcommands `fixture`, `source`, `copy` |
| `scripts/copy_cycle.sh` | production copy of one cycle with the AWS CLI |
| `tests/conftest.py` | `fixture_root` fixture that builds one synthetic cycle |
| `tests/test_source.py`, `tests/test_fixture.py`, `tests/test_copy.py`, `tests/test_progress.py`, `tests/test_config.py` | unit tests |

---

### Task 1: Settings — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/config.py`
- Test: `tests/test_config.py`
- Modify: `pyproject.toml` (add `python-dotenv`)

**Interfaces:**
- Produces: `Settings` with fields `source_url: str`, `source_region: str`, `copy_url: str`, `copy_region: str`, `arraylake_org: str` (from `WXTCO_ORG` or `ARRAYLAKE_ORG`), `arraylake_token: str | None`, `iceberg_uri: str`, `aws_profile: str | None`; `Settings.from_env() -> Settings`; `store_for(url: str, region: str | None = None, anonymous: bool = False) -> obstore.store.ObjectStore`; `split_url(url) -> tuple[str, str]` returning `(bucket_or_path, prefix)`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_config.py
from pathlib import Path

from obstore.store import LocalStore, S3Store

from wxtco.config import Settings, split_url, store_for


def test_defaults(monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith("WXTCO_") or k == "ARRAYLAKE_TOKEN":
            monkeypatch.delenv(k)
    s = Settings.from_env()
    assert s.source_url == "s3://met-office-global-ensemble-model-data/global-ensemble"
    assert s.copy_url == "s3://em-tco-mogreps/netcdf"
    assert s.iceberg_uri == "https://api.earthmover.io/iceberg"


def test_env_override(monkeypatch):
    monkeypatch.setenv("WXTCO_COPY_URL", "file:///tmp/copy")
    monkeypatch.setenv("ARRAYLAKE_ORG", "wxtco-test")
    s = Settings.from_env()
    assert s.copy_url == "file:///tmp/copy"
    assert s.arraylake_org == "wxtco-test"


def test_split_url():
    assert split_url("s3://bucket/a/b") == ("bucket", "a/b")
    assert split_url("s3://bucket") == ("bucket", "")
    assert split_url("file:///tmp/x/y") == ("/tmp/x/y", "")


def test_store_for_local(tmp_path: Path):
    store = store_for(f"file://{tmp_path}")
    assert isinstance(store, LocalStore)


def test_store_for_s3_anonymous():
    store = store_for("s3://met-office-global-ensemble-model-data/global-ensemble", region="eu-west-2", anonymous=True)
    assert isinstance(store, S3Store)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'wxtco.config'`

- [x] **Step 3: Add dependency and write implementation**

Run: `uv add python-dotenv`

```python
# src/wxtco/config.py
"""Project settings. Values come from the environment, then `.env`, then defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from obstore.store import LocalStore, ObjectStore, S3Store

SOURCE_URL = "s3://met-office-global-ensemble-model-data/global-ensemble"
SOURCE_REGION = "eu-west-2"
COPY_URL = "s3://em-tco-mogreps/netcdf"
COPY_REGION = "us-east-1"
ICEBERG_URI = "https://api.earthmover.io/iceberg"


@dataclass(frozen=True)
class Settings:
    source_url: str
    source_region: str
    copy_url: str
    copy_region: str
    arraylake_org: str
    arraylake_token: str | None
    iceberg_uri: str
    aws_profile: str | None

    @classmethod
    def from_env(cls) -> Settings:
        """Read settings. `.env` in the CWD fills gaps but never overrides the environment."""
        load_dotenv(override=False)
        return cls(
            source_url=os.environ.get("WXTCO_SOURCE_URL", SOURCE_URL),
            source_region=os.environ.get("WXTCO_SOURCE_REGION", SOURCE_REGION),
            copy_url=os.environ.get("WXTCO_COPY_URL", COPY_URL),
            copy_region=os.environ.get("WXTCO_COPY_REGION", COPY_REGION),
            arraylake_org=os.environ.get("WXTCO_ORG") or os.environ.get("ARRAYLAKE_ORG", ""),
            arraylake_token=os.environ.get("ARRAYLAKE_TOKEN"),
            iceberg_uri=os.environ.get("WXTCO_ICEBERG_URI", ICEBERG_URI),
            aws_profile=os.environ.get("AWS_PROFILE"),
        )


def split_url(url: str) -> tuple[str, str]:
    """Split `s3://bucket/prefix` into (bucket, prefix) and `file:///path` into (path, "")."""
    scheme, _, rest = url.partition("://")
    if scheme == "file":
        return "/" + rest.lstrip("/"), ""
    if scheme != "s3":
        raise ValueError(f"unsupported url scheme: {url}")
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix.strip("/")


def store_for(url: str, region: str | None = None, anonymous: bool = False) -> ObjectStore:
    """Return an obstore store rooted at `url`. The prefix is part of the store root."""
    root, prefix = split_url(url)
    if url.startswith("file://"):
        Path(root).mkdir(parents=True, exist_ok=True)
        return LocalStore(root)
    return S3Store(root, prefix=prefix or None, region=region, skip_signature=anonymous)
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: 5 PASS

- [x] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock src/wxtco/config.py tests/test_config.py
git commit -m "feat: settings and object store factory"
```

---

### Task 2: Source key parsing and cycle listing — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/source.py`
- Test: `tests/test_source.py`

**Interfaces:**
- Produces:
  - `KEY_RE`, `parse_key(key: str) -> SourceFile | None`
  - `@dataclass(frozen=True) SourceFile(key: str, slug: str, lead_minutes: int, valid_time: datetime)`
  - `@dataclass(frozen=True) Diagnostic(slug: str, files: tuple[SourceFile, ...])` with properties `is_level: bool` (slug contains `_on_` and ends with `_levels`), `window: str` (`PT01H`, `PT03H`, or `""`)
  - `cycle_time(cycle: str) -> datetime` for `"2026/09/16/T0000Z"`
  - `cycle_id(cycle: str) -> str` returning `"20260916T0000Z"`
  - `list_cycle(store, cycle: str) -> list[Diagnostic]` sorted by slug, files sorted by lead
  - `available_cycles(store) -> list[str]` oldest first
  - `surface_only(diagnostics) -> list[Diagnostic]`

- [x] **Step 1: Write the failing test**

```python
# tests/test_source.py
from datetime import UTC, datetime

from obstore.store import MemoryStore
import obstore as obs

from wxtco.source import Diagnostic, available_cycles, cycle_id, cycle_time, list_cycle, parse_key, surface_only

KEY = "2026/09/16/T0000Z/20260916T0300Z-PT0003H00M-temperature_at_screen_level.nc"


def test_parse_key():
    f = parse_key(KEY)
    assert f is not None
    assert f.slug == "temperature_at_screen_level"
    assert f.lead_minutes == 180
    assert f.valid_time == datetime(2026, 9, 16, 3, tzinfo=UTC)


def test_parse_key_rejects_other_files():
    assert parse_key("2026/09/16/T0000Z/README.txt") is None


def test_cycle_time_and_id():
    assert cycle_time("2026/09/16/T0600Z") == datetime(2026, 9, 16, 6, tzinfo=UTC)
    assert cycle_id("2026/09/16/T0600Z") == "20260916T0600Z"


def test_diagnostic_flags():
    f = parse_key(KEY)
    d = Diagnostic("temperature_at_screen_level", (f,))
    assert not d.is_level and d.window == ""
    lv = Diagnostic("wind_speed_on_height_levels", ())
    assert lv.is_level
    st = Diagnostic("precipitation_accumulation-PT01H", ())
    assert st.window == "PT01H"


def _put(store, key):
    obs.put(store, key, b"x")


def test_list_cycle_and_available(tmp_path):
    store = MemoryStore()
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-b_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0100Z-PT0001H00M-b_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-a_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-x_on_height_levels.nc")
    _put(store, "2026/09/15/T1800Z/20260915T1800Z-PT0000H00M-a_var.nc")
    diags = list_cycle(store, "2026/09/16/T0000Z")
    assert [d.slug for d in diags] == ["a_var", "b_var", "x_on_height_levels"]
    assert [f.lead_minutes for f in diags[1].files] == [0, 60]
    assert [d.slug for d in surface_only(diags)] == ["a_var", "b_var"]
    assert available_cycles(store) == ["2026/09/15/T1800Z", "2026/09/16/T0000Z"]
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_source.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/source.py
"""Met Office key layout: <YYYY>/<MM>/<DD>/<THHMMZ>/<valid>-PT<hhhh>H<mm>M-<slug>.nc."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import groupby
from typing import Sequence

import obstore as obs
from obstore.store import ObjectStore

KEY_RE = re.compile(r"(?P<valid>\d{8}T\d{4}Z)-PT(?P<hours>\d{4})H(?P<minutes>\d{2})M-(?P<slug>.+)\.nc$")
WINDOW_RE = re.compile(r"-(PT\d{2}H)$")
CYCLE_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})/T(\d{2})(\d{2})Z$")


@dataclass(frozen=True)
class SourceFile:
    key: str
    slug: str
    lead_minutes: int
    valid_time: datetime


@dataclass(frozen=True)
class Diagnostic:
    slug: str
    files: tuple[SourceFile, ...]

    @property
    def is_level(self) -> bool:
        return "_on_" in self.slug and self.slug.endswith("_levels")

    @property
    def window(self) -> str:
        m = WINDOW_RE.search(self.slug)
        return m[1] if m else ""


def parse_key(key: str) -> SourceFile | None:
    """Parse one object key. Return None for keys that are not forecast files."""
    m = KEY_RE.search(key.rsplit("/", 1)[-1])
    if m is None:
        return None
    valid = datetime.strptime(m["valid"], "%Y%m%dT%H%MZ").replace(tzinfo=UTC)
    return SourceFile(key, m["slug"], int(m["hours"]) * 60 + int(m["minutes"]), valid)


def cycle_time(cycle: str) -> datetime:
    m = CYCLE_RE.match(cycle)
    if m is None:
        raise ValueError(f"bad cycle: {cycle}")
    y, mo, d, h, mi = (int(x) for x in m.groups())
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


def cycle_id(cycle: str) -> str:
    return cycle_time(cycle).strftime("%Y%m%dT%H%MZ")


def _child_prefixes(store: ObjectStore, prefix: str) -> list[str]:
    listing = obs.list_with_delimiter(store, prefix=prefix)
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in listing["common_prefixes"])


def available_cycles(store: ObjectStore) -> list[str]:
    """Every cycle under the store root, oldest first. Keys are listed, never constructed."""
    out: list[str] = []
    for y in _child_prefixes(store, ""):
        for mo in _child_prefixes(store, f"{y}/"):
            for d in _child_prefixes(store, f"{y}/{mo}/"):
                out.extend(f"{y}/{mo}/{d}/{t}" for t in _child_prefixes(store, f"{y}/{mo}/{d}/"))
    return sorted(out, key=cycle_time)


def list_cycle(store: ObjectStore, cycle: str) -> list[Diagnostic]:
    """All forecast files of one cycle grouped by diagnostic, sorted by slug then lead."""
    files = sorted(
        filter(None, (parse_key(item["path"]) for page in obs.list(store, prefix=f"{cycle}/") for item in page)),
        key=lambda f: (f.slug, f.lead_minutes),
    )
    return [Diagnostic(slug, tuple(g)) for slug, g in groupby(files, key=lambda f: f.slug)]


def surface_only(diagnostics: Sequence[Diagnostic]) -> list[Diagnostic]:
    return [d for d in diagnostics if not d.is_level]
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_source.py -v`
Expected: 5 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/source.py tests/test_source.py
git commit -m "feat: source key parsing and cycle listing"
```

---

### Task 3: Synthetic fixture — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/fixture.py`
- Create: `tests/conftest.py`
- Test: `tests/test_fixture.py`

**Interfaces:**
- Produces: `FIXTURE_SLUGS = ("temperature_at_screen_level", "wind_speed_at_10m", "precipitation_accumulation-PT01H")`; `FIXTURE_LEADS_MIN = (0, 60, 120, 180)`; `build_cycle(root: Path, cycle: str, members=2, ny=8, nx=8, slugs=FIXTURE_SLUGS, leads=FIXTURE_LEADS_MIN) -> list[Path]`; the PT01H statistic is absent at lead 0 (mirrors real data). Files mimic the real encoding: variable named after CF name with dims `(realization, latitude, longitude)`, float32, HDF5 chunks `(1, 4, 4)`, zlib level 1, scalar coords `forecast_period` (seconds, int32), `forecast_reference_time`, `time`, `height`. Values are deterministic: `value = member*1000 + lead_hours*10 + lat_index + lon_index/100`.
- `fixture_root` pytest fixture returns `(Path, cycle)`; cycle `"2026/09/16/T0000Z"`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_fixture.py
import xarray as xr

from wxtco.fixture import FIXTURE_LEADS_MIN, FIXTURE_SLUGS, build_cycle
from wxtco.source import list_cycle
from wxtco.config import store_for


def test_build_cycle_layout(tmp_path):
    cycle = "2026/09/16/T0000Z"
    paths = build_cycle(tmp_path, cycle)
    # 3 slugs x 4 leads, minus the PT01H statistic at lead 0
    assert len(paths) == len(FIXTURE_SLUGS) * len(FIXTURE_LEADS_MIN) - 1
    diags = list_cycle(store_for(f"file://{tmp_path}"), cycle)
    assert [d.slug for d in diags] == sorted(FIXTURE_SLUGS)
    ds = xr.open_dataset(paths[0], engine="h5netcdf")
    (name,) = [v for v in ds.data_vars if ds[v].ndim == 3]
    assert ds[name].shape == (2, 8, 8)
    assert ds[name].encoding["chunksizes"] == (1, 4, 4)
    assert ds[name].dtype == "float32"
    assert "forecast_reference_time" in ds.coords and "forecast_period" in ds.coords


def test_values_deterministic(tmp_path):
    paths = build_cycle(tmp_path, "2026/09/16/T0000Z")
    p = [p for p in paths if "PT0002H00M-temperature" in p.name][0]
    ds = xr.open_dataset(p, engine="h5netcdf")
    v = ds["air_temperature"].values
    assert v[1, 3, 5] == 1000 + 20 + 3 + 0.05
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_fixture.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/fixture.py
"""Synthetic MOGREPS-like cycle for tests. Small grid, real encoding and key layout."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Sequence

import numpy as np
import xarray as xr

from wxtco.source import cycle_id, cycle_time

FIXTURE_SLUGS = ("temperature_at_screen_level", "wind_speed_at_10m", "precipitation_accumulation-PT01H")
FIXTURE_LEADS_MIN = (0, 60, 120, 180)
CF_NAMES = {
    "temperature_at_screen_level": ("air_temperature", "K", 1.5),
    "wind_speed_at_10m": ("wind_speed", "m s-1", 10.0),
    "precipitation_accumulation-PT01H": ("lwe_thickness_of_precipitation_amount", "m", None),
}


def _values(members: int, ny: int, nx: int, lead_hours: int) -> np.ndarray:
    m = np.arange(members)[:, None, None] * 1000.0
    y = np.arange(ny)[None, :, None] * 1.0
    x = np.arange(nx)[None, None, :] / 100.0
    return (m + lead_hours * 10 + y + x).astype("float32")


def build_cycle(
    root: Path,
    cycle: str,
    members: int = 2,
    ny: int = 8,
    nx: int = 8,
    slugs: Sequence[str] = FIXTURE_SLUGS,
    leads: Sequence[int] = FIXTURE_LEADS_MIN,
) -> list[Path]:
    """Write one synthetic cycle under `root/<cycle>/`. Return the file paths written."""
    init = cycle_time(cycle)
    out: list[Path] = []
    lat = np.linspace(-89.9, 89.9, ny, dtype="float32")
    lon = np.linspace(-179.9, 179.9, nx, dtype="float32")
    for slug in slugs:
        cf, units, height = CF_NAMES.get(slug, (slug, "1", None))
        for lead_min in leads:
            if slug.endswith("-PT01H") and lead_min == 0:
                continue
            valid = init + timedelta(minutes=lead_min)
            data = _values(members, ny, nx, lead_min // 60)
            coords = {
                "realization": ("realization", np.arange(members, dtype="int32")),
                "latitude": ("latitude", lat),
                "longitude": ("longitude", lon),
                "forecast_period": ((), np.int32(lead_min * 60), {"units": "seconds"}),
                "forecast_reference_time": ((), np.datetime64(init.replace(tzinfo=None), "ns")),
                "time": ((), np.datetime64(valid.replace(tzinfo=None), "ns")),
            }
            if height is not None:
                coords["height"] = ((), np.float32(height), {"units": "m"})
            ds = xr.Dataset({cf: (("realization", "latitude", "longitude"), data, {"units": units})}, coords=coords)
            ds.attrs.update({"institution": "Met Office", "title": "wxtco synthetic fixture", "mosg__model_configuration": "gl_ens"})
            name = f"{valid.strftime('%Y%m%dT%H%MZ')}-PT{lead_min // 60:04d}H{lead_min % 60:02d}M-{slug}.nc"
            path = root / cycle / name
            path.parent.mkdir(parents=True, exist_ok=True)
            ds.to_netcdf(
                path,
                engine="h5netcdf",
                encoding={cf: {"chunksizes": (1, min(4, ny), min(4, nx)), "zlib": True, "complevel": 1, "dtype": "float32"}},
            )
            out.append(path)
    return out
```

```python
# tests/conftest.py
from pathlib import Path

import pytest

from wxtco.fixture import build_cycle

CYCLE = "2026/09/16/T0000Z"


@pytest.fixture
def fixture_root(tmp_path: Path) -> tuple[Path, str]:
    """One synthetic cycle on disk. Returns (root, cycle)."""
    build_cycle(tmp_path, CYCLE)
    return tmp_path, CYCLE
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_fixture.py -v`
Expected: 2 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/fixture.py tests/conftest.py tests/test_fixture.py
git commit -m "feat: synthetic MOGREPS fixture"
```

---

### Task 4: Static copy tooling — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/copy.py`
- Create: `scripts/copy_cycle.sh`
- Test: `tests/test_copy.py`

**Interfaces:**
- Produces: `copy_commands(source_url, copy_url, cycles: Sequence[str], source_region, copy_region) -> list[str]` returning one `aws s3 cp --recursive` command per cycle that excludes level files; `verify_copy(src_store, dst_store, cycle) -> CopyReport(cycle, expected: int, present: int, missing: tuple[str, ...], bytes: int)` where expected counts surface files in the source.

- [x] **Step 1: Write the failing test**

```python
# tests/test_copy.py
import obstore as obs
from obstore.store import MemoryStore

from wxtco.copy import copy_commands, verify_copy


def test_copy_commands():
    cmds = copy_commands(
        "s3://src/global-ensemble", "s3://dst/netcdf", ["2026/09/16/T0000Z"], "eu-west-2", "us-east-1"
    )
    assert len(cmds) == 1
    c = cmds[0]
    assert "s3://src/global-ensemble/2026/09/16/T0000Z/ s3://dst/netcdf/2026/09/16/T0000Z/" in c
    assert "--exclude '*_on_*_levels.nc'" in c
    assert "--source-region eu-west-2" in c and "--region us-east-1" in c


def test_verify_copy_reports_missing():
    src, dst = MemoryStore(), MemoryStore()
    keys = [
        "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-a.nc",
        "2026/09/16/T0000Z/20260916T0100Z-PT0001H00M-a.nc",
        "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-x_on_height_levels.nc",
    ]
    for k in keys:
        obs.put(src, k, b"1234")
    obs.put(dst, keys[0], b"1234")
    r = verify_copy(src, dst, "2026/09/16/T0000Z")
    assert r.expected == 2 and r.present == 1
    assert r.missing == (keys[1],)
    assert r.bytes == 4
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_copy.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/copy.py
"""Static copy of source cycles. The AWS CLI does the transfer; this module plans and verifies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import obstore as obs
from obstore.store import ObjectStore

from wxtco.source import list_cycle, surface_only

LEVEL_GLOB = "*_on_*_levels.nc"


def copy_commands(source_url: str, copy_url: str, cycles: Sequence[str], source_region: str, copy_region: str) -> list[str]:
    """One `aws s3 cp` per cycle. Level files are excluded; everything else is copied as-is."""
    return [
        f"aws s3 cp --recursive --only-show-errors --source-region {source_region} --region {copy_region} "
        f"--exclude '{LEVEL_GLOB}' {source_url}/{c}/ {copy_url}/{c}/"
        for c in cycles
    ]


@dataclass(frozen=True)
class CopyReport:
    cycle: str
    expected: int
    present: int
    missing: tuple[str, ...]
    bytes: int


def verify_copy(src: ObjectStore, dst: ObjectStore, cycle: str) -> CopyReport:
    """Compare surface files in the source with the copy. Byte count is the copy's."""
    want = {f.key for d in surface_only(list_cycle(src, cycle)) for f in d.files}
    have = {item["path"]: item["size"] for page in obs.list(dst, prefix=f"{cycle}/") for item in page}
    missing = tuple(sorted(want - set(have)))
    return CopyReport(cycle, len(want), len(want) - len(missing), missing, sum(have.values()))
```

```bash
# scripts/copy_cycle.sh
#!/usr/bin/env bash
# Copy one MOGREPS-G cycle (surface files only) into the static copy bucket.
# Usage: scripts/copy_cycle.sh 2026/09/16/T0000Z
set -euo pipefail
CYCLE="${1:?cycle like 2026/09/16/T0000Z}"
SRC="${WXTCO_SOURCE_URL:-s3://met-office-global-ensemble-model-data/global-ensemble}"
DST="${WXTCO_COPY_URL:-s3://em-tco-mogreps/netcdf}"
aws s3 cp --recursive --only-show-errors --source-region eu-west-2 --region us-east-1 \
  --exclude '*_on_*_levels.nc' "$SRC/$CYCLE/" "$DST/$CYCLE/"
uv run wxtco copy verify --cycle "$CYCLE"
```

Run: `chmod +x scripts/copy_cycle.sh`

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_copy.py -v`
Expected: 2 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/copy.py scripts/copy_cycle.sh tests/test_copy.py
git commit -m "feat: static copy planning and verification"
```

---

### Task 5: Progress manifest — DONE 2026-09-18 by session 003 (obstore 0.11 raises builtin `FileNotFoundError`; `NotFoundError` is deprecated, so the code catches `FileNotFoundError`)

**Files:**
- Create: `src/wxtco/progress.py`
- Test: `tests/test_progress.py`

**Interfaces:**
- Produces: `Progress(store: ObjectStore, method: str, cycle: str)` with `done(unit: str) -> bool`, `mark(unit: str) -> None`, `units() -> set[str]`, `clear() -> None`. Stored as JSON at `_progress/<method>/<cycle_id>.json` in the given store. `mark` rewrites the whole file (units per cycle are at most a few hundred).

- [x] **Step 1: Write the failing test**

```python
# tests/test_progress.py
from obstore.store import MemoryStore

from wxtco.progress import Progress


def test_progress_roundtrip():
    store = MemoryStore()
    p = Progress(store, "table", "2026/09/16/T0000Z")
    assert not p.done("lead=0")
    p.mark("lead=0")
    p.mark("lead=60")
    assert p.done("lead=0") and p.done("lead=60")
    fresh = Progress(store, "table", "2026/09/16/T0000Z")
    assert fresh.units() == {"lead=0", "lead=60"}
    fresh.clear()
    assert Progress(store, "table", "2026/09/16/T0000Z").units() == set()


def test_progress_isolated_by_method_and_cycle():
    store = MemoryStore()
    Progress(store, "table", "2026/09/16/T0000Z").mark("u")
    assert not Progress(store, "virtual", "2026/09/16/T0000Z").done("u")
    assert not Progress(store, "table", "2026/09/16/T0600Z").done("u")
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_progress.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/progress.py
"""Per-(method, cycle) completion manifest so reruns skip finished units."""

from __future__ import annotations

import json

import obstore as obs
from obstore.exceptions import NotFoundError
from obstore.store import ObjectStore

from wxtco.source import cycle_id


class Progress:
    def __init__(self, store: ObjectStore, method: str, cycle: str) -> None:
        self.store = store
        self.key = f"_progress/{method}/{cycle_id(cycle)}.json"
        self._units: set[str] | None = None

    def units(self) -> set[str]:
        if self._units is None:
            try:
                self._units = set(json.loads(bytes(obs.get(self.store, self.key).bytes()))["done"])
            except NotFoundError:
                self._units = set()
        return self._units

    def done(self, unit: str) -> bool:
        return unit in self.units()

    def mark(self, unit: str) -> None:
        """Record one unit. Rewrites the manifest; unit counts per cycle are small."""
        self.units().add(unit)
        obs.put(self.store, self.key, json.dumps({"done": sorted(self.units())}).encode())

    def clear(self) -> None:
        self._units = set()
        obs.put(self.store, self.key, json.dumps({"done": []}).encode())
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_progress.py -v`
Expected: 2 PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/progress.py tests/test_progress.py
git commit -m "feat: progress manifest"
```

---

### Task 6: CLI subcommands for foundations — DONE 2026-09-18 by session 003

**Files:**
- Modify: `src/wxtco/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces CLI:
  - `wxtco fixture build <root> [--cycle C]`
  - `wxtco source cycles [--copy]` lists cycles in source (or in the copy)
  - `wxtco source inventory <cycle> [--copy]` prints per-diagnostic file count and bytes
  - `wxtco copy plan --cycles C1,C2` prints the `aws s3 cp` commands
  - `wxtco copy verify --cycle C` prints a `CopyReport`; exits 1 if files are missing
- Later plans add `ingest`, `bench`, `tco` sub-apps to the same `app`.

- [x] **Step 1: Write the failing test**

```python
# tests/test_cli.py
from typer.testing import CliRunner

from wxtco.cli import app

runner = CliRunner()


def test_fixture_build_and_source_cycles(tmp_path, monkeypatch):
    r = runner.invoke(app, ["fixture", "build", str(tmp_path), "--cycle", "2026/09/16/T0000Z"])
    assert r.exit_code == 0, r.output
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path}")
    r = runner.invoke(app, ["source", "cycles"])
    assert r.exit_code == 0 and "2026/09/16/T0000Z" in r.output
    r = runner.invoke(app, ["source", "inventory", "2026/09/16/T0000Z"])
    assert r.exit_code == 0 and "temperature_at_screen_level" in r.output


def test_copy_plan_and_verify(tmp_path, monkeypatch):
    runner.invoke(app, ["fixture", "build", str(tmp_path / "src")])
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path / 'src'}")
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'dst'}")
    r = runner.invoke(app, ["copy", "plan", "--cycles", "2026/09/16/T0000Z"])
    assert r.exit_code == 0 and "aws s3 cp" in r.output
    r = runner.invoke(app, ["copy", "verify", "--cycle", "2026/09/16/T0000Z"])
    assert r.exit_code == 1 and "missing" in r.output
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli.py -v`
Expected: FAIL (`No such command 'fixture'`)

- [x] **Step 3: Write implementation**

```python
# src/wxtco/cli.py
"""CLI entry point. Sub-apps: fixture, source, copy. Later plans add ingest, bench, tco."""

from __future__ import annotations

from pathlib import Path

import typer

from wxtco.config import Settings, store_for

app = typer.Typer(no_args_is_help=True, help="Weather forecast data TCO analysis")
fixture_app = typer.Typer(help="Synthetic test data")
source_app = typer.Typer(help="Inspect source or copy buckets")
copy_app = typer.Typer(help="Static copy of source cycles")
app.add_typer(fixture_app, name="fixture")
app.add_typer(source_app, name="source")
app.add_typer(copy_app, name="copy")

DEFAULT_CYCLE = "2026/09/16/T0000Z"


def _store(settings: Settings, use_copy: bool):
    """Anonymous source store, or the copy store with the ambient AWS credentials."""
    if use_copy:
        return store_for(settings.copy_url, region=settings.copy_region)
    return store_for(settings.source_url, region=settings.source_region, anonymous=settings.source_url.startswith("s3://met-office"))


@app.command()
def version() -> None:
    """Print the package version."""
    from importlib.metadata import version as v

    typer.echo(v("wxtco"))


@fixture_app.command("build")
def fixture_build(root: Path, cycle: str = DEFAULT_CYCLE) -> None:
    """Write one synthetic cycle under ROOT."""
    from wxtco.fixture import build_cycle

    paths = build_cycle(root, cycle)
    typer.echo(f"wrote {len(paths)} files under {root / cycle}")


@source_app.command("cycles")
def source_cycles(copy: bool = typer.Option(False, "--copy", help="List the static copy instead of the source")) -> None:
    """List cycles, oldest first."""
    from wxtco.source import available_cycles

    for c in available_cycles(_store(Settings.from_env(), copy)):
        typer.echo(c)


@source_app.command("inventory")
def source_inventory(cycle: str, copy: bool = typer.Option(False, "--copy")) -> None:
    """Per-diagnostic file count for one cycle."""
    from wxtco.source import list_cycle

    diags = list_cycle(_store(Settings.from_env(), copy), cycle)
    for d in diags:
        typer.echo(f"{len(d.files):4d}  {'level ' if d.is_level else 'surface'}  {d.slug}")
    typer.echo(f"{sum(len(d.files) for d in diags)} files, {len(diags)} diagnostics")


@copy_app.command("plan")
def copy_plan(cycles: str = typer.Option(..., help="Comma-separated cycles")) -> None:
    """Print the aws s3 cp commands for the given cycles."""
    from wxtco.copy import copy_commands

    s = Settings.from_env()
    for cmd in copy_commands(s.source_url, s.copy_url, cycles.split(","), s.source_region, s.copy_region):
        typer.echo(cmd)


@copy_app.command("verify")
def copy_verify(cycle: str = typer.Option(...)) -> None:
    """Compare the copy with the source for one cycle. Exit 1 if files are missing."""
    from wxtco.copy import verify_copy

    s = Settings.from_env()
    r = verify_copy(_store(s, False), _store(s, True), cycle)
    typer.echo(f"{r.cycle}: {r.present}/{r.expected} files present, {r.bytes / 1e9:.1f} GB, {len(r.missing)} missing")
    for k in r.missing[:20]:
        typer.echo(f"  missing {k}")
    raise typer.Exit(code=1 if r.missing else 0)
```

- [x] **Step 4: Run all tests**

Run: `uv run pytest -q`
Expected: all PASS

- [x] **Step 5: Commit**

```bash
git add src/wxtco/cli.py tests/test_cli.py
git commit -m "feat: fixture, source, and copy CLI"
```

---

### Task 7: Choose the study window and record it — DONE 2026-09-18 by session 003 (`study_cycles.txt` is written when the copy runs)

**Files:**
- Create: `src/wxtco/window.py`
- Modify: `docs/decisions/002-data-volume.md` (append the chosen cycles when the copy runs)
- Test: `tests/test_window.py`

**Interfaces:**
- Produces: `STUDY_CYCLES: list[str]` loaded from `study_cycles.txt` at repo root (one cycle per line, created when the copy is made); `pick_window(cycles: Sequence[str], days: int = 7, margin_days: int = 3) -> list[str]` choosing the newest `4*days` consecutive complete cycles whose oldest cycle is at least `margin_days` newer than the oldest available cycle.

- [x] **Step 1: Write the failing test**

```python
# tests/test_window.py
from wxtco.window import pick_window


def _cycles(days: int, start_day: int = 1) -> list[str]:
    return [f"2026/09/{d:02d}/T{h:02d}00Z" for d in range(start_day, start_day + days) for h in (0, 6, 12, 18)]


def test_pick_window_newest_week():
    avail = _cycles(30)
    w = pick_window(avail, days=7, margin_days=3)
    assert len(w) == 28
    assert w[-1] == avail[-1]
    assert w[0] == "2026/09/24/T0000Z"


def test_pick_window_needs_enough_cycles():
    import pytest

    with pytest.raises(ValueError):
        pick_window(_cycles(5), days=7)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_window.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/window.py
"""The study window: which 28 cycles the whole study uses."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from wxtco.source import cycle_time

WINDOW_FILE = Path(__file__).resolve().parents[2] / "study_cycles.txt"


def pick_window(cycles: Sequence[str], days: int = 7, margin_days: int = 3) -> list[str]:
    """Newest `4*days` cycles, provided the window starts `margin_days` after the oldest available."""
    n = 4 * days
    ordered = sorted(cycles, key=cycle_time)
    if len(ordered) < n:
        raise ValueError(f"need {n} cycles, have {len(ordered)}")
    window = ordered[-n:]
    if (cycle_time(window[0]) - cycle_time(ordered[0])).days < margin_days:
        raise ValueError("window starts too close to the deletion edge")
    return window


def study_cycles() -> list[str]:
    """Cycles pinned in study_cycles.txt at repo root. Empty until the copy is made."""
    if not WINDOW_FILE.exists():
        return []
    return [line.strip() for line in WINDOW_FILE.read_text().splitlines() if line.strip()]
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_window.py -v`
Expected: 2 PASS

- [x] **Step 5: Add `wxtco copy pick-window` command**

Append to `src/wxtco/cli.py`:

```python
@copy_app.command("pick-window")
def copy_pick_window(days: int = 7, write: bool = typer.Option(False, help="Write study_cycles.txt")) -> None:
    """Choose the study window from cycles available in the source."""
    from wxtco.source import available_cycles
    from wxtco.window import WINDOW_FILE, pick_window

    w = pick_window(available_cycles(_store(Settings.from_env(), False)), days=days)
    for c in w:
        typer.echo(c)
    if write:
        WINDOW_FILE.write_text("\n".join(w) + "\n")
        typer.echo(f"wrote {WINDOW_FILE}")
```

- [x] **Step 6: Run all tests and commit**

Run: `uv run pytest -q`

```bash
git add src/wxtco/window.py src/wxtco/cli.py tests/test_window.py
git commit -m "feat: study window selection"
```

---

### Task 8: Cloud bootstrap scripts (no cloud writes in tests) — DONE 2026-09-18 by session 001

**Files:**
- Create: `scripts/aws/create_bucket.sh`
- Create: `scripts/aws/iam_policy_wxtco.json`
- Create: `scripts/aws/README.md`

These are run once by a human or a session with AWS SSO active. They create the copy bucket
with tags, and define the IAM policy the EC2 instance role uses. No tests; verification is
`aws s3 ls s3://em-tco-mogreps/` succeeding.

- [x] **Step 1: Write the bucket script**

```bash
# scripts/aws/create_bucket.sh
#!/usr/bin/env bash
# Create the static copy bucket in the Earthmover Sandbox account. Idempotent.
set -euo pipefail
BUCKET="${WXTCO_BUCKET:-em-tco-mogreps}"
REGION="${WXTCO_COPY_REGION:-us-east-1}"
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "bucket $BUCKET exists"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
fi
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging 'TagSet=[{Key=project,Value=wxtco}]'
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# Request metrics let CloudWatch count GET/PUT per prefix for the cost model.
aws s3api put-bucket-metrics-configuration --bucket "$BUCKET" --id wxtco-all \
  --metrics-configuration '{"Id":"wxtco-all"}'
echo "ok: s3://$BUCKET in $REGION"
```

- [x] **Step 2: Write the IAM policy**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CopyBucketReadWrite",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::em-tco-mogreps", "arn:aws:s3:::em-tco-mogreps/*"]
    },
    {
      "Sid": "SourceBucketRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::met-office-global-ensemble-model-data", "arn:aws:s3:::met-office-global-ensemble-model-data/*"]
    },
    {
      "Sid": "CodeTarballRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::em-tco-mogreps/_code/*"
    },
    {
      "Sid": "ArraylakeTokenSecret",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:us-east-1:<ACCOUNT_ID>:secret:wxtco/arraylake-token-*"
    },
    {
      "Sid": "JobLogs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "*"
    }
  ]
}
```

- [x] **Step 3: Write the controller policy**

The cloud session (see `docs/infra/cloud-session.md`) holds only this user's key. It can start,
watch, and stop tagged instances and read job status; it cannot touch forecast data or the token.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LaunchTaggedInstances",
      "Effect": "Allow",
      "Action": ["ec2:RunInstances"],
      "Resource": "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:instance/*",
      "Condition": {"StringEquals": {"aws:RequestTag/project": "wxtco"}}
    },
    {
      "Sid": "LaunchSupportingResources",
      "Effect": "Allow",
      "Action": ["ec2:RunInstances", "ec2:CreateTags"],
      "Resource": [
        "arn:aws:ec2:us-east-1::image/*",
        "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:volume/*",
        "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:network-interface/*",
        "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:subnet/*",
        "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:security-group/*",
        "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:instance/*"
      ]
    },
    {
      "Sid": "ManageTaggedInstances",
      "Effect": "Allow",
      "Action": ["ec2:TerminateInstances", "ec2:StopInstances"],
      "Resource": "arn:aws:ec2:us-east-1:<ACCOUNT_ID>:instance/*",
      "Condition": {"StringEquals": {"aws:ResourceTag/project": "wxtco"}}
    },
    {
      "Sid": "Describe",
      "Effect": "Allow",
      "Action": ["ec2:Describe*", "ssm:GetParameter", "ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation", "ce:GetCostAndUsage", "cloudwatch:GetMetricStatistics"],
      "Resource": "*"
    },
    {
      "Sid": "RunCommandsOnTaggedInstances",
      "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": ["arn:aws:ec2:us-east-1:<ACCOUNT_ID>:instance/*"],
      "Condition": {"StringEquals": {"ssm:resourceTag/project": "wxtco"}}
    },
    {
      "Sid": "RunShellDocument",
      "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": "arn:aws:ssm:us-east-1::document/AWS-RunShellScript"
    },
    {
      "Sid": "PassInstanceRole",
      "Effect": "Allow",
      "Action": ["iam:PassRole"],
      "Resource": "arn:aws:iam::<ACCOUNT_ID>:role/wxtco-ec2"
    },
    {
      "Sid": "ControlPrefixesOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": ["arn:aws:s3:::em-tco-mogreps/_code/*", "arn:aws:s3:::em-tco-mogreps/_logs/*", "arn:aws:s3:::em-tco-mogreps/_progress/*"]
    },
    {
      "Sid": "ListControlPrefixes",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::em-tco-mogreps",
      "Condition": {"StringLike": {"s3:prefix": ["_code/*", "_logs/*", "_progress/*"]}}
    }
  ]
}
```

Save as `scripts/aws/iam_policy_wxtco_controller.json`.

- [x] **Step 4: Write the README**

```markdown
# AWS bootstrap

Account `<ACCOUNT_ID>` (Earthmover Sandbox), region `us-east-1`, profile `PowerUserAccess-<ACCOUNT_ID>`.

One-time, with an active SSO session (`aws sso login --sso-session ryans-laptop-session`):

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>
    scripts/aws/create_bucket.sh
    aws iam create-policy --policy-name wxtco-ec2 --policy-document file://scripts/aws/iam_policy_wxtco.json --tags Key=project,Value=wxtco
    aws iam create-role --role-name wxtco-ec2 --tags Key=project,Value=wxtco \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    aws iam attach-role-policy --role-name wxtco-ec2 --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/wxtco-ec2
    aws iam attach-role-policy --role-name wxtco-ec2 --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
    aws iam create-instance-profile --instance-profile-name wxtco-ec2
    aws iam add-role-to-instance-profile --instance-profile-name wxtco-ec2 --role-name wxtco-ec2
    aws secretsmanager create-secret --name wxtco/arraylake-token --secret-string "$ARRAYLAKE_TOKEN" --tags Key=project,Value=wxtco

Controller user for the cloud session (key goes into the cloud environment variables, nowhere else):

    aws iam create-user --user-name wxtco-controller --tags Key=project,Value=wxtco
    aws iam create-policy --policy-name wxtco-controller --policy-document file://scripts/aws/iam_policy_wxtco_controller.json --tags Key=project,Value=wxtco
    aws iam attach-user-policy --user-name wxtco-controller --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/wxtco-controller
    aws iam create-access-key --user-name wxtco-controller   # copy AccessKeyId and SecretAccessKey once

Rotate or delete the key at project end: `aws iam delete-access-key --user-name wxtco-controller --access-key-id ...`.

Record the outcome in `docs/infra/access-checklist.md`.
```

- [x] **Step 5: Commit**

```bash
chmod +x scripts/aws/create_bucket.sh
git add scripts/aws
git commit -m "chore: AWS bootstrap scripts for bucket, IAM roles, controller user, secret"
```

---

### Task 9: Cloud session support — DONE 2026-09-18 by session 001

**Files:**
- Create: `docs/infra/cloud-session.md`
- Modify: `.claude/settings.json` (SessionStart hook)
- Create: `scripts/cloud_setup.sh`

No unit tests. Verified by starting a cloud session and seeing `uv run pytest -q` pass.

- [x] **Step 1: SessionStart hook**

Merge into `.claude/settings.json`:

```json
{
  "enabledPlugins": {"superpowers@claude-plugins-official": true},
  "hooks": {
    "SessionStart": [
      {
        "matcher": "",
        "hooks": [
          {"type": "command", "command": "if [ \"$CLAUDE_CODE_REMOTE\" = \"true\" ]; then uv sync --frozen >/dev/null 2>&1 || true; fi"}
        ]
      }
    ]
  }
}
```

- [x] **Step 2: Environment setup script (pasted into the cloud environment's setup script field)**

```bash
# scripts/cloud_setup.sh
#!/usr/bin/env bash
# Cloud environment setup script. Runs once as root, cached as a filesystem snapshot.
set -euxo pipefail
curl -LsSf https://astral.sh/uv/install.sh | sh
ln -sf "$HOME/.local/bin/uv" /usr/local/bin/uv
curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip
cd /tmp && unzip -q awscli.zip && ./aws/install
uv --version && aws --version
```

- [x] **Step 3: Write `docs/infra/cloud-session.md`**

```markdown
# Running this project from a Claude Code cloud session

The cloud VM (4 vCPU, 16 GB) is a controller. It edits code, runs fixture tests, launches EC2
jobs, and reads job status. All data movement happens on EC2 under the `wxtco-ec2` instance role.

## Environment configuration (claude.ai/code, once)

| setting | value |
|---|---|
| repository | `earth-mover/wxtco-icechunk-v-iceberg` (Claude GitHub App installed) |
| network access | Custom: default allowlist plus `api.earthmover.io` |
| setup script | contents of `scripts/cloud_setup.sh` |
| environment variables | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (user `wxtco-controller`), `AWS_DEFAULT_REGION=us-east-1`, `ARRAYLAKE_ORG=wxtco` |
| API credential (optional) | `ARRAYLAKE_TOKEN` as Bearer for host `api.earthmover.io`, only if the session itself must call Arraylake |

Never put the `wxtco-ec2` role, the copy bucket write key, or the Arraylake token in environment
variables. Environment variables are plain text to anyone who can use the environment.

## What the session does

    uv run pytest -q                                  # fixture tests, local
    scripts/ec2/launch.sh c7i.16xlarge wxtco-ingest   # start a job machine
    scripts/ec2/run_job.sh <id> table-c01 -- uv run wxtco ingest table --cycle ...
    uv run wxtco jobs status                          # read _progress/ and _logs/
    scripts/ec2/terminate.sh <id>

Jobs are detached (SSM + nohup). A session that dies from inactivity loses nothing: the job
continues on EC2, and every ingest is idempotent per unit, so a new session resumes by rerunning
the same command. Use `/loop 20m uv run wxtco jobs status` to keep a session polling.

## Code delivery to EC2

EC2 never talks to GitHub. `scripts/ec2/run_job.sh` uploads `git archive HEAD` to
`s3://em-tco-mogreps/_code/<sha>.tar.gz` and the instance unpacks it. Commit before launching.

## Rotation

Delete the `wxtco-controller` access key when the study ends.
```

- [x] **Step 4: Commit**

```bash
chmod +x scripts/cloud_setup.sh
git add .claude/settings.json scripts/cloud_setup.sh docs/infra/cloud-session.md
git commit -m "chore: cloud session setup and controller documentation"
```

---

## Self-review notes

- Spec §3 (data, window, copy): Tasks 2, 4, 7, 8. Spec §5 idempotency: Task 5. Spec §9 synthetic fixture: Task 3. Spec §8 `config.py`, `source.py`, `copy.py`, `progress.py`: Tasks 1, 2, 4, 5. `ingest/progress.py` in the spec is `wxtco/progress.py` here because both ingest and bench use it.
- Interfaces used by later plans: `Settings.from_env()`, `store_for`, `list_cycle`, `surface_only`, `Diagnostic`, `SourceFile`, `cycle_time`, `cycle_id`, `Progress`, `build_cycle`, `fixture_root`, `study_cycles()`.
