# Stage A, Plan 03: Virtual Ingest (NetCDF to virtual Icechunk) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a virtual Icechunk repo of MOGREPS-G surface diagnostics whose chunks reference the static NetCDF copy, adapting the reference virtualization pipeline with the source bucket, origin cycle, and repo target made configurable, and Modal removed.

**Architecture:** Adapt `virtualize_met_office_mogreps_g.py` into `src/wxtco/ingest/mogreps_virtual.py` unchanged except for a small set of edits that route constants through `wxtco.config` and accept a local `file://` source for tests. A thin `wxtco/ingest/virtual.py` drives it: resolves the repo (local Icechunk for tests, Arraylake for production), runs one cycle, marks `Progress`. Groups, dims, per-variable commits, status arrays, manifest splitting, and the append-only init axis all come from Joe's code.

**Tech Stack:** virtualizarr 2.7 (HDF parser), icechunk 2.2, arraylake 1.3, obstore, obspec-utils, xarray, zarr 3.

**Spec:** `docs/superpowers/specs/2026-09-18-tco-study-design.md` (sections 4.2, 5).

## Global Constraints

- Same as Plan 01. Depends on Plan 01: `Settings`, `store_for`, `split_url`, `Progress`, `fixture_root`, `cycle_time`.
- Adapted code keeps the original style and comments. Edits are marked with a comment so a diff against upstream stays readable.
- Repo name: `<org>/mogreps-g-virtual`. Virtual chunk container URL prefix = `settings.copy_url` with a trailing slash (`s3://em-tco-mogreps/netcdf/`).
- `ORIGIN_CYCLE` = the first cycle in `study_cycles.txt`; the init axis is append-only from there.
- Level diagnostics are never ingested (`--include-levels` is not exposed).
- Session 003 notes: the reference pipeline is append-only (no ring; its README is stale). `prod_repo` must pass `authorize_virtual_chunk_access` to `get_repo` as well as `create_repo` (fixed in the adapted copy). The adapted file is excluded from `ruff format`.

---

## File map

| file | responsibility |
|---|---|
| `src/wxtco/ingest/mogreps_virtual.py` | adapted pipeline, parametrized |
| `src/wxtco/ingest/virtual.py` | `open_virtual_repo()`, `ingest_cycle_virtual()` |
| `src/wxtco/cli.py` | add `ingest virtual` |
| `tests/test_virtual_ingest.py` | end-to-end on the fixture into a local Icechunk repo |
| `docs/infra/runbook-virtual-ingest.md` | production commands |

---

### Task 1: Adapt the reference script — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/ingest/mogreps_virtual.py`
- Modify: `pyproject.toml` (add `obspec-utils`)

- [x] **Step 1: Fetch the file at the pinned commit**

```bash
uv add obspec-utils
curl -sSL <reference pipeline URL> \
  -o src/wxtco/ingest/mogreps_virtual.py
```

If the repo is private and `curl` returns 404, use `the GitHub API --jq .content | base64 -d > src/wxtco/ingest/mogreps_virtual.py`.

- [x] **Step 2: Remove the PEP 723 header and `__main__` block**

Delete lines 1 to 14 (the `#!/usr/bin/env -S uv run --script` shebang and the `# /// script ... # ///` block). Delete the final two lines (`if __name__ == "__main__": sys.exit(run(parse_args(sys.argv[1:])))`).

- [x] **Step 3: Parametrize the source**

Replace the constants block

```python
BUCKET_NAME = "met-office-global-ensemble-model-data"
BUCKET_URL = f"s3://{BUCKET_NAME}/"
REGION = "eu-west-2"
ROOT_PREFIX = "global-ensemble"
```

with

```python
# Source is the static copy, from wxtco.config. `file://` roots are used by tests.
from wxtco.config import Settings, split_url

_SETTINGS = Settings.from_env()
BUCKET_URL = _SETTINGS.copy_url.rstrip("/") + "/"
BUCKET_NAME, ROOT_PREFIX = split_url(_SETTINGS.copy_url)
REGION = _SETTINGS.copy_region
```

Replace `source_store()`:

```python
@cache
def source_store():
    # Local store for tests, S3 with ambient credentials for the copy bucket.
    from obstore.store import LocalStore, S3Store

    if BUCKET_URL.startswith("file://"):
        return LocalStore(BUCKET_NAME)
    return S3Store(BUCKET_NAME, prefix=ROOT_PREFIX or None, region=REGION)
```

In `available_cycles` and `list_cycle`, the prefix `f"{ROOT_PREFIX}/"` becomes `""` when the store already carries the prefix. Replace every `f"{ROOT_PREFIX}/{...}"` with `f"{...}"` and `f"{ROOT_PREFIX}/"` with `""`. For the `file://` case `BUCKET_NAME` is the directory and `ROOT_PREFIX` is `""`, so this is consistent.

- [x] **Step 4: Parametrize the origin cycle and completeness gate**

Replace

```python
ORIGIN_CYCLE = "2026/08/15/T0000Z"
```

with

```python
# The study window's first cycle. Set by wxtco.ingest.virtual before run().
ORIGIN_CYCLE = "2026/01/01/T0000Z"
```

and make `run()` read `SINGLE_LEVEL_FILES` only when `not args.allow_incomplete` (already the case). The driver in Task 2 always passes `allow_incomplete=True` for fixtures and `False` for production.

- [x] **Step 5: Make the dry-run repo accept a virtual container for `file://`**

Replace `dry_run_repo`:

```python
def dry_run_repo(path: str | None = None) -> ic.Repository:
    config = ic.RepositoryConfig.default()
    config.manifest = manifest_config()
    # The virtual container must match the source root, local or S3.
    if BUCKET_URL.startswith("file://"):
        config.set_virtual_chunk_container(ic.VirtualChunkContainer(BUCKET_URL, ic.local_filesystem_store(BUCKET_NAME)))
        credentials = None
    else:
        config.set_virtual_chunk_container(ic.VirtualChunkContainer(BUCKET_URL, ic.s3_store(region=REGION)))
        credentials = ic.Credentials.S3(ic.S3Credentials.FromEnv())
    storage = ic.local_filesystem_storage(path) if path else ic.in_memory_storage()
    return ic.Repository.open_or_create(storage, config=config, authorize_virtual_chunk_access={BUCKET_URL: credentials})
```

Check the exact constructor names with `uv run python -c "import icechunk as ic; print([n for n in dir(ic) if 'store' in n.lower()]); print(dir(ic.S3Credentials))"` and adjust if `local_filesystem_store` or `FromEnv` are named differently in icechunk 2.2.2.

- [x] **Step 6: Point `prod_repo` at the existing bucket configs (decision 013)**

Bucket configs and the VCAP are created by `scripts/aws/setup_arraylake_storage.sh`, not by
code. Remove the `create_bucket_config` and `set_virtual_chunk_access_policy` calls from
`prod_repo` and replace them with a check:

```python
    # Bucket configs come from scripts/aws/setup_arraylake_storage.sh (decision 013).
    nicknames = {b.nickname for b in client.list_bucket_configs(org)}
    if nickname not in nicknames:
        raise SystemExit(f"bucket config {nickname} missing on org {org}; run scripts/aws/setup_arraylake_storage.sh")
```

`wxtco.ingest.virtual` passes `nickname="wxtco-netcdf"` and `storage_nickname=None` (org default
`wxtco-storage`). Keep `authorize_virtual_chunk_access={BUCKET_URL: nickname}` in `create_repo`.

- [x] **Step 7: Import check**

Run: `uv run python -c "import wxtco.ingest.mogreps_virtual as m; print(m.BUCKET_URL, m.ORIGIN_CYCLE)"`
Expected: prints `s3://em-tco-mogreps/netcdf/ 2026/01/01/T0000Z` with no import error.

- [x] **Step 8: Attribution and commit**

```markdown
```bash
git add pyproject.toml uv.lock src/wxtco/ingest/mogreps_virtual.py
git commit -m "feat: adapt MOGREPS-G virtualization pipeline, parametrized for the static copy"
```

---

### Task 2: Driver with local repo and progress — DONE 2026-09-18 by session 003

**Files:**
- Create: `src/wxtco/ingest/virtual.py`
- Test: `tests/test_virtual_ingest.py`

**Interfaces:**
- Produces:
  - `VirtualTarget = Literal["local", "arraylake"]`
  - `open_virtual_repo(settings, target, local_path: Path | None = None, cycle: str = "") -> ic.Repository`
  - `ingest_cycle_virtual(repo, progress_store, cycle, origin_cycle, workers=8, variables: Sequence[str] | None = None, leads: int | None = None, allow_incomplete=False, log=print) -> IngestReport` (same `IngestReport` dataclass as Plan 02 Task 5, imported from `wxtco.ingest.table`). Unit id is `"cycle"` (Joe's pipeline commits once per cycle and resumes per cycle itself, so one unit per cycle).
  - `open_virtual_dataset_group(repo, group="surface") -> xr.Dataset` (read side, used by Plan 04).

- [x] **Step 1: Write the failing test**

```python
# tests/test_virtual_ingest.py
import numpy as np
import xarray as xr

from wxtco.config import Settings, store_for
from wxtco.fixture import build_cycle
from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_dataset_group, open_virtual_repo


def test_virtual_ingest_two_cycles(tmp_path, monkeypatch):
    c0, c1 = "2026/09/16/T0000Z", "2026/09/16/T0600Z"
    build_cycle(tmp_path / "src", c0)
    build_cycle(tmp_path / "src", c1)
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'src'}")
    # the adapted module reads settings at import; import after the env is set
    import importlib
    import wxtco.ingest.mogreps_virtual as mv
    importlib.reload(mv)

    s = Settings.from_env()
    repo = open_virtual_repo(s, "local", local_path=tmp_path / "repo")
    prog = store_for(f"file://{tmp_path / 'prog'}")
    r0 = ingest_cycle_virtual(repo, prog, c0, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None)
    assert r0.units_done == 1
    r1 = ingest_cycle_virtual(repo, prog, c1, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None)
    assert r1.units_done == 1
    again = ingest_cycle_virtual(repo, prog, c1, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None)
    assert again.units_skipped == 1

    ds = open_virtual_dataset_group(repo, "surface")
    assert "temperature_at_screen_level" in ds
    assert ds["temperature_at_screen_level"].dims == ("forecast_reference_time", "forecast_period", "realization", "latitude", "longitude")
    assert ds.sizes["forecast_reference_time"] >= 2 and ds.sizes["forecast_period"] == 4
    # slot 1 = second cycle, lead index 2 (120 min), member 1, lat 3, lon 5
    v = ds["temperature_at_screen_level"].isel(forecast_reference_time=1, forecast_period=2, realization=1, latitude=3, longitude=5).values
    assert v == np.float32(1000 + 20 + 3 + 0.05)
    st = xr.open_zarr(repo.readonly_session("main").store, group="surface/status", consolidated=False, zarr_format=3)
    assert int(st["temperature_at_screen_level"].isel(forecast_reference_time=1).max()) == 0
```

Note: the fixture's `precipitation_accumulation-PT01H` has 3 leads and lands in a `surface_PT01H` group; only `surface` is asserted.

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_virtual_ingest.py -v`
Expected: FAIL with `ModuleNotFoundError: wxtco.ingest.virtual`

- [x] **Step 3: Write implementation**

```python
# src/wxtco/ingest/virtual.py
"""Drive the adapted MOGREPS-G virtualization for one cycle into a local or Arraylake repo."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Callable, Literal, Sequence

import icechunk as ic
import xarray as xr
from obstore.store import ObjectStore

from wxtco.config import Settings
from wxtco.ingest import mogreps_virtual as mv
from wxtco.ingest.table import IngestReport
from wxtco.progress import Progress

VirtualTarget = Literal["local", "arraylake"]
REPO_NAME = "mogreps-g-virtual"
BUCKET_NICKNAME = "wxtco-netcdf"


def open_virtual_repo(settings: Settings, target: VirtualTarget, local_path: Path | None = None, cycle: str = "") -> ic.Repository:
    if target == "local":
        return mv.dry_run_repo(str(local_path) if local_path else None)
    return mv.prod_repo(settings.arraylake_org, REPO_NAME, BUCKET_NICKNAME, None, cycle)


def _args(cycle: str, workers: int, variables: Sequence[str] | None, leads: int | None, allow_incomplete: bool) -> argparse.Namespace:
    """The namespace Joe's run() expects, minus the repo fields it no longer needs."""
    return argparse.Namespace(
        cycle=cycle, workers=workers, variables=list(variables or []), include_levels=False, limit=None, leads=leads,
        plan=False, dry_run=True, dry_run_path=None, fresh=False, allow_incomplete=allow_incomplete, horizon_hours=0,
        org=None, repo=None, bucket_nickname=None, storage_nickname=None,
    )


def ingest_cycle_virtual(
    repo: ic.Repository,
    progress_store: ObjectStore,
    cycle: str,
    origin_cycle: str,
    workers: int = 8,
    variables: Sequence[str] | None = None,
    leads: int | None = None,
    allow_incomplete: bool = False,
    log: Callable[[str], None] = print,
) -> IngestReport:
    """One cycle, one unit. Joe's pipeline resumes per variable internally."""
    started = time.perf_counter()
    progress = Progress(progress_store, "virtual", cycle)
    if progress.done("cycle"):
        return IngestReport(cycle, 0, 1, 0, 0.0)
    mv.ORIGIN_CYCLE = origin_cycle
    mv.run_with_repo(repo, _args(cycle, workers, variables, leads, allow_incomplete), log=log)
    progress.mark("cycle")
    return IngestReport(cycle, 1, 0, 0, time.perf_counter() - started)


def open_virtual_dataset_group(repo: ic.Repository, group: str = "surface") -> xr.Dataset:
    return xr.open_zarr(repo.readonly_session("main").store, group=group, consolidated=False, zarr_format=3)
```

- [x] **Step 4: Add `run_with_repo` to the adapted module**

Joe's `run(args)` builds the repo itself from `args`. Split it: rename the body after the repo is chosen into `run_with_repo(repo, args, log=print)` and have `run(args)` call it. Concretely, in `mogreps_virtual.py`:

```python
def run(args: argparse.Namespace) -> int:
    # Kept for parity with upstream; wxtco.ingest.virtual calls run_with_repo directly.
    repo = dry_run_repo(args.dry_run_path) if args.dry_run else prod_repo(args.org, args.repo, args.bucket_nickname, args.storage_nickname, args.cycle)
    return run_with_repo(repo, args)


def run_with_repo(repo: ic.Repository, args: argparse.Namespace, log=print) -> int:
    started = time.perf_counter()
    cycle = resolve_cycle(args.cycle)
    ...  # the original body of run(), with `repo = ...` removed and every print(...) replaced by log(...)
```

Keep the `--fresh` handling (`if args.fresh and not args.dry_run: reset_to_root(repo)`) and everything else. Ensure `resolve_cycle` still lists the store; with the fixture it finds the two synthetic cycles.

- [x] **Step 5: Run the test; iterate on the adapted edits until it passes**

Run: `uv run pytest tests/test_virtual_ingest.py -v -s`
Expected: PASS. Likely failures and fixes:
- `SINGLE_LEVEL_FILES` gate: `allow_incomplete=True` bypasses it.
- Seeding rule "first ingest must be the origin cycle": the test passes `origin_cycle=c0` and ingests `c0` first.
- `refusing to seed a real repo from a truncated cycle`: only applies when `not args.dry_run`; the namespace sets `dry_run=True` for the local target. For the Arraylake target, `ingest_cycle_virtual` must pass `dry_run=False`; add a `production: bool` parameter that flips it, default `False`.
- Manifest split rule names (`UNSPLIT_NAMES`, `DATA_SPLIT_CYCLES`) are module constants near `manifest_config`; keep them.

- [x] **Step 6: Commit**

```bash
git add src/wxtco/ingest/virtual.py src/wxtco/ingest/mogreps_virtual.py tests/test_virtual_ingest.py
git commit -m "feat: virtual ingest driver over adapted pipeline"
```

---

### Task 3: `wxtco ingest virtual` CLI — DONE 2026-09-18 by session 003

**Files:**
- Modify: `src/wxtco/cli.py`
- Test: `tests/test_cli_ingest.py`

**Interfaces:**
- `wxtco ingest virtual --cycle C [--local PATH] [--workers N] [--variables a,b] [--leads N] [--allow-incomplete]`. Origin cycle = first line of `study_cycles.txt`, or `--origin C` for local runs.

- [x] **Step 1: Write the failing test**

Append to `tests/test_cli_ingest.py`:

```python
def test_ingest_virtual_local(tmp_path, monkeypatch):
    from wxtco.fixture import build_cycle

    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'src'}")
    import importlib, wxtco.ingest.mogreps_virtual as mv
    importlib.reload(mv)
    r = runner.invoke(app, ["ingest", "virtual", "--cycle", cycle, "--origin", cycle, "--local", str(tmp_path / "repo"), "--workers", "2", "--allow-incomplete"])
    assert r.exit_code == 0, r.output
    assert "1 units done" in r.output
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli_ingest.py::test_ingest_virtual_local -v`
Expected: FAIL (`No such command 'virtual'`)

- [x] **Step 3: Implement**

Append to `src/wxtco/cli.py`:

```python
@ingest_app.command("virtual")
def ingest_virtual(
    cycle: str = typer.Option(...),
    local: Path | None = typer.Option(None, help="Local Icechunk repo path instead of Arraylake"),
    origin: str | None = typer.Option(None, help="Origin cycle; default first line of study_cycles.txt"),
    workers: int = 8,
    variables: str | None = None,
    leads: int | None = None,
    allow_incomplete: bool = False,
) -> None:
    """Ingest one cycle as virtual chunk references."""
    from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_repo
    from wxtco.window import study_cycles

    s = Settings.from_env()
    origin_cycle = origin or (study_cycles() or [cycle])[0]
    if local is not None:
        repo, progress_store = open_virtual_repo(s, "local", local_path=local), store_for(f"file://{local}_progress")
    else:
        repo, progress_store = open_virtual_repo(s, "arraylake", cycle=cycle), _store(s, True)
    r = ingest_cycle_virtual(
        repo, progress_store, cycle, origin_cycle, workers=workers,
        variables=variables.split(",") if variables else None, leads=leads, allow_incomplete=allow_incomplete,
        production=local is None, log=typer.echo,
    )
    typer.echo(f"{r.cycle}: {r.units_done} units done, {r.units_skipped} skipped, {r.seconds:.0f}s")
```

- [x] **Step 4: Run all tests and commit**

Run: `uv run pytest -q`

```bash
git add src/wxtco/cli.py tests/test_cli_ingest.py
git commit -m "feat: ingest virtual CLI"
```

---

### Task 4: Production runbook — DONE 2026-09-18 by session 003

**Files:**
- Create: `docs/infra/runbook-virtual-ingest.md`

```markdown
# Runbook: virtual ingest

Prereqs: `.env` has `ARRAYLAKE_TOKEN`, `ARRAYLAKE_ORG`; `study_cycles.txt`; copy verified; an org
admin has set the virtual chunk access policy for `s3://em-tco-mogreps/netcdf/` (Joe's README:
`set_virtual_chunk_access_policy` is admin-only and logs a 403 otherwise).

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID>
    CYCLE=$(head -1 study_cycles.txt)
    # plan only, no writes
    uv run python -c "import wxtco.ingest.mogreps_virtual as m, sys; sys.exit(m.run(m.parse_args(['--cycle','$CYCLE','--plan'])))"
    # first cycle seeds the repo; must be the origin cycle, full selection
    uv run wxtco ingest virtual --cycle "$CYCLE" --workers 48
    # remaining cycles, in order
    tail -n +2 study_cycles.txt | while read c; do uv run wxtco ingest virtual --cycle "$c" --workers 48; done

Expect CPU-bound parsing at ~1 s/file/core in-region. Record wall clock per cycle in
`docs/findings/etl/virtual.csv` (columns: cycle, instance_type, workers, seconds, files).
```

- [x] **Step 1: Commit**

```bash
git add docs/infra/runbook-virtual-ingest.md
git commit -m "chore: virtual ingest runbook"
```

---

## Self-review notes

- Spec §4.2: groups, dims, HDF chunk references, append-only axis, no Modal: Task 1 (vendor + edits), Task 2 (driver). §5 idempotency: `Progress` per cycle plus Joe's per-cycle resume.
- The adapted module reads `Settings` at import. Tests set `WXTCO_COPY_URL` and `importlib.reload` it. Plan 04 must not import `mogreps_virtual` before settings are final; `wxtco.ingest.virtual` imports it lazily enough because the CLI imports inside the command function.
- Bucket access is decided (decision 013): delegation role `wxtco-arraylake`, bucket configs `wxtco-storage` and `wxtco-netcdf`, private VCAP. Created by `scripts/aws/setup_arraylake_storage.sh` before this plan runs.
