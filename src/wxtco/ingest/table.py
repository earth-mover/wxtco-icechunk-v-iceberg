"""NetCDF to wide Arrow rows, one lead at a time, then Iceberg append."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pyarrow as pa
from obstore.store import ObjectStore
from pyiceberg.catalog import Catalog

from wxtco.catalog import ensure_table
from wxtco.ingest.hilbert import row_permutation
from wxtco.ingest.netcdf import grid_values, open_netcdf
from wxtco.ingest.table_schema import SORT, arrow_schema, column_name, progress_method
from wxtco.progress import Progress
from wxtco.source import Diagnostic, SourceFile, cycle_time, list_cycle, surface_only


def files_by_lead(diagnostics: Sequence[Diagnostic]) -> dict[int, dict[str, SourceFile]]:
    """Group the files of all diagnostics by lead: lead_minutes -> {slug: file}."""
    out: dict[int, dict[str, SourceFile]] = {}
    for d in diagnostics:
        for f in d.files:
            out.setdefault(f.lead_minutes, {})[d.slug] = f
    return dict(sorted(out.items()))


def _grid(store: ObjectStore, f: SourceFile) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Read one file. Give the (member, ny, nx) grid with its lat and lon coordinates."""
    ds = open_netcdf(store, f.key)
    lat = ds["latitude"].values.astype("float32")
    lon = ds["longitude"].values.astype("float32")
    return grid_values(ds), lat, lon


def lead_table(
    store: ObjectStore,
    cycle: str,
    lead_minutes: int,
    files: dict[str, SourceFile],
    slugs: Sequence[str],
    workers: int = 8,
) -> pa.Table:
    """One Arrow table for one lead. Rows ordered member, lat, lon, or Hilbert cell then member when
    WXTCO_TABLE_SORT=hilbert. Missing diagnostics are NULL.
    Peak memory is `workers` open files plus all decoded grids (~7 GB for 81 vars at full grid)."""
    if not files:
        raise ValueError(f"no files for lead {lead_minutes}")
    if lead_minutes % 60 != 0:
        raise ValueError(f"lead {lead_minutes} is not a whole hour; lead_hours would collide")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        grids = dict(zip(files, pool.map(lambda f: _grid(store, f), files.values())))
    first, lat, lon = next(iter(grids.values()))
    # Every file of one lead must share the grid, or the flat rows would not line up.
    for slug, (grid, glat, glon) in grids.items():
        if grid.shape != first.shape or not np.array_equal(glat, lat) or not np.array_equal(glon, lon):
            raise ValueError(f"grid mismatch for {slug} at lead {lead_minutes}")
    members, ny, nx = first.shape
    n = members * ny * nx
    init = np.datetime64(cycle_time(cycle).replace(tzinfo=None), "us")
    valid = np.datetime64((cycle_time(cycle) + timedelta(minutes=lead_minutes)).replace(tzinfo=None), "us")
    # Broadcast the three coordinates to the grid shape in C order. ravel() then matches grid.reshape(-1).
    member, lat2d, lon2d = np.meshgrid(np.arange(members, dtype="int32"), lat, lon, indexing="ij")
    # Hilbert: one gather per column with a permutation fixed by the grid shape.
    perm = row_permutation(members, ny, nx) if SORT == "hilbert" else None

    def flat(a: np.ndarray) -> np.ndarray:
        v = a.reshape(-1)
        return v[perm] if perm is not None else v

    schema = arrow_schema(slugs)
    columns: dict[str, pa.Array] = {
        "init_time": pa.array(np.full(n, init), pa.timestamp("us")),
        "lead_hours": pa.array(np.full(n, lead_minutes // 60, dtype="int32")),
        "valid_time": pa.array(np.full(n, valid), pa.timestamp("us")),
        "member": pa.array(flat(member)),
        "lat": pa.array(flat(lat2d)),
        "lon": pa.array(flat(lon2d)),
    }
    for slug in slugs:
        name = column_name(slug)
        if slug in grids:
            grid, _, _ = grids[slug]
            # NaN in the grid stays NaN. NULL means the diagnostic has no file at this lead.
            columns[name] = pa.array(flat(grid))
        else:
            columns[name] = pa.nulls(n, pa.float32())
    return pa.table([columns[f.name] for f in schema], schema=schema)


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
    """Append one Parquet data file per lead. `leads` selects lead minutes; reruns skip finished leads.
    `rows` counts only rows appended by this run, never filtered leads. An append is final, so verify first."""
    started = time.perf_counter()
    table = ensure_table(catalog, slugs)
    progress = Progress(progress_store, progress_method(), cycle)
    wanted = set(slugs)
    by_lead = files_by_lead([d for d in surface_only(list_cycle(store, cycle)) if d.slug in wanted])
    if leads is not None:
        missing = sorted(set(leads) - set(by_lead))
        if missing:
            raise ValueError(f"no files for lead minutes {missing} in cycle {cycle}")
    # Units already appended by a run that died before it could mark them.
    seen = {
        s.summary.get("wxtco.unit")
        for s in table.metadata.snapshots
        if s.summary and s.summary.get("wxtco.cycle") == cycle
    }
    done = skipped = rows = 0
    for lead_minutes, files in by_lead.items():
        if leads is not None and lead_minutes not in leads:
            continue
        unit = f"lead={lead_minutes}"
        if progress.done(unit):
            skipped += 1
            continue
        if unit in seen:
            # The append landed but the mark was lost. Catch the manifest up; do not append twice.
            progress.mark(unit)
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
