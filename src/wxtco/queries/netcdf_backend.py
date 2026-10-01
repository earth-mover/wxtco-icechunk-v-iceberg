"""Query the original NetCDF files, with no ETL by construction.

Two ways to reach the files: download them per query (`DownloadBackend`), or read them through a
read-only FUSE mount of the bucket (`FuseBackend`). Both list the cycle and read the files again on
every call, because that is the cost the no-ETL method really pays.

`DownloadBackend` has two transfer clients, chosen by `transfer`:
- `obstore`: the Rust object-store client through its async API, one `asyncio` task per object,
  bounded by a semaphore. One GET per object.
- `crt`: boto3's S3 Transfer Manager on the AWS Common Runtime (the client Mountpoint and the AWS
  CLI use). It splits objects above `multipart_threshold` into ranged GETs, keeps many in flight
  and paces to the instance's target throughput. Needs an S3 bucket and region, not an ObjectStore.
`s5cmd cp` and `aws s3 cp --recursive` with a high `max_concurrent_requests` are the CLI equivalents.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
from collections.abc import Callable, Coroutine, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import numpy as np
import obstore as obs
import xarray as xr
from obstore.store import ObjectStore

from wxtco.ingest.netcdf import data_variable
from wxtco.queries.base import Box, Point
from wxtco.queries.tensor_backend import RENAME
from wxtco.source import Diagnostic, SourceFile, list_cycle, surface_only

# Coordinates kept on a query result. Anything else (height, pressure) would collide between vars.
# `time` keeps the file name, as the tensor backend does; only `point_series` renames it.
KEEP_COORDS = ("member", "lead", "lat", "lon", "time", "init")

# Narrows one file's lazy DataArray before it is read.
Select = Callable[[xr.DataArray], xr.DataArray]


def lead_in_hours(lead: xr.DataArray) -> xr.DataArray:
    """Give `forecast_period` as whole hours. A lead that is not a whole hour is an error."""
    # Without CF units xarray leaves forecast_period as seconds; with them it is a timedelta.
    per_hour: Any = np.timedelta64(1, "h") if np.issubdtype(lead.dtype, np.timedelta64) else 3600
    hours = lead / per_hour
    values = np.asarray(hours.values, dtype="float64")
    # Truncating here would give two files the same lead index and a silently wrong concat.
    if not np.all(np.isfinite(values)) or np.any(values != np.floor(values)):
        raise ValueError(f"lead {np.asarray(values).tolist()} h is not a whole number of hours")
    return hours.astype(int)


def point_select(point: Point) -> Select:
    """Snap lat and lon independently to the nearest cell, as the tensor backend does."""
    return lambda da: da.sel(lat=point.lat, lon=point.lon, method="nearest")


def box_select(box: Box) -> Select:
    """Take the inclusive window. The slice direction must follow the stored latitude order."""

    def select(da: xr.DataArray) -> xr.DataArray:
        lat = da["lat"]
        lat_slice = slice(box.lat_min, box.lat_max) if lat[0] < lat[-1] else slice(box.lat_max, box.lat_min)
        return da.sel(lat=lat_slice, lon=slice(box.lon_min, box.lon_max))

    return select


class NetcdfFilesBackend:
    """Base for backends that read Met Office NetCDF files directly. Subclasses only fetch."""

    name = "netcdf"

    def __init__(self, store: ObjectStore) -> None:
        self.store = store
        # LIST seconds of the last query; `list_seconds()` reads and resets it.
        self.last_list_seconds = 0.0

    # --- fetching, per subclass -------------------------------------------------

    def _materialize(self, files: Sequence[SourceFile]) -> dict[str, Path]:
        """Give a local readable path per object key."""
        raise NotImplementedError

    def _release(self) -> None:
        """Drop anything the last `_materialize` made. The default keeps nothing."""

    def bytes_read(self) -> int | None:
        """Give the bytes read by the last query, or None if the backend does not count them."""
        return None

    def list_seconds(self) -> float:
        """Give the LIST seconds since the last read, then reset. Not a bench CSV column."""
        out = self.last_list_seconds
        self.last_list_seconds = 0.0
        return out

    @contextmanager
    def _local(self, files: Sequence[SourceFile]) -> Iterator[dict[str, Path]]:
        """Materialize `files` for the body and release them after it, success or not.
        The fetch is inside the try, so a partial download does not leak its temp dir."""
        try:
            yield self._materialize(_unique(files))
        finally:
            self._release()

    # --- listing ----------------------------------------------------------------

    def _diagnostics(self, cycle: str) -> dict[str, Diagnostic]:
        """List the surface diagnostics of one cycle. No caching: every query pays the LIST."""
        t0 = time.perf_counter()
        listing = surface_only(list_cycle(self.store, cycle))
        self.last_list_seconds += time.perf_counter() - t0
        diags = {d.slug: d for d in listing}
        if not diags:
            raise ValueError(f"cycle {cycle} has no surface files under the store root")
        return diags

    def _files_of(self, diags: dict[str, Diagnostic], var: str, cycle: str) -> tuple[SourceFile, ...]:
        """Give every file of diagnostic `var`."""
        if var not in diags:
            raise ValueError(f"variable {var!r} not in cycle {cycle}; available: {sorted(diags)}")
        return diags[var].files

    @staticmethod
    def _file_at_lead(diag: Diagnostic, cycle: str, lead_hours: int) -> SourceFile:
        """Give the one file of `diag` at `lead_hours`."""
        want = lead_hours * 60
        for f in diag.files:
            if f.lead_minutes == want:
                return f
        leads = sorted(f.lead_minutes // 60 for f in diag.files)
        raise ValueError(f"lead {lead_hours} h not in cycle {cycle} for {diag.slug}; available: {leads}")

    # --- reading ----------------------------------------------------------------

    @staticmethod
    def _open(path: Path, name: str, select: Select | None = None) -> xr.DataArray:
        """Open one file lazily, apply `select`, and read only that. Dims get the study names."""
        with xr.open_dataset(path, engine="h5netcdf") as ds:
            da = ds[data_variable(ds)]
            da = da.rename({k: v for k, v in RENAME.items() if k in da.dims or k in da.coords})
            da = da.assign_coords(lead=lead_in_hours(da["lead"])).rename(name)
            # `.load()` after the selection: the rest of the (member, lat, lon) grid is never read.
            da = (select(da) if select is not None else da).load()
        return da.drop_vars([c for c in da.coords if c not in KEEP_COORDS])

    def _stack(
        self,
        files: Sequence[SourceFile],
        paths: dict[str, Path],
        name: str,
        select: Select | None = None,
    ) -> xr.DataArray:
        """Concatenate one diagnostic's per-file selections along `lead`, shortest lead first."""
        ordered = sorted(files, key=lambda f: f.lead_minutes)
        # `coords="different"` promotes the per-file time onto `lead`; it is stated because the
        # xarray default changes to "minimal", which would drop it.
        return xr.concat(
            [self._open(paths[f.key], name, select) for f in ordered],
            dim="lead",
            coords="different",
            compat="equals",
            join="exact",
        )

    # --- queries ----------------------------------------------------------------

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        """Give one variable at the nearest cell, dims (member, lead), with a `valid_time` coord."""
        files = self._files_of(self._diagnostics(cycle), var, cycle)
        with self._local(files) as paths:
            da = self._stack(files, paths, var, point_select(point))
        out = da.transpose("member", "lead").astype("float32")
        if "time" not in out.coords:
            raise ValueError(f"{var} files have no time coordinate; cannot build valid_time")
        out = out.rename({"time": "valid_time"})
        return out.drop_vars([c for c in out.coords if c not in ("member", "lead", "valid_time")])

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        """Give `vars` inside `box`, dims (member, lead, lat, lon), on the leads they share."""
        if not vars:
            raise ValueError("box_fields needs at least one variable")
        diags = self._diagnostics(cycle)
        per_var = {v: self._files_of(diags, v, cycle) for v in vars}
        select = box_select(box)
        with self._local([f for files in per_var.values() for f in files]) as paths:
            arrays = [self._stack(files, paths, v, select) for v, files in per_var.items()]
        # Lead schedules differ per diagnostic (an accumulation has no lead 0), so the per-lead
        # time coord would not merge. Keep the shared leads only.
        if len(arrays) > 1:
            arrays = list(xr.align(*arrays, join="inner"))
        if arrays[0].sizes.get("lead", 0) == 0:
            raise ValueError(f"{', '.join(vars)} share no lead in cycle {cycle}")
        ds = xr.Dataset({str(da.name): da for da in arrays})
        return ds.transpose("member", "lead", "lat", "lon").astype("float32")

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        """Give one lead as float32 of shape (len(vars), member, ny, nx)."""
        diags = self._diagnostics(cycle)
        wanted = []
        for v in vars:
            self._files_of(diags, v, cycle)
            wanted.append(self._file_at_lead(diags[v], cycle, lead_hours))
        with self._local(wanted) as paths:
            # No selection here: this query wants the whole grid.
            arrays = [
                self._open(paths[f.key], v).transpose("member", "lat", "lon").values.astype("float32")
                for v, f in zip(vars, wanted, strict=True)
            ]
        return np.stack(arrays)

    def batch_fields(self, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]) -> np.ndarray:
        """Give all samples as float32 (cycle, lead, var, member, ny, nx).

        One LIST per cycle and one `_materialize` for every file of the batch, so the download
        backends fetch the whole batch in one parallel transfer.
        """
        if not cycles or not leads:
            raise ValueError("no (cycle, lead) pairs")
        wanted: dict[tuple[int, int, int], SourceFile] = {}
        for ci, cycle in enumerate(cycles):
            diags = self._diagnostics(cycle)
            for v in vars:
                self._files_of(diags, v, cycle)
            for li, lead in enumerate(leads):
                for vi, v in enumerate(vars):
                    wanted[ci, li, vi] = self._file_at_lead(diags[v], cycle, int(lead))
        out: np.ndarray | None = None
        with self._local(list(wanted.values())) as paths:
            for (ci, li, vi), f in wanted.items():
                arr = self._open(paths[f.key], vars[vi]).transpose("member", "lat", "lon").values
                if out is None:
                    out = np.empty((len(cycles), len(leads), len(vars), *arr.shape), dtype="float32")
                out[ci, li, vi] = arr
        assert out is not None
        return out


def _unique(files: Sequence[SourceFile]) -> list[SourceFile]:
    """Drop repeated keys, order kept."""
    seen: dict[str, SourceFile] = {}
    for f in files:
        seen.setdefault(f.key, f)
    return list(seen.values())


def _run_async(coro: Coroutine[Any, Any, int]) -> int:
    """Run `coro` to completion. Inside a running loop, use a worker thread with its own loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


class DownloadBackend(NetcdfFilesBackend):
    """Download every needed object per query into a fresh temp dir, then read it from disk.

    `transfer="crt"` needs `bucket` and `region`; `prefix` is the key prefix inside the bucket that
    the store's keys are relative to (the copy store is rooted at `s3://bucket/netcdf/`).
    """

    name = "download"

    def __init__(
        self,
        store: ObjectStore,
        concurrency: int = 64,
        transfer: str = "obstore",
        bucket: str | None = None,
        prefix: str = "",
        region: str | None = None,
    ) -> None:
        super().__init__(store)
        if transfer not in ("obstore", "crt"):
            raise ValueError(f"transfer={transfer!r}; expected obstore or crt")
        if transfer == "crt" and not bucket:
            raise ValueError("transfer='crt' needs an S3 bucket")
        self.concurrency = concurrency
        self.transfer = transfer
        self.bucket, self.prefix, self.region = bucket, prefix.strip("/"), region
        self.name = "download" if transfer == "obstore" else "download_crt"
        self._dir: Path | None = None
        self._bytes = 0
        self._manager = None

    def _materialize(self, files: Sequence[SourceFile]) -> dict[str, Path]:
        """Fetch all objects in parallel into a new temp dir. Keys are flattened into file names."""
        self._dir = Path(tempfile.mkdtemp(prefix="wxtco-download-"))
        paths = {f.key: self._dir / f.key.replace("/", "_") for f in files}
        # One query can materialize many times (q2 two cycles, q3 one per sample), so accumulate.
        if self.transfer == "crt":
            self._bytes += self._fetch_all_crt(files, paths)
        else:
            self._bytes += _run_async(self._fetch_all(files, paths))
        return paths

    def _crt_manager(self):
        """Give the CRT transfer manager, made once. The client is connection state, not a data cache."""
        if self._manager is None:
            import boto3
            from boto3.s3.transfer import TransferConfig, create_transfer_manager

            client = boto3.client("s3", region_name=self.region)
            config = TransferConfig(max_concurrency=self.concurrency, preferred_transfer_client="crt")
            self._manager = create_transfer_manager(client, config)
        return self._manager

    def _fetch_all_crt(self, files: Sequence[SourceFile], paths: dict[str, Path]) -> int:
        """Submit every download to the CRT transfer manager, then wait. Give the bytes written."""
        manager = self._crt_manager()
        full = f"{self.prefix}/" if self.prefix else ""
        futures = [manager.download(self.bucket, f"{full}{f.key}", str(paths[f.key])) for f in files]
        for fut in futures:
            fut.result()
        return sum(paths[f.key].stat().st_size for f in files)

    async def _fetch_all(self, files: Sequence[SourceFile], paths: dict[str, Path]) -> int:
        """GET every key with at most `concurrency` requests in flight. Give the bytes written."""
        sem = asyncio.Semaphore(self.concurrency)

        async def one(f: SourceFile) -> int:
            async with sem:
                payload = await (await obs.get_async(self.store, f.key)).bytes_async()
            # `write_bytes` takes the obstore buffer as is; no copy. The write goes off the loop thread.
            await asyncio.to_thread(paths[f.key].write_bytes, payload)
            return len(payload)

        return sum(await asyncio.gather(*(one(f) for f in files)))

    def _release(self) -> None:
        """Remove the temp dir. Nothing is cached between queries."""
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)
            self._dir = None

    def bytes_read(self) -> int | None:
        """Give the bytes downloaded since the last read, then reset. Bench reads once per timed run."""
        out = self._bytes
        self._bytes = 0
        return out


class FuseBackend(NetcdfFilesBackend):
    """Read the files through a read-only FUSE mount. Mounting the bucket is infrastructure, not this."""

    name = "fuse"

    def __init__(self, store: ObjectStore, mount_root: Path) -> None:
        super().__init__(store)
        self.mount_root = Path(mount_root)
        self._paths: list[Path] = []

    def _materialize(self, files: Sequence[SourceFile]) -> dict[str, Path]:
        """Map each key onto the mount. The mount decides what is fetched and when.
        No existence check: under a minimal metadata TTL each stat is a HEAD plus a LIST."""
        self._paths = [self.mount_root / f.key for f in files]
        return dict(zip((f.key for f in files), self._paths))

    def _release(self) -> None:
        """Evict the files from the page cache, so the next query reads S3 again, not RAM."""
        for path in self._paths:
            try:
                fd = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
        self._paths = []

    def bytes_read(self) -> int | None:
        """Give None. The kernel, not this process, does the reads."""
        return None
