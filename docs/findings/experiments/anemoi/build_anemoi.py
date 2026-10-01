"""Build the unsharded Anemoi-style experiment repo from the native dl repo: 4 cycles, 8 leads, 4 variables."""
import os
import time

import numpy as np
import xarray as xr
import zarr
from zarr.codecs import BloscCodec
from zarr.codecs.numcodecs import PCodec

from wxtco.config import Settings
from wxtco.ingest.native import REPO_PREFIX, open_native_repo
from wxtco.ingest.native_layout import LEAD_ENCODING, PCODEC_LEVEL, Q3_VARIABLES, TIME_ENCODING, ZARR_KWARGS
from wxtco.queries.native_backend import open_native_backend
from wxtco.source import cycle_time

CYCLES = ["2026/09/15/T0000Z", "2026/09/15/T0600Z", "2026/09/15/T1200Z", "2026/09/15/T1800Z"]
LEADS_H = [0, 6, 12, 24, 48, 72, 96, 120]
CODEC = os.environ.get("WXTCO_ANEMOI_CODEC", "pcodec")  # pcodec or lz4
NAME = f"{REPO_PREFIX}-anemoi" + ("" if CODEC == "pcodec" else f"-{CODEC}")
zarr.config.set({"async.concurrency": 64})

s = Settings.from_env()
src_repo = open_native_repo(s, "dl", "arraylake")
ds = xr.open_zarr(src_repo.readonly_session("main").store, chunks=None, **ZARR_KWARGS)
if "valid_time" in ds.data_vars:
    ds = ds.set_coords("valid_time")
names = [str(v) for v in ds["variable"].values[:4]]
assert names == list(Q3_VARIABLES), names
print("source data", ds["data"].shape, ds["data"].encoding.get("chunks"), ds["data"].encoding.get("shards"))

inits = np.array([np.datetime64(cycle_time(c).replace(tzinfo=None), "ns") for c in CYCLES])
ipos = ds.indexes["init"].get_indexer(inits)
assert (ipos >= 0).all() and list(ipos) == list(range(ipos[0], ipos[0] + len(ipos))), ipos
lpos = ds.indexes["lead"].get_indexer(np.array(LEADS_H, "timedelta64[h]").astype("timedelta64[ns]"))
assert (lpos >= 0).all(), lpos
# Slices on init and variable keep lead the only array-indexed axis (zarr coordinate-indexing fallback).
sub = ds[["data"]].isel(init=slice(int(ipos[0]), int(ipos[0]) + len(ipos)), variable=slice(0, 4)).isel(lead=list(lpos))

dst = open_native_repo(s, "dl", "arraylake", name=NAME)
session = dst.writable_session("main")
store = session.store
enc = {
    "data": {
        "chunks": (1, 1, 4, 18, 960, 1280),  # one chunk per sample: all variables, members and grid
        "shards": None,
        **(
            {"serializer": PCodec(level=PCODEC_LEVEL), "compressors": None}
            if CODEC == "pcodec"
            # Anemoi default family: Blosc, here LZ4 with byte shuffle for the fastest decode.
            else {"compressors": BloscCodec(cname="lz4", clevel=5, shuffle="shuffle", typesize=4)}
        ),
        "filters": None,
        "fill_value": float("nan"),
        "_FillValue": None,
    },
    # Fixed CF units, so an appended 06Z init does not reuse units picked from the first init alone.
    "init": dict(TIME_ENCODING),
    "valid_time": dict(TIME_ENCODING),
    "lead": dict(LEAD_ENCODING),
}
t_read = t_write = 0.0
for k in range(len(CYCLES)):
    t0 = time.perf_counter()
    part = sub.isel(init=slice(k, k + 1)).load()
    t1 = time.perf_counter()
    for v in part.variables.values():
        v.encoding = {}
    if k == 0:
        part.to_zarr(store, mode="w-", encoding=enc, **ZARR_KWARGS)
    else:
        # Append only what grows along init; the fixed coordinates are already stored.
        part = part.drop_vars([c for c in part.coords if "init" not in part[c].dims])
        part.to_zarr(store, append_dim="init", **ZARR_KWARGS)
    t2 = time.perf_counter()
    t_read += t1 - t0
    t_write += t2 - t1
    print(f"cycle {CYCLES[k]}: read {t1 - t0:.1f}s write {t2 - t1:.1f}s", flush=True)
snap = session.commit(f"Anemoi-style experiment ({CODEC}): 4 cycles x 8 leads x 4 variables, one unsharded chunk per sample")
print(f"committed {snap}; read {t_read:.1f}s write {t_write:.1f}s total")

arr = zarr.open_array(dst.readonly_session("main").store, path="data", mode="r")
print("anemoi data", arr.shape, "chunks", arr.chunks, "shards", arr.shards, "serializer", arr.metadata.codecs)

# Same decoded values as the dl repo for one batch.
a = open_native_backend(dst, "dl", NAME).batch_fields(names, CYCLES[:1], LEADS_H)
b = open_native_backend(src_repo, "dl", "native_dl").batch_fields(names, CYCLES[:1], LEADS_H)
print("verify equal:", a.shape, np.array_equal(a, b, equal_nan=True))
