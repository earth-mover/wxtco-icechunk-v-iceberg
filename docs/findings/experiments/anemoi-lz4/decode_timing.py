"""Time one sample-chunk decode, pcodec vs Blosc LZ4, on 1 thread and 8 threads."""
import time
from concurrent.futures import ThreadPoolExecutor

import numcodecs
import numpy as np
import xarray as xr
from numcodecs import Blosc

from wxtco.config import Settings
from wxtco.ingest.native import REPO_PREFIX, open_native_repo
from wxtco.ingest.native_layout import ZARR_KWARGS

s = Settings.from_env()
repo = open_native_repo(s, "dl", "arraylake", name=f"{REPO_PREFIX}-anemoi")
ds = xr.open_zarr(repo.readonly_session("main").store, chunks=None, **ZARR_KWARGS)
x = np.ascontiguousarray(ds["data"].isel(init=0, lead=3).values)  # one 354 MB sample chunk
print(f"sample {x.shape} {x.nbytes / 1e6:.0f} MB")
codecs = {
    "pcodec8": numcodecs.get_codec({"id": "pcodec", "level": 8}),
    "lz4-shuffle": Blosc(cname="lz4", clevel=5, shuffle=Blosc.SHUFFLE),
}
for name, c in codecs.items():
    enc = c.encode(x)
    t = []
    for _ in range(3):
        t0 = time.perf_counter(); c.decode(enc); t.append(time.perf_counter() - t0)
    one = min(t)
    with ThreadPoolExecutor(8) as ex:
        t0 = time.perf_counter(); list(ex.map(lambda _: c.decode(enc), range(8))); eight = time.perf_counter() - t0
    print(f"DECODE {name}: ratio {x.nbytes / len(enc):.2f}x, 1 chunk {one:.2f}s ({x.nbytes / one / 1e9:.2f} GB/s), "
          f"8 chunks on 8 threads {eight:.2f}s ({8 * x.nbytes / eight / 1e9:.2f} GB/s, speedup {8 * one / eight:.1f}x)", flush=True)
print("blosc threads", Blosc.get_nthreads() if hasattr(Blosc, "get_nthreads") else "n/a", "use_threads", Blosc.use_threads)
