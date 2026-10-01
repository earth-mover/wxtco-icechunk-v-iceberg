"""Compare numpy tile/repeat vs xr.to_dataframe for flattening one lead. Real grid, 8 vars."""
import resource, sys, time
import numpy as np, pandas as pd, pyarrow as pa, xarray as xr

M, NY, NX, NV = 18, 960, 1280, int(sys.argv[2])
mode = sys.argv[1]
rng = np.random.default_rng(0)
lat = np.linspace(-89.9, 89.9, NY, dtype="float32"); lon = np.linspace(-179.9, 179.9, NX, dtype="float32")
grids = {f"v{i}": rng.random((M, NY, NX), dtype="float32") for i in range(NV)}
grids["v0"][0, 0, :10] = np.nan  # real NaN data values exist in some diagnostics
t0 = time.perf_counter()
if mode == "numpy":
    n = M * NY * NX
    cols = {
        "member": pa.array(np.repeat(np.arange(M, dtype="int32"), NY * NX)),
        "lat": pa.array(np.tile(np.repeat(lat, NX), M)),
        "lon": pa.array(np.tile(lon, M * NY)),
    }
    for k, g in grids.items():
        cols[k] = pa.array(g.reshape(-1))
    t = pa.table(cols)
elif mode == "meshgrid":
    mm, la, lo = np.meshgrid(np.arange(M, dtype="int32"), lat, lon, indexing="ij")
    cols = {"member": pa.array(mm.ravel()), "lat": pa.array(la.ravel()), "lon": pa.array(lo.ravel())}
    for k, g in grids.items():
        cols[k] = pa.array(g.reshape(-1))
    t = pa.table(cols)
elif mode == "to_dataframe":
    ds = xr.Dataset(
        {k: (("realization", "latitude", "longitude"), g) for k, g in grids.items()},
        coords={"realization": np.arange(M, dtype="int32"), "latitude": lat, "longitude": lon,
                "forecast_period": ((), np.int32(0)), "height": ((), np.float32(1.5))},
    )
    df = ds.to_dataframe(dim_order=["realization", "latitude", "longitude"]).reset_index()
    t = pa.Table.from_pandas(df, preserve_index=False)
dt = time.perf_counter() - t0
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
print(f"{mode:13s} vars={NV:2d} rows={t.num_rows:,} {dt:6.1f}s maxrss={rss:5.1f}GB cols={t.column_names[:6]} "
      f"dtypes={[str(t.schema.field(c).type) for c in t.column_names[:4]]} v0_nulls={t['v0'].null_count}")
