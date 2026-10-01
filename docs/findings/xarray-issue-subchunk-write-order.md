# Issue record: `subchunk_write_order` is unreachable from `Dataset.to_zarr`

Ready to submit to `pydata/xarray`. Not yet filed.

**Title:** `to_zarr` cannot set or preserve a sharded array's `subchunk_write_order`

## Versions

| package | version |
|---|---|
| xarray | 2026.7.0 |
| zarr | 3.4.0 |
| icechunk | 2.2.2 |
| numpy | 2.5.3 |

## Summary

`zarr.codecs.ShardingCodec` takes `subchunk_write_order` (`"morton"`, the default, or
`"lexicographic"`). It decides the byte order of the inner chunks inside a shard object. It is a
write-time property of the codec instance and zarr does **not** persist it in array metadata.

Two consequences for xarray:

1. `Dataset.to_zarr` has no `encoding` key for it. It can be set indirectly on the initial write by
   passing a configured codec as `encoding={"var": {"serializer": ShardingCodec(...)}}` instead of
   `encoding={"var": {"shards": ...}}`, but that only affects that one call.
2. A region write (`to_zarr(..., region=...)`) reopens the array from stored metadata. The stored
   metadata cannot carry the order, so the codec is rebuilt with the default and the shard is
   written in Morton order, silently and with no warning.

The only workaround we found rebuilds the codec and reaches into a private attribute:

```python
fixed = dataclasses.replace(arr.metadata.codecs[0], subchunk_write_order="lexicographic")
meta = dataclasses.replace(arr.metadata, codecs=(fixed,))
arr = zarr.Array(dataclasses.replace(arr._async_array, metadata=meta))  # private
arr[region] = block  # bypasses xarray entirely
```

## Reproducible example

Create one sharded array in lexicographic order with zarr, rewrite the same shard through
`to_zarr(region=...)`, then read the shard index and print the inner-chunk offsets in C order.

```python
import struct, tempfile
import numpy as np, xarray as xr, zarr
from zarr.codecs import ShardingCodec

store = zarr.storage.LocalStore(tempfile.mkdtemp())
shape, chunks, shards = (1, 8, 4, 4), (1, 1, 2, 2), (1, 8, 4, 4)
n = int(np.prod([s // c for s, c in zip(shards, chunks)]))  # 32 inner chunks

a = zarr.create_array(
    store, name="var", shape=shape, dtype="float32", chunks=shards,
    serializer=ShardingCodec(chunk_shape=chunks, codecs=[zarr.codecs.BytesCodec()],
                             subchunk_write_order="lexicographic"),
    compressors=None, fill_value=np.float32("nan"), dimension_names=("t", "v", "y", "x"),
)
a[:] = np.arange(np.prod(shape), dtype="float32").reshape(shape)

def offsets(key="var/c/0/0/0/0"):
    """Inner-chunk byte offsets, read from the shard index, in C order."""
    raw = zarr.core.sync.sync(store.get(key, zarr.core.buffer.default_buffer_prototype())).to_bytes()
    idx = raw[-(n * 16 + 4):-4]
    return [struct.unpack("<QQ", idx[i * 16:(i + 1) * 16])[0] for i in range(n)]

print(offsets(), offsets() == sorted(offsets()))

ds = xr.open_zarr(store, consolidated=False, chunks=None)
ds["var"][:] = np.arange(np.prod(shape), dtype="float32").reshape(shape) + 100
ds.drop_encoding().to_zarr(store, region={"t": slice(0, 1)}, consolidated=False)

print(offsets(), offsets() == sorted(offsets()))
```

Output:

```
after zarr wrote it with subchunk_write_order="lexicographic"
[0, 16, 32, 48, 64, 80, 96, 112, 128, ...]                    monotonic: True
after xarray to_zarr(region=...)
[0, 64, 32, 96, 16, 80, 48, 112, 128, 192, 160, 224, ...]     monotonic: False
```

The second list is Morton order. Nothing in the array metadata records which order is on disk.

## Expected behaviour

Either

- an `encoding` key, e.g. `encoding={"var": {"subchunk_write_order": "lexicographic"}}`, honoured on
  the initial write **and** on region and append writes; or
- a documented way to hand `to_zarr` a configured codec instance that region writes reuse instead of
  rebuilding the codec from stored metadata.

A warning when a region write silently changes the order of an existing shard would also help.

## Why it matters

We store an ensemble forecast archive as one shard per (init, lead) sample: dims
(init, lead, variable, member, lat, lon), 81 variables, shard ~7.2 GB of 1296 inner chunks of
~5.5 MB. A reader usually wants 2-4 variables out of the 81.

Under lexicographic order the variable index is the outermost within-shard index, so the 16 spatial
chunks of one variable are contiguous (~88 MB) and a 4-variable subset is one run of 64 chunks. A
range coalescer (ours merges ranges less than 1 MiB apart, up to a 16 MiB maximum range) folds that
run into a few sequential ranges. Under Morton order the same 4 variables are still contiguous
because 4 and the chunk counts are powers of two, but any subset that crosses a power-of-two
boundary fragments into single-chunk requests of ~1 MB each.

So the order is a real read-performance property of the stored object, and it is currently neither
selectable from xarray nor recoverable from the file.

## Note for zarr-python

zarr-python may also want to persist `subchunk_write_order` in the sharding codec configuration, or
expose it on the opened array. Today the value cannot be read back from a stored array, so no reader
or writer can tell which order a shard is in.
