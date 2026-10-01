"""Hilbert-curve row order for the regular lat/lon grid. Pure layout; queries never see it."""

from __future__ import annotations

from functools import lru_cache

import numpy as np


def hilbert_index(ix: np.ndarray, iy: np.ndarray, order: int) -> np.ndarray:
    """Give the distance along a Hilbert curve on a 2**order square for each (ix, iy)."""
    n = 1 << order
    x = np.asarray(ix, dtype=np.int64).copy()
    y = np.asarray(iy, dtype=np.int64).copy()
    if x.min() < 0 or y.min() < 0 or x.max() >= n or y.max() >= n:
        raise ValueError(f"indices exceed the {n}x{n} curve")
    d = np.zeros_like(x)
    s = n >> 1
    while s > 0:
        rx = (x & s) > 0
        ry = (y & s) > 0
        d += s * s * ((3 * rx.astype(np.int64)) ^ ry.astype(np.int64))
        # Rotate the quadrant so the curve stays continuous (Wikipedia xy2d).
        flip = ~ry & rx
        x = np.where(flip, n - 1 - x, x)
        y = np.where(flip, n - 1 - y, y)
        x, y = np.where(ry, x, y), np.where(ry, y, x)
        s >>= 1
    return d


@lru_cache(maxsize=4)
def cell_order(ny: int, nx: int) -> np.ndarray:
    """Give flat cell indices (iy * nx + ix) in Hilbert order. Cached; the grid is fixed for a study."""
    order = max(1, int(np.ceil(np.log2(max(ny, nx)))))
    iy, ix = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    out = np.argsort(hilbert_index(ix.ravel(), iy.ravel(), order), kind="stable")
    out.setflags(write=False)  # Shared through the cache.
    return out


def row_permutation(members: int, ny: int, nx: int) -> np.ndarray:
    """Give indices into the flat (member, ny, nx) grid so rows come out cell by cell, member innermost."""
    cells = cell_order(ny, nx)
    return (np.arange(members, dtype=np.int64)[None, :] * (ny * nx) + cells[:, None]).ravel()
