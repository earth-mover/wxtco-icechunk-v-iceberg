"""Backend protocol. Backends fetch; query modules compute."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import xarray as xr


@dataclass(frozen=True)
class Point:
    """One geographic point in degrees."""

    lat: float
    lon: float


@dataclass(frozen=True)
class Box:
    """An inclusive latitude and longitude window in degrees."""

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


class Backend(Protocol):
    """Data access for the three queries. Each method gives plain xarray or numpy."""

    name: str

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        """Give dims (member, lead) at the grid cell nearest `point`, with a `valid_time` coord."""
        ...

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        """Give dims (member, lead, lat, lon) for `vars` inside `box`."""
        ...

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        """Give one lead as float32 of shape (len(vars), member, ny, nx)."""
        ...

    def batch_fields(self, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]) -> np.ndarray:
        """Give all (cycle, lead) samples as float32 of shape (cycle, lead, var, member, ny, nx)."""
        ...

    def bytes_read(self) -> int | None:
        """Give the bytes read since the last query, or None if the backend does not count them."""
        ...


def stack_leads(
    backend: Backend, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]
) -> np.ndarray:
    """Build the `batch_fields` array with one `lead_fields` call per sample. Fallback for no bulk path."""
    return np.stack([np.stack([backend.lead_fields(vars, c, lead) for lead in leads]) for c in cycles])


def timed(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> tuple[Any, float]:
    """Call `fn` and give (result, wall seconds)."""
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - t0
