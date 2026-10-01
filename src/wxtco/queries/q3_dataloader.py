"""Q3: ML dataloader pattern. All members and several variables per (cycle, lead) sample.

`q3` reads one sample per `lead_fields` call, in a seeded random sample order.
`q3_stream` is the streaming model: blocks of consecutive cycles x leads, one `batch_fields`
call per block, a seeded random block order, and a seeded random sample order in each block.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

import numpy as np

from wxtco.queries.base import Backend


def sample_order(n_cycles: int, n_leads: int, seed: int = 0) -> Iterator[tuple[int, int]]:
    """Give (cycle index, lead index) for each pair in a seeded random order. `q3` uses it."""
    rng = np.random.default_rng(seed)
    for i in rng.permutation(n_cycles * n_leads):
        ci, li = divmod(int(i), n_leads)
        yield ci, li


def iter_batches(
    backend: Backend,
    vars: Sequence[str],
    cycles: Sequence[str],
    leads: Sequence[int],
    seed: int = 0,
) -> Iterator[tuple[str, int, np.ndarray]]:
    """Give (cycle, lead_hours, array) for each (cycle, lead) pair in a seeded random order."""
    for ci, li in sample_order(len(cycles), len(leads), seed):
        cycle, lead = cycles[ci], leads[li]
        yield cycle, lead, backend.lead_fields(vars, cycle, lead)


@dataclass(frozen=True)
class Q3Result:
    """Dataloader throughput: sample count, bytes, wall seconds, and the two rates.

    `bytes` counts decoded array bytes, not wire bytes, thus `gbps` is decode throughput.
    """

    samples: int
    bytes: int
    seconds: float
    samples_per_s: float
    gbps: float


def q3(
    backend: Backend,
    vars: Sequence[str],
    cycles: Sequence[str],
    leads: Sequence[int],
    seed: int = 0,
) -> Q3Result:
    """Read all (cycle, lead) samples in random order and give the throughput."""
    if not cycles or not leads:
        raise ValueError("no (cycle, lead) pairs")
    t0 = time.perf_counter()
    n = nbytes = 0
    for _, _, arr in iter_batches(backend, vars, cycles, leads, seed):
        n += 1
        nbytes += arr.nbytes
    return _result(n, nbytes, time.perf_counter() - t0)


@dataclass(frozen=True)
class Q3StreamResult:
    """Streaming throughput. `bytes` counts decoded array bytes, thus the rates are decode throughput.

    `steady_*` rates exclude the first batch (cold start). With only one batch they use the totals,
    and `steady_seconds` is about zero.
    """

    samples: int
    batches: int
    batch: int
    bytes: int
    seconds: float
    first_batch_seconds: float
    steady_seconds: float
    steady_samples_per_s: float
    steady_gbps: float
    samples_per_s: float
    gbps: float


def batch_geometry(n_cycles: int, n_leads: int, batch: int) -> tuple[int, int]:
    """Give (nc, nl): a batch is nc consecutive cycles x nl consecutive leads, nc * nl == batch."""
    if batch < 1:
        raise ValueError(f"batch must be positive, got {batch}")
    nl = min(batch, n_leads)
    nc = batch // nl
    if n_leads % nl:
        raise ValueError(
            f"len(leads) = {n_leads} must be a multiple of batch = {batch} when batch < len(leads)"
        )
    if batch % nl:
        raise ValueError(f"batch = {batch} must be a multiple of len(leads) = {n_leads} when larger")
    if n_cycles % nc:
        raise ValueError(f"len(cycles) = {n_cycles} must be a multiple of the cycle block {nc}")
    return nc, nl


def q3_stream(
    backend: Backend,
    vars: Sequence[str],
    cycles: Sequence[str],
    leads: Sequence[int],
    batch: int,
    samples: int | None = None,
    seed: int = 0,
) -> Q3StreamResult:
    """Stream shuffled batches until `samples` samples (default all, capped at all) are visited.

    Each batch is one `backend.batch_fields` call on consecutive positions of `cycles` and `leads`.
    Pass them in stored order, thus a batch is a contiguous read.
    """
    if not cycles or not leads:
        raise ValueError("no (cycle, lead) pairs")
    nc, nl = batch_geometry(len(cycles), len(leads), batch)
    total = len(cycles) * len(leads)
    want = total if samples is None else samples
    if want < 1 or want % batch:
        raise ValueError(f"samples = {want} must be a positive multiple of batch = {batch}")
    n_batches = min(want, total) // batch
    blocks = [(cb, lb) for cb in range(len(cycles) // nc) for lb in range(len(leads) // nl)]
    rng = np.random.default_rng(seed)
    order = rng.permutation(len(blocks))[:n_batches]
    n = nbytes = 0
    first_seconds = 0.0
    first_n = first_bytes = 0
    t0 = time.perf_counter()
    for k, bi in enumerate(order):
        tb = time.perf_counter()
        cb, lb = blocks[int(bi)]
        arr = backend.batch_fields(vars, cycles[cb * nc : (cb + 1) * nc], leads[lb * nl : (lb + 1) * nl])
        for i in rng.permutation(batch):
            ci, li = divmod(int(i), nl)
            n += 1
            nbytes += arr[ci, li].nbytes
        if k == 0:
            first_seconds, first_n, first_bytes = time.perf_counter() - tb, n, nbytes
    dt = time.perf_counter() - t0
    if n_batches > 1:
        steady_dt, steady_n, steady_bytes = dt - first_seconds, n - first_n, nbytes - first_bytes
    else:
        steady_dt, steady_n, steady_bytes = dt, n, nbytes
    d, sd = max(dt, 1e-9), max(steady_dt, 1e-9)  # Keep the rates finite on a fast clock.
    return Q3StreamResult(
        samples=n,
        batches=n_batches,
        batch=batch,
        bytes=nbytes,
        seconds=dt,
        first_batch_seconds=first_seconds,
        steady_seconds=dt - first_seconds,
        steady_samples_per_s=steady_n / sd,
        steady_gbps=steady_bytes * 8 / sd / 1e9,
        samples_per_s=n / d,
        gbps=nbytes * 8 / d / 1e9,
    )


def _result(n: int, nbytes: int, dt: float) -> Q3Result:
    """Give the result with the two rates."""
    d = max(dt, 1e-9)  # Keep the rates finite on a fast clock.
    return Q3Result(n, nbytes, dt, n / d, nbytes * 8 / d / 1e9)
