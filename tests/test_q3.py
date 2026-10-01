"""Q3 dataloader tests: deterministic order, byte count, batch geometry, and empty product."""

import numpy as np
import pytest

from wxtco.queries.base import stack_leads
from wxtco.queries.q3_dataloader import iter_batches, q3, q3_stream


class FakeBackend:
    """Backend stub. Gives a fixed (vars, member, ny, nx) float32 block."""

    name = "fake"

    def lead_fields(self, vars, cycle, lead_hours):
        """Give zeros of shape (len(vars), 2, 4, 4)."""
        return np.zeros((len(vars), 2, 4, 4), dtype="float32")

    def batch_fields(self, vars, cycles, leads):
        """Stack `lead_fields` per sample."""
        return stack_leads(self, vars, cycles, leads)

    def bytes_read(self):
        """This backend does not count bytes."""
        return


class RecordingBackend(FakeBackend):
    """Backend stub. Records each `batch_fields` call and fills samples with their flat (cycle, lead) id."""

    def __init__(self, cycles, leads):
        self.calls = []
        self.cycles, self.leads = list(cycles), list(leads)

    def batch_fields(self, vars, cycles, leads):
        """Give (nc, nl, len(vars), 2, 4, 4); each sample holds its flat id."""
        self.calls.append((list(cycles), list(leads)))
        arr = stack_leads(self, vars, cycles, leads)
        for ci, c in enumerate(cycles):
            for li, lead in enumerate(leads):
                arr[ci, li] = self.cycles.index(c) * len(self.leads) + self.leads.index(lead)
        return arr


def test_iter_batches_random_order_is_deterministic():
    b = FakeBackend()
    order1 = [(c, l) for c, l, _ in iter_batches(b, ["a"], ["c1", "c2"], [0, 1, 2], seed=1)]
    order2 = [(c, l) for c, l, _ in iter_batches(b, ["a"], ["c1", "c2"], [0, 1, 2], seed=1)]
    assert order1 == order2 and sorted(order1) == [
        ("c1", 0),
        ("c1", 1),
        ("c1", 2),
        ("c2", 0),
        ("c2", 1),
        ("c2", 2),
    ]


def test_q3_counts_bytes():
    r = q3(FakeBackend(), ["a", "b"], ["c1"], [0, 1])
    assert r.samples == 2
    assert r.bytes == 2 * (2 * 2 * 4 * 4 * 4)
    assert r.samples_per_s > 0


def test_q3_rejects_empty_product():
    with pytest.raises(ValueError, match="no \\(cycle, lead\\) pairs"):
        q3(FakeBackend(), ["a"], [], [0, 1])
    with pytest.raises(ValueError, match="no \\(cycle, lead\\) pairs"):
        q3(FakeBackend(), ["a"], ["c1"], [])


def test_stack_leads_shape():
    arr = stack_leads(FakeBackend(), ["a", "b", "c"], ["c1", "c2"], [0, 1, 2, 3])
    assert arr.shape == (2, 4, 3, 2, 4, 4) and arr.dtype == np.float32


CYCLES = ["c1", "c2", "c3", "c4"]
LEADS = [0, 1, 2]
SAMPLE_BYTES = 2 * 2 * 4 * 4 * 4


@pytest.mark.parametrize("batch", [1, len(LEADS), 2 * len(LEADS)])
def test_q3_stream_matches_q3(batch):
    loop = q3(FakeBackend(), ["a", "b"], CYCLES, LEADS)
    r = q3_stream(FakeBackend(), ["a", "b"], CYCLES, LEADS, batch)
    assert r.samples == loop.samples == len(CYCLES) * len(LEADS)
    assert r.bytes == loop.bytes == r.samples * SAMPLE_BYTES
    assert r.batch == batch and r.batches == r.samples // batch
    assert r.samples_per_s > 0 and r.steady_samples_per_s > 0 and r.gbps > 0
    assert r.steady_seconds == pytest.approx(r.seconds - r.first_batch_seconds)


def test_q3_stream_single_batch_uses_totals():
    r = q3_stream(FakeBackend(), ["a"], ["c1"], LEADS, len(LEADS))
    assert r.batches == 1
    assert (r.steady_samples_per_s, r.steady_gbps) == (r.samples_per_s, r.gbps)


def _blocks(calls):
    return [(tuple(c), tuple(l)) for c, l in calls]


def test_q3_stream_batch_order_is_seeded_permutation():
    # batch 6 over 3 leads: 2 cycles x 3 leads per batch, 2 batches.
    b1, b2, b3 = (RecordingBackend(CYCLES, LEADS) for _ in range(3))
    q3_stream(b1, ["a"], CYCLES, LEADS, 6, seed=5)
    q3_stream(b2, ["a"], CYCLES, LEADS, 6, seed=5)
    assert _blocks(b1.calls) == _blocks(b2.calls)
    q3_stream(b3, ["a"], CYCLES, LEADS, 1, seed=5)
    natural = [((c,), (lead,)) for c in CYCLES for lead in LEADS]
    got = _blocks(b3.calls)
    assert sorted(got) == sorted(natural) and len(got) == len(natural)
    idx = [natural.index(x) for x in got]
    assert idx == np.random.default_rng(5).permutation(len(natural)).tolist()
    assert _blocks(b1.calls) in (
        [(("c1", "c2"), (0, 1, 2)), (("c3", "c4"), (0, 1, 2))],
        [(("c3", "c4"), (0, 1, 2)), (("c1", "c2"), (0, 1, 2))],
    )


@pytest.mark.parametrize("batch", [1, 3, 6, 12])
def test_q3_stream_visits_each_sample_once(batch, monkeypatch):
    b = RecordingBackend(CYCLES, LEADS)
    seen = []
    real = b.batch_fields

    def spy(vars, cycles, leads):
        arr = real(vars, cycles, leads)
        # Consecutive cycle and lead positions per batch.
        ci = [CYCLES.index(c) for c in cycles]
        li = [LEADS.index(x) for x in leads]
        assert ci == list(range(ci[0], ci[0] + len(ci))) and li == list(range(li[0], li[0] + len(li)))
        assert arr.shape[:2] == (len(cycles), len(leads))
        seen.extend(int(arr[i, j].flat[0]) for i in range(len(cycles)) for j in range(len(leads)))
        return arr

    monkeypatch.setattr(b, "batch_fields", spy)
    r = q3_stream(b, ["a"], CYCLES, LEADS, batch, seed=2)
    assert sorted(seen) == list(range(len(CYCLES) * len(LEADS)))
    assert r.samples == len(seen)


def test_q3_stream_rejects_bad_geometry():
    with pytest.raises(ValueError, match="len\\(leads\\) = 3 must be a multiple of batch = 2"):
        q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 2)
    with pytest.raises(ValueError, match="batch = 4 must be a multiple of len\\(leads\\) = 3"):
        q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 4)
    with pytest.raises(ValueError, match="len\\(cycles\\) = 4 must be a multiple of the cycle block 3"):
        q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 9)
    with pytest.raises(ValueError, match="batch must be positive"):
        q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 0)


def test_q3_stream_rejects_empty_product():
    with pytest.raises(ValueError, match="no \\(cycle, lead\\) pairs"):
        q3_stream(FakeBackend(), ["a"], [], [0, 1], 1)
    with pytest.raises(ValueError, match="no \\(cycle, lead\\) pairs"):
        q3_stream(FakeBackend(), ["a"], ["c1"], [], 1)


def test_q3_stream_samples_caps_the_stream():
    b = RecordingBackend(CYCLES, LEADS)
    r = q3_stream(b, ["a"], CYCLES, LEADS, 3, samples=6)
    assert (r.samples, r.batches, r.bytes) == (6, 2, 6 * 2 * 4 * 4 * 4) and len(b.calls) == 2
    # Above the total: cap at all samples.
    r = q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 3, samples=300)
    assert (r.samples, r.batches) == (12, 4)
    for bad in (4, 0, -3):
        with pytest.raises(ValueError, match="positive multiple of batch = 3"):
            q3_stream(FakeBackend(), ["a"], CYCLES, LEADS, 3, samples=bad)
