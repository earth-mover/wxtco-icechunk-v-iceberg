import numpy as np
import pytest

from wxtco.ingest.hilbert import cell_order, hilbert_index, row_permutation


@pytest.mark.parametrize("order", range(1, 7))
def test_hilbert_index_is_a_continuous_permutation(order):
    n = 1 << order
    iy, ix = np.meshgrid(np.arange(n), np.arange(n), indexing="ij")
    d = hilbert_index(ix.ravel(), iy.ravel(), order)
    assert sorted(d.tolist()) == list(range(n * n))
    # Consecutive curve positions are grid neighbours.
    o = np.argsort(d)
    steps = np.abs(np.diff(ix.ravel()[o])) + np.abs(np.diff(iy.ravel()[o]))
    assert steps.max() == 1


def test_cell_order_is_local_on_a_non_square_grid():
    ny, nx = 6, 10
    cells = cell_order(ny, nx)
    assert sorted(cells.tolist()) == list(range(ny * nx))
    iy, ix = np.divmod(cells, nx)
    steps = np.abs(np.diff(ix)) + np.abs(np.diff(iy))
    # Cells the curve skips (outside the grid) give a few longer steps; most are unit steps.
    assert steps.mean() < 1.5
    assert not cells.flags.writeable


def test_row_permutation_covers_every_row_member_innermost():
    ny, nx, members = 6, 10, 3
    perm = row_permutation(members, ny, nx)
    assert sorted(perm.tolist()) == list(range(members * ny * nx))
    assert (perm[:members] % (ny * nx) == perm[0]).all()
    assert (perm[:members] // (ny * nx) == np.arange(members)).all()
