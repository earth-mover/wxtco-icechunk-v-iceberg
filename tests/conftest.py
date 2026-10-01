import os
from pathlib import Path

import pytest

# The suite tests the study layout. Scratch-table overrides in the shell must not leak into it.
# This runs before any wxtco module binds its constants.
for _v in ("WXTCO_TABLE_ID", "WXTCO_TABLE_SORT", "WXTCO_SLUGS"):
    os.environ.pop(_v, None)

from wxtco.fixture import build_cycle

CYCLE = "2026/09/16/T0000Z"


@pytest.fixture
def fixture_root(tmp_path: Path) -> tuple[Path, str]:
    """One synthetic cycle on disk. Returns (root, cycle)."""
    build_cycle(tmp_path, CYCLE)
    return tmp_path, CYCLE
