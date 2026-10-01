"""The study window: which 28 cycles the whole study uses."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from wxtco.source import cycle_time

# Repo root in a source checkout. Set WXTCO_STUDY_CYCLES when the package is installed as a wheel.
WINDOW_FILE = Path(
    os.environ.get("WXTCO_STUDY_CYCLES", Path(__file__).resolve().parents[2] / "study_cycles.txt")
)


def pick_window(cycles: Sequence[str], days: int = 7, margin_days: int = 3) -> list[str]:
    """Newest `4*days` cycles, provided the window starts `margin_days` after the oldest available."""
    n = 4 * days
    ordered = sorted(cycles, key=cycle_time)
    if len(ordered) < n:
        raise ValueError(f"need {n} cycles, have {len(ordered)}")
    window = ordered[-n:]
    if (cycle_time(window[0]) - cycle_time(ordered[0])).days < margin_days:
        raise ValueError("window starts too close to the deletion edge")
    return window


def study_cycles() -> list[str]:
    """Cycles pinned in study_cycles.txt at repo root. Empty until the copy is made."""
    if not WINDOW_FILE.exists():
        return []
    return [line.strip() for line in WINDOW_FILE.read_text().splitlines() if line.strip()]
