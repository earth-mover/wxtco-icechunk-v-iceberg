"""Static copy of source cycles. The AWS CLI does the transfer; this module plans and verifies."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import obstore as obs
from obstore.store import ObjectStore

from wxtco.source import list_cycle, surface_only

LEVEL_GLOB = "*_on_*_levels.nc"


def copy_commands(
    source_url: str, copy_url: str, cycles: Sequence[str], source_region: str, copy_region: str
) -> list[str]:
    """One `aws s3 cp` per cycle. Level files are excluded; everything else is copied as-is.

    Return shell strings; run with `shell=True` or paste into a terminal.
    """
    src, dst = source_url.rstrip("/"), copy_url.rstrip("/")
    return [
        f"aws s3 cp --recursive --only-show-errors --copy-props none --source-region {source_region} --region {copy_region} "
        f"--exclude '{LEVEL_GLOB}' {src}/{c}/ {dst}/{c}/"
        for c in cycles
    ]


@dataclass(frozen=True)
class CopyReport:
    cycle: str
    expected: int
    present: int
    missing: tuple[str, ...]
    bytes: int


def verify_copy(src: ObjectStore, dst: ObjectStore, cycle: str) -> CopyReport:
    """Compare surface files in the source with the copy. Byte count is the copy's, wanted keys only.

    Only forecast files that match the key pattern are verified, thus `missing == ()` does not prove
    that non-forecast objects were copied.
    """
    cycle = cycle.rstrip("/")
    want = {f.key for d in surface_only(list_cycle(src, cycle)) for f in d.files}
    have = {item["path"]: item["size"] for page in obs.list(dst, prefix=f"{cycle}/") for item in page}
    missing = tuple(sorted(want - set(have)))
    copied = sum(sz for k, sz in have.items() if k in want)
    return CopyReport(cycle, len(want), len(want) - len(missing), missing, copied)
