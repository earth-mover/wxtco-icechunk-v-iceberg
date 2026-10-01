"""Time queries and append rows to a findings CSV."""

from __future__ import annotations

import csv
import json
import os
import subprocess
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wxtco.queries.base import Backend, Box, Point, timed
from wxtco.queries.q1_point import q1
from wxtco.queries.q2_regional import q2
from wxtco.queries.q3_dataloader import q3, q3_stream

QUERIES = ("q1", "q2", "q3", "q3_stream")
NET_DEV = Path("/proc/net/dev")


@dataclass(frozen=True)
class BenchRow:
    """One timed run. The field order is the CSV column order."""

    timestamp: str
    git_sha: str
    method: str
    query: str
    params: str
    run: int
    seconds: float
    bytes_read: int | None
    net_rx_bytes: int | None
    instance_type: str
    region: str
    # JSON object. For q3_stream it holds all Q3StreamResult fields; else "{}".
    detail: str


def git_sha() -> str:
    """Give the short commit of the working tree. `WXTCO_GIT_SHA` wins, for the EC2 tarball."""
    env = os.environ.get("WXTCO_GIT_SHA")
    if env:
        return env
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        )
    # A tarball checkout has no .git, and git may be absent.
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return proc.stdout.strip() or "unknown"


def net_rx_bytes() -> int | None:
    """Give the bytes received on all interfaces except `lo`, or None if there is no /proc/net/dev (macOS)."""
    try:
        lines = NET_DEV.read_text().splitlines()
    except OSError:
        return None
    total = 0
    # Two header lines, then `iface: rx_bytes rx_packets ...`.
    for line in lines[2:]:
        iface, sep, counters = line.partition(":")
        if sep and iface.strip() != "lo":
            total += int(counters.split()[0])
    return total


def _call(backend: Backend, query: str, p: Mapping[str, Any]) -> Any:
    """Run one query with the parameters the CLI passed as JSON."""
    if query == "q1":
        return q1(backend, Point(p["lat"], p["lon"]), p["cycle"])
    if query == "q2":
        return q2(backend, Box(p["lat_min"], p["lat_max"], p["lon_min"], p["lon_max"]), p["cycle"])
    if query == "q3":
        return q3(backend, p["vars"], p["cycles"], p["leads"])
    if query == "q3_stream":
        return q3_stream(backend, p["vars"], p["cycles"], p["leads"], p["batch"], p.get("samples"))
    raise ValueError(f"unknown query {query!r}; expected one of {', '.join(QUERIES)}")


def run_query(
    backend: Backend,
    query: str,
    params: Mapping[str, Any],
    runs: int,
    out_csv: Path,
    meta: Mapping[str, str],
) -> list[BenchRow]:
    """Time `query` `runs` times and append the rows to `out_csv`, with a header if the file is new."""
    header = [x.name for x in fields(BenchRow)]
    # Check before the timed runs: appending to an old-schema CSV would shift the columns.
    if out_csv.exists():
        with out_csv.open(newline="") as f:
            old = next(csv.reader(f), None)
        if old is not None and old != header:
            raise ValueError(f"{out_csv} has columns {old}; this version writes {header}. Use a new CSV.")
    rows: list[BenchRow] = []
    for i in range(runs):
        rx0 = net_rx_bytes()
        result, seconds = timed(_call, backend, query, params)
        rx1 = net_rx_bytes()
        rows.append(
            BenchRow(
                timestamp=datetime.now(UTC).isoformat(timespec="seconds"),
                git_sha=meta["git_sha"],
                method=backend.name,
                query=query,
                params=json.dumps(dict(params), sort_keys=True),
                run=i,
                seconds=round(seconds, 4),
                bytes_read=backend.bytes_read(),
                # Host-wide counter: other traffic in the run window is counted too.
                net_rx_bytes=rx1 - rx0 if rx0 is not None and rx1 is not None else None,
                instance_type=meta["instance_type"],
                region=meta["region"],
                detail=json.dumps(asdict(result)) if query == "q3_stream" else "{}",
            )
        )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    new = not out_csv.exists()
    with out_csv.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if new:
            w.writeheader()
        for r in rows:
            # csv writes None as an empty cell, which pandas reads back as NaN.
            w.writerow(asdict(r))
    return rows
