"""Job status from the control prefixes: _progress/<method>/<cycle>.json and _logs/<name>.log."""

from __future__ import annotations

import json
from dataclasses import dataclass

import obstore as obs
from obstore.store import ObjectStore


@dataclass(frozen=True)
class ProgressLine:
    """One progress manifest: the method, the cycle, and the count of done units."""

    method: str
    cycle_id: str
    units: int


def progress_lines(store: ObjectStore) -> list[ProgressLine]:
    """Read every manifest below `_progress/` and return the lines sorted by method, then cycle."""
    out = []
    for page in obs.list(store, prefix="_progress/"):
        for item in page:
            parts = item["path"].split("/", 2)
            # A directory marker or a stray key has no <method>/<cycle>.json tail.
            if len(parts) != 3 or not parts[2].endswith(".json"):
                continue
            _, method, name = parts
            units = len(json.loads(bytes(obs.get(store, item["path"]).bytes()))["done"])
            out.append(ProgressLine(method, name.removesuffix(".json"), units))
    return sorted(out, key=lambda p: (p.method, p.cycle_id))


def log_tail(store: ObjectStore, name: str, lines: int = 20) -> str:
    """Return the last `lines` lines of `_logs/<name>.log`."""
    key = f"_logs/{name}.log"
    try:
        data = bytes(obs.get(store, key).bytes())
    # obstore 0.11 gives FileNotFoundError for a missing key.
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"no log at {key}; the job may still run, because run_job.sh uploads at the end"
        ) from exc
    return "\n".join(data.decode(errors="replace").splitlines()[-lines:])
