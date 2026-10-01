"""Per-(method, cycle) completion manifest so reruns skip finished units."""

from __future__ import annotations

import json

import obstore as obs
from obstore.store import ObjectStore

from wxtco.source import cycle_id


class Progress:
    """JSON manifest at `_progress/<method>/<cycle_id>.json` that records the done units.

    Concurrent writers overwrite each other and the last writer wins. Keep units idempotent,
    so a lost mark only causes repeated work.
    """

    def __init__(self, store: ObjectStore, method: str, cycle: str) -> None:
        self.store = store
        self.key = f"_progress/{method}/{cycle_id(cycle)}.json"
        self._units: set[str] | None = None

    def units(self) -> set[str]:
        """Return the done units. Read the manifest once, then cache."""
        if self._units is None:
            try:
                self._units = set(json.loads(bytes(obs.get(self.store, self.key).bytes()))["done"])
            # obstore 0.11 gives FileNotFoundError for a missing key. A bad manifest reads as empty.
            except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError):
                self._units = set()
        return self._units

    def done(self, unit: str) -> bool:
        return unit in self.units()

    def mark(self, unit: str) -> None:
        """Record one unit. Rewrites the manifest; unit counts per cycle are small."""
        self.units().add(unit)
        obs.put(self.store, self.key, json.dumps({"done": sorted(self.units())}).encode())

    def clear(self) -> None:
        self._units = set()
        obs.put(self.store, self.key, json.dumps({"done": []}).encode())
