import obstore as obs
from obstore.store import MemoryStore

from wxtco.progress import Progress


def test_progress_roundtrip():
    store = MemoryStore()
    p = Progress(store, "table", "2026/09/16/T0000Z")
    assert not p.done("lead=0")
    p.mark("lead=0")
    p.mark("lead=60")
    assert p.done("lead=0") and p.done("lead=60")
    fresh = Progress(store, "table", "2026/09/16/T0000Z")
    assert fresh.units() == {"lead=0", "lead=60"}
    fresh.clear()
    assert Progress(store, "table", "2026/09/16/T0000Z").units() == set()


def test_progress_isolated_by_method_and_cycle():
    store = MemoryStore()
    Progress(store, "table", "2026/09/16/T0000Z").mark("u")
    assert not Progress(store, "virtual", "2026/09/16/T0000Z").done("u")
    assert not Progress(store, "table", "2026/09/16/T0600Z").done("u")


def test_progress_corrupt_manifest_is_empty():
    store = MemoryStore()
    p = Progress(store, "table", "2026/09/16/T0000Z")
    obs.put(store, p.key, b"not json")
    assert p.units() == set()
    p.mark("u")
    assert Progress(store, "table", "2026/09/16/T0000Z").units() == {"u"}
