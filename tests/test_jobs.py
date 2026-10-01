import obstore as obs
import pytest
from obstore.store import MemoryStore

from wxtco.jobs import ProgressLine, log_tail, progress_lines
from wxtco.progress import Progress


def _store_with_two_manifests() -> MemoryStore:
    """Two manifests, written out of order, so the sort has something to do."""
    store = MemoryStore()
    virtual = Progress(store, "virtual", "2026/09/16/T0600Z")
    virtual.mark("lead=0")
    table = Progress(store, "table", "2026/09/16/T0000Z")
    for unit in ("lead=0", "lead=60", "lead=120"):
        table.mark(unit)
    return store


def test_progress_lines_sorted_by_method_and_cycle():
    lines = progress_lines(_store_with_two_manifests())
    assert lines == [
        ProgressLine("table", "20260916T0000Z", 3),
        ProgressLine("virtual", "20260916T0600Z", 1),
    ]


def test_progress_lines_empty_store():
    assert progress_lines(MemoryStore()) == []


def test_log_tail_returns_last_lines():
    store = MemoryStore()
    obs.put(store, "_logs/table-c01.log", "\n".join(f"line {i}" for i in range(50)).encode())
    assert log_tail(store, "table-c01", lines=3) == "line 47\nline 48\nline 49"


def test_log_tail_missing_log_explains():
    with pytest.raises(FileNotFoundError, match="_logs/nope.log"):
        log_tail(MemoryStore(), "nope")
