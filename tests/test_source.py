from datetime import UTC, datetime

import obstore as obs
from obstore.store import MemoryStore

from wxtco.source import (
    Diagnostic,
    available_cycles,
    cycle_id,
    cycle_time,
    list_cycle,
    parse_key,
    surface_only,
)

KEY = "2026/09/16/T0000Z/20260916T0300Z-PT0003H00M-temperature_at_screen_level.nc"
WINDOWED_KEY = "2026/09/16/T0000Z/20260916T0100Z-PT0001H00M-precipitation_accumulation-PT01H.nc"


def test_parse_key():
    f = parse_key(KEY)
    assert f is not None
    assert f.slug == "temperature_at_screen_level"
    assert f.lead_minutes == 180
    assert f.valid_time == datetime(2026, 9, 16, 3, tzinfo=UTC)
    w = parse_key(WINDOWED_KEY)
    assert w is not None
    assert w.slug == "precipitation_accumulation-PT01H"
    assert w.lead_minutes == 60


def test_parse_key_rejects_other_files():
    assert parse_key("2026/09/16/T0000Z/README.txt") is None
    assert parse_key("2026/09/16/T0000Z/junk-20260916T0100Z-PT0001H00M-x.nc") is None


def test_cycle_time_and_id():
    assert cycle_time("2026/09/16/T0600Z") == datetime(2026, 9, 16, 6, tzinfo=UTC)
    assert cycle_id("2026/09/16/T0600Z") == "20260916T0600Z"


def test_diagnostic_flags():
    f = parse_key(KEY)
    d = Diagnostic("temperature_at_screen_level", (f,))
    assert not d.is_level and d.window == ""
    lv = Diagnostic("wind_speed_on_height_levels", ())
    assert lv.is_level
    st = Diagnostic("precipitation_accumulation-PT01H", ())
    assert st.window == "PT01H"
    wlv = Diagnostic("wind_speed_on_height_levels-PT01H", ())
    assert wlv.is_level and wlv.window == "PT01H"


def _put(store, key):
    obs.put(store, key, b"x")


def test_list_cycle_and_available():
    store = MemoryStore()
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-b_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0100Z-PT0001H00M-b_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-a_var.nc")
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-x_on_height_levels.nc")
    _put(store, "2026/09/15/T1800Z/20260915T1800Z-PT0000H00M-a_var.nc")
    diags = list_cycle(store, "2026/09/16/T0000Z")
    assert [d.slug for d in diags] == ["a_var", "b_var", "x_on_height_levels"]
    assert [f.lead_minutes for f in diags[1].files] == [0, 60]
    assert [d.slug for d in surface_only(diags)] == ["a_var", "b_var"]
    assert available_cycles(store) == ["2026/09/15/T1800Z", "2026/09/16/T0000Z"]


def test_available_cycles_ignores_control_prefixes():
    store = MemoryStore()
    _put(store, "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-a_var.nc")
    _put(store, "_progress/table/aa/bb/cc.json")
    _put(store, "_code/wxtco/source.py")
    _put(store, "_logs/2026/09/16/run.log")
    assert available_cycles(store) == ["2026/09/16/T0000Z"]
