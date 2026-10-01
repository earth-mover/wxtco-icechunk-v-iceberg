from wxtco.window import pick_window


def _cycles(days: int, start_day: int = 1) -> list[str]:
    return [
        f"2026/09/{d:02d}/T{h:02d}00Z" for d in range(start_day, start_day + days) for h in (0, 6, 12, 18)
    ]


def test_pick_window_newest_week():
    avail = _cycles(30)
    w = pick_window(avail, days=7, margin_days=3)
    assert len(w) == 28
    assert w[-1] == avail[-1]
    assert w[0] == "2026/09/24/T0000Z"


def test_pick_window_needs_enough_cycles():
    import pytest

    with pytest.raises(ValueError):
        pick_window(_cycles(5), days=7)
