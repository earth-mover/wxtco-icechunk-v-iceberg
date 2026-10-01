"""Checks on the study slug list and its environment override."""

from wxtco.slugs import SURFACE_SLUGS, study_slugs


def test_surface_slug_count() -> None:
    # 102 inventory diagnostics - 21 level diagnostics.
    assert len(SURFACE_SLUGS) == 81


def test_no_level_slugs() -> None:
    assert not [s for s in SURFACE_SLUGS if s.endswith("_levels")]


def test_no_duplicates() -> None:
    assert len(set(SURFACE_SLUGS)) == len(SURFACE_SLUGS)


def test_known_slugs_present() -> None:
    assert "temperature_at_screen_level" in SURFACE_SLUGS
    assert "precipitation_accumulation-PT01H" in SURFACE_SLUGS


def test_study_slugs_default() -> None:
    assert study_slugs() == list(SURFACE_SLUGS)


def test_study_slugs_env_override(monkeypatch) -> None:
    monkeypatch.setenv("WXTCO_SLUGS", "a, b")
    assert study_slugs() == ["a", "b"]
