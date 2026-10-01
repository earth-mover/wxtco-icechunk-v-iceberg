"""Drive the virtualization over the synthetic fixture into a local Icechunk repo."""

import importlib

import numpy as np
import pytest
import xarray as xr

import wxtco.ingest.mogreps_virtual as mv
from wxtco.config import Settings, store_for
from wxtco.fixture import build_cycle
from wxtco.ingest.virtual import ingest_cycle_virtual, open_virtual_dataset_group, open_virtual_repo
from wxtco.progress import Progress


def _use_fixture(monkeypatch, root):
    """Point the virtualization module at a fixture root. It reads its settings at import, so reload it."""
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{root}")
    importlib.reload(mv)


def _restore(monkeypatch):
    """Undo `_use_fixture`, so the module left in sys.modules matches the real environment."""
    monkeypatch.delenv("WXTCO_COPY_URL", raising=False)
    importlib.reload(mv)


def test_virtual_ingest_two_cycles(tmp_path, monkeypatch):
    c0, c1 = "2026/09/16/T0000Z", "2026/09/16/T0600Z"
    build_cycle(tmp_path / "src", c0)
    build_cycle(tmp_path / "src", c1)
    _use_fixture(monkeypatch, tmp_path / "src")
    try:
        s = Settings.from_env()
        repo = open_virtual_repo(s, "local", local_path=tmp_path / "repo")
        prog = store_for(f"file://{tmp_path / 'prog'}")
        r0 = ingest_cycle_virtual(
            repo, prog, c0, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None
        )
        assert r0.units_done == 1
        r1 = ingest_cycle_virtual(
            repo, prog, c1, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None
        )
        assert r1.units_done == 1
        again = ingest_cycle_virtual(
            repo, prog, c1, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None
        )
        assert again.units_skipped == 1

        ds = open_virtual_dataset_group(repo, "surface")
        assert "temperature_at_screen_level" in ds
        assert ds["temperature_at_screen_level"].dims == (
            "forecast_reference_time",
            "forecast_period",
            "realization",
            "latitude",
            "longitude",
        )
        assert ds.sizes["forecast_reference_time"] >= 2 and ds.sizes["forecast_period"] == 4
        # slot 1 = second cycle, lead index 2 (120 min), member 1, lat 3, lon 5
        v = (
            ds["temperature_at_screen_level"]
            .isel(forecast_reference_time=1, forecast_period=2, realization=1, latitude=3, longitude=5)
            .values
        )
        assert v == np.float32(1000 + 20 + 3 + 0.05)
        st = xr.open_zarr(
            repo.readonly_session("main").store, group="surface/status", consolidated=False, zarr_format=3
        )
        assert int(st["temperature_at_screen_level"].isel(forecast_reference_time=1).max()) == 0
    finally:
        _restore(monkeypatch)


def test_virtual_ingest_skips_written_cycle(tmp_path, monkeypatch):
    """A lost progress mark must not rewrite the slot: the commit ancestry is the second guard."""
    c0 = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", c0)
    _use_fixture(monkeypatch, tmp_path / "src")
    try:
        repo = open_virtual_repo(Settings.from_env(), "local", local_path=tmp_path / "repo")
        prog = store_for(f"file://{tmp_path / 'prog'}")
        first = ingest_cycle_virtual(
            repo, prog, c0, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None
        )
        assert first.units_done == 1
        before = len(list(repo.ancestry(branch="main")))

        Progress(prog, "virtual", c0).clear()
        again = ingest_cycle_virtual(
            repo, prog, c0, origin_cycle=c0, workers=2, allow_incomplete=True, log=lambda *_: None
        )
        assert again.units_skipped == 1 and again.units_done == 0
        assert len(list(repo.ancestry(branch="main"))) == before
        assert Progress(prog, "virtual", c0).done("cycle")
    finally:
        _restore(monkeypatch)


def test_open_virtual_repo_arraylake_requires_token():
    """Fail fast rather than let the Arraylake client fall back to interactive auth."""
    settings = Settings("s", "r", "c", "r", "org", None, "u", None)
    with pytest.raises(ValueError, match="ARRAYLAKE_TOKEN"):
        open_virtual_repo(settings, "arraylake")


def test_open_virtual_repo_sets_chunk_cache_and_keeps_manifest_config(tmp_path, monkeypatch):
    build_cycle(tmp_path / "src", "2026/09/16/T0000Z")
    _use_fixture(monkeypatch, tmp_path / "src")
    monkeypatch.setenv("WXTCO_CHUNK_CACHE_BYTES", "12345678")
    try:
        repo = open_virtual_repo(Settings.from_env(), "local", local_path=tmp_path / "repo")
    finally:
        _restore(monkeypatch)
    assert repo.config.caching.num_bytes_chunks == 12345678
    # Reader config merges over the writer config; manifest splitting must survive.
    assert repo.config.manifest is not None and repo.config.manifest.splitting is not None
