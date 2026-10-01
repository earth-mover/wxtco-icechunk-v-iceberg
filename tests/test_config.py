import os
from pathlib import Path

import pytest
from obstore.store import LocalStore, S3Store

from wxtco.config import Settings, split_url, store_for


def _clear_env(monkeypatch):
    """Drop project variables so only defaults and `.env` remain."""
    for k in list(os.environ):
        if k.startswith("WXTCO_") or k in {"ARRAYLAKE_TOKEN", "ARRAYLAKE_ORG"}:
            monkeypatch.delenv(k)


def test_defaults(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _clear_env(monkeypatch)
    s = Settings.from_env()
    assert s.source_url == "s3://met-office-global-ensemble-model-data/global-ensemble"
    assert s.copy_url == "s3://em-tco-mogreps/netcdf"
    assert s.iceberg_uri == "https://api.earthmover.io/iceberg"


def test_env_override(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _clear_env(monkeypatch)
    monkeypatch.setenv("WXTCO_COPY_URL", "file:///tmp/copy")
    monkeypatch.setenv("ARRAYLAKE_ORG", "wxtco-test")
    s = Settings.from_env()
    assert s.copy_url == "file:///tmp/copy"
    assert s.arraylake_org == "wxtco-test"


def test_dotenv_precedence(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("WXTCO_COPY_URL=file:///from-dotenv\n")

    # `.env` fills the gap when the variable is absent. Set first, so teardown restores the real value.
    monkeypatch.setenv("WXTCO_COPY_URL", "placeholder")
    monkeypatch.delenv("WXTCO_COPY_URL")
    assert Settings.from_env().copy_url == "file:///from-dotenv"

    # The environment wins. `load_dotenv` set the variable above, so set it again here.
    monkeypatch.setenv("WXTCO_COPY_URL", "file:///from-env")
    assert Settings.from_env().copy_url == "file:///from-env"


def test_split_url():
    assert split_url("s3://bucket/a/b") == ("bucket", "a/b")
    assert split_url("s3://bucket") == ("bucket", "")
    assert split_url("file:///tmp/x/y") == ("/tmp/x/y", "")
    with pytest.raises(ValueError):
        split_url("s3://")
    with pytest.raises(ValueError):
        split_url("file://tmp/x")
    with pytest.raises(ValueError):
        split_url("gs://bucket/a")


def test_store_for_local(tmp_path: Path):
    store = store_for(f"file://{tmp_path}")
    assert isinstance(store, LocalStore)


def test_store_for_s3_anonymous():
    store = store_for(
        "s3://met-office-global-ensemble-model-data/global-ensemble", region="eu-west-2", anonymous=True
    )
    assert isinstance(store, S3Store)
