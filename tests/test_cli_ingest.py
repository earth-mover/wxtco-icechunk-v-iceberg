from typer.testing import CliRunner

from wxtco.cli import app
from wxtco.fixture import FIXTURE_SLUGS

runner = CliRunner()


def _invoke(cycle, root, cat):
    """Run `ingest table` against the fixture with a local catalog."""
    return runner.invoke(app, ["ingest", "table", "--cycle", cycle, "--local", str(cat)])


def test_ingest_table_local(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    # Settings reads `.env` from the CWD, so keep the CWD off the repo.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{root}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    r = _invoke(cycle, root, tmp_path / "cat")
    assert r.exit_code == 0, r.output
    assert f"{cycle}: 4 leads done, 0 skipped, {4 * 2 * 8 * 8} rows" in r.output


def test_ingest_table_resumes(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{root}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    assert _invoke(cycle, root, tmp_path / "cat").exit_code == 0
    r = _invoke(cycle, root, tmp_path / "cat")
    assert r.exit_code == 0, r.output
    assert "0 leads done, 4 skipped" in r.output


def test_ingest_virtual_local(tmp_path, monkeypatch):
    from wxtco.fixture import build_cycle

    cycle = "2026/09/16/T0000Z"
    build_cycle(tmp_path / "src", cycle)
    # Settings reads `.env` from the CWD, so keep the CWD off the repo.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'src'}")
    # the virtualization module reads settings at import; reload after the env is set
    import importlib

    import wxtco.ingest.mogreps_virtual as mv

    importlib.reload(mv)

    r = runner.invoke(
        app,
        [
            "ingest",
            "virtual",
            "--cycle",
            cycle,
            "--origin",
            cycle,
            "--local",
            str(tmp_path / "repo"),
            "--workers",
            "2",
            "--allow-incomplete",
        ],
    )
    assert r.exit_code == 0, r.output
    assert "1 units done" in r.output
