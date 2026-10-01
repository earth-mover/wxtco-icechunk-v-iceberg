from typer.testing import CliRunner

from wxtco.cli import app

runner = CliRunner()


def test_fixture_build_and_source_cycles(tmp_path, monkeypatch):
    # Settings.from_env reads `.env` from the CWD; keep it out of the repo root.
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["fixture", "build", str(tmp_path), "--cycle", "2026/09/16/T0000Z"])
    assert r.exit_code == 0, r.output
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path}")
    r = runner.invoke(app, ["source", "cycles"])
    assert r.exit_code == 0 and "2026/09/16/T0000Z" in r.output
    r = runner.invoke(app, ["source", "inventory", "2026/09/16/T0000Z"])
    assert r.exit_code == 0 and "temperature_at_screen_level" in r.output


def test_copy_plan_and_verify(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["fixture", "build", str(tmp_path / "src")])
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{tmp_path / 'src'}")
    monkeypatch.setenv("WXTCO_COPY_URL", f"file://{tmp_path / 'dst'}")
    r = runner.invoke(app, ["copy", "plan", "--cycles", "2026/09/16/T0000Z"])
    assert r.exit_code == 0 and "aws s3 cp" in r.output
    # Whitespace and trailing slashes must not reach the s3 paths.
    r = runner.invoke(app, ["copy", "plan", "--cycles", "2026/09/16/T0000Z, 2026/09/16/T0600Z/"])
    cmds = [ln for ln in r.output.splitlines() if ln.startswith("aws s3 cp")]
    assert r.exit_code == 0 and len(cmds) == 2
    assert all("//" not in cmd.replace("file://", "").replace("s3://", "") for cmd in cmds)
    r = runner.invoke(app, ["copy", "verify", "--cycle", "2026/09/16/T0000Z"])
    assert r.exit_code == 1 and "  missing " in r.output
    # An unknown cycle has no source files, which is a failure, not a clean copy.
    r = runner.invoke(app, ["copy", "verify", "--cycle", "2026/09/15/T1200Z"])
    assert r.exit_code == 1 and "no source files for 2026/09/15/T1200Z" in r.output


def test_copy_pick_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    # Cheap cycle tree: LocalStore reports a prefix only when it holds a file.
    for day in range(5, 17):
        for hour in (0, 6, 12, 18):
            cycle = src / "2026" / "09" / f"{day:02d}" / f"T{hour:02d}00Z"
            cycle.mkdir(parents=True)
            (cycle / "dummy.nc").write_bytes(b"")
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{src}")
    # WINDOW_FILE is bound at import time, so patch the module attribute.
    out_file = tmp_path / "study_cycles.txt"
    monkeypatch.setattr("wxtco.window.WINDOW_FILE", out_file)
    r = runner.invoke(app, ["copy", "pick-window", "--days", "7", "--write"])
    assert r.exit_code == 0, r.output
    cycles = [ln for ln in r.output.splitlines() if ln.startswith("2026/")]
    assert len(cycles) == 28
    assert cycles[0] == "2026/09/10/T0000Z" and cycles[-1] == "2026/09/16/T1800Z"
    assert len(out_file.read_text().strip().splitlines()) == 28
