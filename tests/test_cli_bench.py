import csv
import json
from pathlib import Path

import numpy as np
from typer.testing import CliRunner

from wxtco.cli import app
from wxtco.fixture import FIXTURE_SLUGS

runner = CliRunner()
PRICES = Path(__file__).resolve().parents[1] / "prices.toml"


def test_bench_q1_table_local(fixture_root, tmp_path, monkeypatch):
    root, cycle = fixture_root
    # Settings reads `.env` from the CWD, so keep the CWD off the repo.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WXTCO_SOURCE_URL", f"file://{root}")
    monkeypatch.setenv("WXTCO_SLUGS", ",".join(FIXTURE_SLUGS))
    cat = tmp_path / "cat"
    assert runner.invoke(app, ["ingest", "table", "--cycle", cycle, "--local", str(cat)]).exit_code == 0

    # Nearest grid cell to (lat index 3, lon index 5) on the 8x8 fixture grid.
    params = {
        "lat": float(np.linspace(-89.9, 89.9, 8)[3]),
        "lon": float(np.linspace(-179.9, 179.9, 8)[5]),
        "cycle": cycle,
    }
    out = tmp_path / "bench" / "q1.csv"
    r = runner.invoke(
        app,
        [
            "bench",
            "--method",
            "table",
            "--query",
            "q1",
            "--local",
            str(cat),
            "--params",
            json.dumps(params),
            "--runs",
            "2",
            "--out",
            str(out),
        ],
    )
    assert r.exit_code == 0, r.output
    with out.open() as f:
        recs = list(csv.DictReader(f))
    assert len(recs) == 2
    assert {rec["method"] for rec in recs} == {"table"}
    assert [rec["run"] for rec in recs] == ["0", "1"]


def test_bench_rejects_bad_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(
        app,
        [
            "bench",
            "--method",
            "table",
            "--query",
            "q1",
            "--params",
            "{oops",
            "--out",
            str(tmp_path / "b.csv"),
        ],
    )
    assert r.exit_code != 0


def test_tco_prints_table(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    findings = tmp_path / "findings"
    (findings / "etl").mkdir(parents=True)
    (findings / "bench").mkdir()
    (findings / "storage.csv").write_text("method,variant,gb\ntable,data,100.0\n")
    r = runner.invoke(app, ["tco", "--findings", str(findings), "--prices", str(PRICES)])
    assert r.exit_code == 0, r.output
    assert "table" in r.output


def test_jobs_help_lists_commands():
    r = runner.invoke(app, ["jobs", "--help"])
    assert r.exit_code == 0, r.output
    assert "status" in r.output
    assert "log" in r.output
