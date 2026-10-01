import csv
import json

import numpy as np
import pytest
import xarray as xr

from wxtco import bench
from wxtco.bench import run_query


class FakeBackend:
    name = "fake"

    def point_series(self, var, point, cycle):
        valid = np.array(["2026-09-16T00", "2026-09-16T06"], dtype="datetime64[ns]")
        return xr.DataArray(
            np.ones((2, 2), dtype="float32") * 280,
            dims=("member", "lead"),
            coords={"member": [0, 1], "lead": [0, 6], "valid_time": ("lead", valid)},
        )

    def bytes_read(self):
        return 123


PARAMS = {"lat": 1.0, "lon": 2.0, "cycle": "2026/09/16/T0000Z"}
META = {"instance_type": "local", "region": "local", "git_sha": "abc"}


def test_run_query_writes_csv(tmp_path):
    out = tmp_path / "q1.csv"
    rows = run_query(FakeBackend(), "q1", PARAMS, runs=3, out_csv=out, meta=META)
    assert len(rows) == 3
    with out.open() as f:
        recs = list(csv.DictReader(f))
    assert len(recs) == 3
    assert recs[0]["method"] == "fake"
    assert recs[0]["query"] == "q1"
    assert recs[0]["bytes_read"] == "123"
    assert list(recs[0]).index("net_rx_bytes") == list(recs[0]).index("bytes_read") + 1
    assert list(recs[0])[-1] == "detail" and recs[0]["detail"] == "{}"
    # Empty where /proc/net/dev is absent; a non-negative count otherwise.
    assert all(r["net_rx_bytes"] == "" or int(r["net_rx_bytes"]) >= 0 for r in recs)
    run_query(FakeBackend(), "q1", PARAMS, runs=1, out_csv=out, meta=META)
    with out.open() as f:
        assert len(list(csv.DictReader(f))) == 4


def test_net_rx_bytes_skips_lo(tmp_path, monkeypatch):
    dev = tmp_path / "dev"
    dev.write_text(
        "Inter-|   Receive\n face |bytes    packets\n"
        "    lo:  500 1 0 0 0 0 0 0 500 1\n  eth0: 1000 2 0 0 0 0 0 0 9 1\n  eth1:24 1 0 0 0 0 0 0 0 0\n"
    )
    monkeypatch.setattr(bench, "NET_DEV", dev)
    assert bench.net_rx_bytes() == 1024
    monkeypatch.setattr(bench, "NET_DEV", tmp_path / "absent")
    assert bench.net_rx_bytes() is None
    rows = run_query(FakeBackend(), "q1", PARAMS, runs=1, out_csv=tmp_path / "q1.csv", meta=META)
    assert rows[0].net_rx_bytes is None


def test_run_query_rejects_old_schema_csv(tmp_path):
    out = tmp_path / "old.csv"
    out.write_text("timestamp,git_sha,method,query,params,run,seconds,bytes_read,instance_type,region\n")
    with pytest.raises(ValueError, match="new CSV"):
        run_query(FakeBackend(), "q1", PARAMS, runs=1, out_csv=out, meta=META)


class FakeQ3Backend:
    name = "fake"

    def batch_fields(self, vars, cycles, leads):
        return np.zeros((len(cycles), len(leads), len(vars), 2, 4, 4), dtype="float32")

    def bytes_read(self):
        return None


def test_run_query_q3_stream_writes_detail(tmp_path):
    out = tmp_path / "q3s.csv"
    params = {"vars": ["a"], "cycles": ["c1", "c2"], "leads": [0, 1], "batch": 2}
    run_query(FakeQ3Backend(), "q3_stream", params, runs=1, out_csv=out, meta=META)
    with out.open() as f:
        rec = next(csv.DictReader(f))
    detail = json.loads(rec["detail"])
    assert (detail["samples"], detail["batches"], detail["batch"]) == (4, 2, 2)
    assert {"steady_gbps", "first_batch_seconds", "gbps"} <= set(detail)
