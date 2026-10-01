"""Export the canonical findings as an ES module for the blog chart widgets.

Reads docs/findings/{storage.csv,requests.csv,etl/*.csv,bench/*.csv} and prices.toml. Writes web/wxtco-data.js.
Run: uv run python scripts/export_web_data.py
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from wxtco.tco import Q3_BATCH, Q3_SAMPLES, _q3_stream_rows, _read_all, load_prices

ROOT = Path(__file__).resolve().parents[1]
FINDINGS = ROOT / "docs" / "findings"
OUT = ROOT / "web" / "wxtco-data.js"

# Display metadata per method. Order is the display order.
METHODS = [
    ("download", "Download NetCDF", "Download", "whole files per query, obstore", "file"),
    ("fuse", "FUSE mount", "FUSE", "NetCDF read in place through Mountpoint", "file"),
    ("table_hilbert", "Iceberg", "Iceberg", "Parquet, Hilbert row order, DuckDB", "table"),
    ("virtual", "Virtual Icechunk", "Virtual", "references into the NetCDF files", "tensor"),
    (
        "native_ts",
        "Native Icechunk, timeseries",
        "Native ts",
        "chunks (18 members, 57 leads, 16 x 16)",
        "tensor",
    ),
    ("native_dl", "Native Icechunk, dataloader", "Native dl", "one 7.2 GB shard per (cycle, lead)", "tensor"),
]

QUERIES = {
    "q1": {"label": "Q1 point series", "sub": "one variable, one grid cell, 171 leads x 18 members"},
    "q2": {"label": "Q2 regional stats", "sub": "two variables over the UK, two cycles, CRPS"},
    "q3": {
        "label": "Q3 dataloader",
        "sub": f"{Q3_SAMPLES} samples, 4 variables, full grid, batches of {Q3_BATCH}",
    },
}


def _median(rows: pd.DataFrame, col: str) -> float | None:
    """Median of a column, or None when the column is absent or empty."""
    if col not in rows.columns or rows[col].dropna().empty:
        return None
    return float(rows[col].dropna().median())


def main() -> None:
    prices = load_prices(ROOT / "prices.toml")
    storage = pd.read_csv(FINDINGS / "storage.csv")
    etl = _read_all(FINDINGS / "etl", "*.csv")
    bench = _read_all(FINDINGS / "bench", "*.csv")
    requests = pd.read_csv(FINDINGS / "requests.csv")
    netcdf_gb = float(storage.query("method == 'download' and variant == 'with_netcdf'")["gb"].iloc[0])

    methods = []
    for mid, label, short, detail, family in METHODS:
        own = storage.query("method == @mid and variant == 'data'")["gb"].sum()
        e = etl[etl["method"] == mid] if len(etl) else etl
        q = {}
        for qid in ("q1", "q2", "q3"):
            rows = (
                _q3_stream_rows(bench, mid)
                if qid == "q3"
                else bench[(bench["method"] == mid) & (bench["query"] == qid)]
            )
            if qid == "q3" and rows.empty:
                rows = bench[(bench["method"] == mid) & (bench["query"] == "q3")]
            wire = _median(rows, "net_rx_bytes")
            if qid == "q3" and wire is not None and len(rows):
                # Stream rows carry wire bytes for the full 32-sample run; scale to one job.
                samples = json.loads(rows["detail"].iloc[0])["samples"]
                wire = wire * Q3_SAMPLES / samples
            req = requests[(requests["method"] == mid) & (requests["query"] == qid)]
            counts = (
                {k: round(float(req[f"{k}_per_run"].iloc[0]), 1) for k in ("get", "head", "list")}
                if len(req)
                else None
            )
            q[qid] = {
                "seconds": _median(rows, "seconds"),
                "runs": len(rows),
                "wire_bytes": wire,
                "requests": counts,
            }
        methods.append(
            {
                "id": mid,
                "label": label,
                "short": short,
                "detail": detail,
                "family": family,
                "storage_gb": round(float(own), 1),
                "etl_seconds": _median(e, "seconds"),
                "etl_cycles_measured": len(e),
                "queries": q,
            }
        )

    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=ROOT, check=False
    ).stdout.strip()
    data = {
        "generated": datetime.now(UTC).strftime("%Y-%m-%d"),
        "commit": commit,
        "prices": {
            "s3_gb_month": prices["s3"]["storage_gb_month"],
            "s3_get_per_1000": prices["s3"]["get_per_1000"],
            "s3_list_per_1000": prices["s3"]["list_per_1000"],
            "etl_instance": "c7i.16xlarge",
            "etl_usd_per_hour": prices["ec2"]["c7i.16xlarge"],
            "query_instance": "m7i.4xlarge",
            "query_usd_per_hour": prices["ec2"]["m7i.4xlarge"],
        },
        "workload": {k: prices["workload"][k] for k in ("q1", "q2", "q3", "etl_cycles")},
        "netcdf_gb": netcdf_gb,
        "queries": QUERIES,
        "methods": methods,
    }
    OUT.parent.mkdir(exist_ok=True)
    body = json.dumps(data, indent=2)
    OUT.write_text(
        f"// Generated by scripts/export_web_data.py from docs/findings. Do not edit.\nexport default {body};\n"
    )
    print(f"wrote {OUT.relative_to(ROOT)} ({len(methods)} methods, commit {commit})")


if __name__ == "__main__":
    main()
