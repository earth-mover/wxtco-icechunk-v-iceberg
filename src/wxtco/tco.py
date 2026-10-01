"""Monthly TCO from findings CSVs and prices.toml."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pandas as pd

DAYS = 30
QUERIES = ("q1", "q2", "q3")
# Q3 prices the batched stream at this batch size, per job of this many samples (decision 016).
Q3_BATCH = 8
Q3_SAMPLES = 16
COLUMNS = ["method", "storage_usd", "etl_usd", "query_usd", "request_usd", "total_usd"]
# S3 request kinds per query and the prices.toml key of each (LIST is billed at the PUT tier).
REQUEST_PRICES = {
    "get_per_run": "get_per_1000",
    "head_per_run": "get_per_1000",
    "list_per_run": "list_per_1000",
}


def load_prices(path: Path) -> dict:
    """Read the unit prices table."""
    return tomllib.loads(path.read_text())


def _read_all(folder: Path, pattern: str) -> pd.DataFrame:
    """Concatenate all CSVs that match the pattern. Empty frame if there are none."""
    files = sorted(folder.glob(pattern))
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True) if files else pd.DataFrame()


def _methods(frame: pd.DataFrame) -> set[str]:
    """Method names in a frame. An absent `method` column reads as no rows."""
    return set(frame["method"]) if "method" in frame.columns else set()


def _rows_for(frame: pd.DataFrame, **equals: object) -> pd.DataFrame:
    """Rows that match all column/value pairs. A missing column gives no rows."""
    if any(col not in frame.columns for col in equals):
        return frame.iloc[0:0]
    mask = pd.Series(True, index=frame.index)
    for col, value in equals.items():
        mask &= frame[col] == value
    return frame[mask]


def _hourly(prices: dict, instance: str) -> float:
    """Hourly on-demand price of an instance type."""
    try:
        return prices["ec2"][instance]
    except KeyError:
        raise KeyError(f"no EC2 price for instance type {instance!r}; add it to prices.toml") from None


def _median_run_usd(rows: pd.DataFrame, prices: dict) -> float:
    """Median compute cost of one run. Each row is priced on its own instance first."""
    per_row = rows["seconds"] / 3600 * rows["instance_type"].map(lambda i: _hourly(prices, i))
    return float(per_row.median())


def _request_usd(rows: pd.DataFrame, prices: dict) -> float:
    """Median S3 request cost of one ETL cycle. Zero when the counts are absent."""
    usd = 0.0
    for col, price_key in (("s3_put", "put_per_1000"), ("s3_get", "get_per_1000")):
        if col in rows.columns:
            usd += rows[col].fillna(0).median() * prices["s3"][price_key] / 1000
    return usd


def _q3_stream_rows(bench: pd.DataFrame, method: str) -> pd.DataFrame:
    """Q3 rows from the `q3_stream` runs at batch `Q3_BATCH`, scaled to one `Q3_SAMPLES` job.

    Q3 is priced as a stream (decision 016); a method without stream rows falls back to `q3`.
    """
    rows = _rows_for(bench, method=method, query="q3_stream")
    if not len(rows) or "detail" not in rows.columns:
        return rows.iloc[0:0]
    detail = rows["detail"].map(json.loads)
    rows = rows[detail.map(lambda d: d.get("batch") == Q3_BATCH)].copy()
    samples = detail[rows.index].map(lambda d: d["samples"])
    rows["seconds"] = rows["seconds"] * Q3_SAMPLES / samples
    return rows


def monthly_tco(
    prices: dict,
    storage: pd.DataFrame,
    etl: pd.DataFrame,
    bench: pd.DataFrame,
    workload_multiplier: float = 1.0,
    storage_variant: str = "data",
    requests: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Monthly cost per method. The multiplier scales the query workload only.

    `storage_variant` picks the storage.csv rows: `data` (the method's own bytes) or
    `with_netcdf` (plus the NetCDF copy, which the virtual method cannot do without).

    The query term is a serialized per-second model (workload x median seconds x instance
    price), with no concurrency and no idle time. The request term is workload x S3 requests per
    query (`requests.csv`: method, query, get/head/list per run) x request price.
    """
    requests = requests if requests is not None else pd.DataFrame()
    rows = []
    for method in sorted(_methods(storage) | _methods(etl) | _methods(bench) | _methods(requests)):
        has_gb = "gb" in storage.columns
        gb = _rows_for(storage, method=method, variant=storage_variant)["gb"].sum() if has_gb else 0.0
        storage_usd = gb * prices["s3"]["storage_gb_month"]

        e = _rows_for(etl, method=method)
        etl_usd = 0.0
        if len(e) and {"seconds", "instance_type"} <= set(e.columns):
            per_cycle = _median_run_usd(e, prices)
            etl_usd = (per_cycle + _request_usd(e, prices)) * prices["workload"]["etl_cycles"] * DAYS

        query_usd = 0.0
        for q in QUERIES:
            b = _q3_stream_rows(bench, method) if q == "q3" else pd.DataFrame()
            if not len(b):
                b = _rows_for(bench, method=method, query=q)
            if len(b) and {"seconds", "instance_type"} <= set(b.columns):
                per_day = prices["workload"].get(q, 0)
                query_usd += _median_run_usd(b, prices) * per_day * workload_multiplier * DAYS

        request_usd = 0.0
        for q in QUERIES:
            r = _rows_for(requests, method=method, query=q)
            per_query = sum(
                float(r[col].fillna(0).sum()) * prices["s3"].get(key, prices["s3"]["put_per_1000"]) / 1000
                for col, key in REQUEST_PRICES.items()
                if col in r.columns
            )
            request_usd += per_query * prices["workload"].get(q, 0) * workload_multiplier * DAYS

        rows.append(
            {
                "method": method,
                "storage_usd": storage_usd,
                "etl_usd": etl_usd,
                "query_usd": query_usd,
                "request_usd": request_usd,
                "total_usd": storage_usd + etl_usd + query_usd + request_usd,
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def tco_from_findings(
    findings: Path, prices_path: Path, multiplier: float = 1.0, storage_variant: str = "data"
) -> pd.DataFrame:
    """Build the TCO table from the findings folder and a prices file."""
    prices = load_prices(prices_path)
    storage_csv = findings / "storage.csv"
    storage = (
        pd.read_csv(storage_csv)
        if storage_csv.exists()
        else pd.DataFrame(columns=["method", "variant", "gb"])
    )
    etl = _read_all(findings / "etl", "*.csv")
    bench = _read_all(findings / "bench", "*.csv")
    requests_csv = findings / "requests.csv"
    requests = pd.read_csv(requests_csv) if requests_csv.exists() else pd.DataFrame()
    return monthly_tco(prices, storage, etl, bench, multiplier, storage_variant, requests)
