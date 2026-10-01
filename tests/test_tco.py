import pandas as pd

from wxtco.tco import monthly_tco

PRICES = {
    "s3": {"storage_gb_month": 0.02, "put_per_1000": 0.005, "get_per_1000": 0.0004},
    "ec2": {"big": 3.6, "small": 0.36, "local": 0.0},
    "workload": {"q1": 100, "q2": 0, "q3": 0, "etl_cycles": 1},
}


def test_monthly_tco_arithmetic():
    storage = pd.DataFrame({"method": ["table"], "variant": ["data"], "gb": [1000.0]})
    etl = pd.DataFrame(
        {
            "method": ["table"],
            "cycle": ["c"],
            "instance_type": ["big"],
            "seconds": [3600.0],
            "s3_put": [None],
            "s3_get": [None],
        }
    )
    bench = pd.DataFrame(
        {
            "method": ["table"] * 2,
            "query": ["q1"] * 2,
            "seconds": [1.0, 3.0],
            "instance_type": ["small"] * 2,
        }
    )
    out = monthly_tco(PRICES, storage, etl, bench).set_index("method")
    assert out.loc["table", "storage_usd"] == 20.0
    assert out.loc["table", "etl_usd"] == 3.6 * 30
    # median 2 s x 100/day x 30 days x 0.36/h / 3600
    assert abs(out.loc["table", "query_usd"] - 2 * 100 * 30 * 0.36 / 3600) < 1e-9
    assert (
        out.loc["table", "total_usd"]
        == out.loc["table", ["storage_usd", "etl_usd", "query_usd", "request_usd"]].sum()
    )


def test_monthly_tco_empty_inputs():
    empty = pd.DataFrame()
    out = monthly_tco(PRICES, empty, empty, empty)
    assert list(out.columns) == ["method", "storage_usd", "etl_usd", "query_usd", "request_usd", "total_usd"]
    assert len(out) == 0


def test_monthly_tco_multiplier():
    storage = pd.DataFrame({"method": ["table"], "variant": ["data"], "gb": [1000.0]})
    etl = pd.DataFrame({"method": ["table"], "cycle": ["c"], "instance_type": ["big"], "seconds": [3600.0]})
    bench = pd.DataFrame({"method": ["table"], "query": ["q1"], "seconds": [2.0], "instance_type": ["small"]})
    base = monthly_tco(PRICES, storage, etl, bench).set_index("method")
    ten = monthly_tco(PRICES, storage, etl, bench, workload_multiplier=10.0).set_index("method")
    assert ten.loc["table", "storage_usd"] == base.loc["table", "storage_usd"]
    assert ten.loc["table", "etl_usd"] == base.loc["table", "etl_usd"]
    assert abs(ten.loc["table", "query_usd"] - 10 * base.loc["table", "query_usd"]) < 1e-9
    assert (
        abs(
            ten.loc["table", "total_usd"]
            - ten.loc["table", ["storage_usd", "etl_usd", "query_usd", "request_usd"]].sum()
        )
        < 1e-9
    )


def test_monthly_tco_mixed_instances():
    # Rows are priced one by one, so a leading local (free) row must not zero the method.
    bench = pd.DataFrame(
        {
            "method": ["table"] * 2,
            "query": ["q1"] * 2,
            "seconds": [4.0, 2.0],
            "instance_type": ["local", "small"],
        }
    )
    empty = pd.DataFrame()
    out = monthly_tco(PRICES, empty, empty, bench).set_index("method")
    expected = ((4.0 / 3600 * 0.0) + (2.0 / 3600 * 0.36)) / 2 * 100 * 30
    assert out.loc["table", "query_usd"] > 0
    assert abs(out.loc["table", "query_usd"] - expected) < 1e-9


def test_q3_prices_the_b8_stream_per_16_samples():
    import json

    import pandas as pd

    from wxtco.tco import monthly_tco

    prices = {
        "s3": {"storage_gb_month": 0.0, "put_per_1000": 0.0, "get_per_1000": 0.0},
        "ec2": {"m7i.4xlarge": 3600.0},
        "workload": {"q1": 0, "q2": 0, "q3": 1, "etl_cycles": 0},
    }
    bench = pd.DataFrame(
        [
            {"method": "m", "query": "q3", "seconds": 100.0, "instance_type": "m7i.4xlarge", "detail": "{}"},
            {
                "method": "m",
                "query": "q3_stream",
                "seconds": 8.0,
                "instance_type": "m7i.4xlarge",
                "detail": json.dumps({"batch": 8, "samples": 32}),
            },
            {
                "method": "m",
                "query": "q3_stream",
                "seconds": 50.0,
                "instance_type": "m7i.4xlarge",
                "detail": json.dumps({"batch": 1, "samples": 32}),
            },
        ]
    )
    df = monthly_tco(prices, pd.DataFrame(), pd.DataFrame(), bench)
    # 8 s for 32 samples at B=8 -> 4 s per 16-sample job, $1/s, 30 days.
    assert df.loc[0, "query_usd"] == 4.0 * 30
    # Without stream rows the loop row is used.
    df2 = monthly_tco(prices, pd.DataFrame(), pd.DataFrame(), bench[bench["query"] == "q3"])
    assert df2.loc[0, "query_usd"] == 100.0 * 30


def test_request_cost_per_query():
    import pandas as pd

    from wxtco.tco import monthly_tco

    prices = {
        "s3": {
            "storage_gb_month": 0.0,
            "put_per_1000": 0.005,
            "get_per_1000": 0.0004,
            "list_per_1000": 0.005,
        },
        "ec2": {},
        "workload": {"q1": 10000, "q2": 0, "q3": 0, "etl_cycles": 0},
    }
    req = pd.DataFrame(
        [{"method": "m", "query": "q1", "get_per_run": 1000, "head_per_run": 0, "list_per_run": 10}]
    )
    out = monthly_tco(prices, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), requests=req).set_index(
        "method"
    )
    # (1000 x 0.0004 + 10 x 0.005) / 1000 per query x 10,000 a day x 30 days.
    assert abs(out.loc["m", "request_usd"] - 0.00045 * 10000 * 30) < 1e-9
    ten = monthly_tco(prices, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 10.0, requests=req).set_index(
        "method"
    )
    assert abs(ten.loc["m", "request_usd"] - 10 * out.loc["m", "request_usd"]) < 1e-9
