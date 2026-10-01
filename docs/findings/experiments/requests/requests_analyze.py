"""Per-query S3 request counts from CloudWatch, for minute-aligned phases."""
import json, subprocess, sys
import pandas as pd

METRICS = {"GetRequests": "get", "HeadRequests": "head", "ListRequests": "list", "PutRequests": "put"}

def cw(metric, start, end):
    out = subprocess.run(["aws", "cloudwatch", "get-metric-statistics", "--region", "us-east-1", "--namespace", "AWS/S3",
        "--metric-name", metric, "--dimensions", "Name=BucketName,Value=em-tco-mogreps", "Name=FilterId,Value=wxtco-all",
        "--start-time", start, "--end-time", end, "--period", "60", "--statistics", "Sum", "--output", "json"],
        capture_output=True, text=True, check=True).stdout
    return {int(d["Timestamp"]) // 60 * 60: d["Sum"] for d in json.loads(out)["Datapoints"]}

def counts(series, s, e):
    """Sum the whole minutes in [s, e)."""
    m0, m1 = (s + 59) // 60 * 60 - 60 if s % 60 else s, e // 60 * 60
    return sum(v for t, v in series.items() if m0 <= t < m1), (m1 - m0) // 60

phases = [l.split()[1:] for l in open(sys.argv[1]) if l.startswith("PHASE")]
t0 = pd.Timestamp(int(phases[0][3]) - 120, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
t1 = pd.Timestamp(int(phases[-1][4]) + 120, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
series = {k: cw(m, t0, t1) for m, k in METRICS.items()}
rows = []
for label, q, runs, s, e in phases:
    s, e, runs = int(s), int(e), int(runs)
    rec = dict(label=label, query=q, runs=runs)
    for k, ser in series.items():
        rec[k], rec["minutes"] = counts(ser, s, e)
    rows.append(rec)
t = pd.DataFrame(rows)
idle = t[t.label == "idle"].iloc[0]
for k in METRICS.values():
    t[k + "_net"] = t[k] - idle[k] / idle["minutes"] * t["minutes"]
print(t.round(0).to_string(index=False))
out = []
for (label, q), g in t[t.label != "idle"].groupby(["label", "query"], sort=False):
    one, many = g[g.runs == 1].iloc[0], g[g.runs > 1].iloc[0]
    rec = dict(method=label, query=q)
    for k in METRICS.values():
        per = (many[k + "_net"] - one[k + "_net"]) / (many.runs - 1)
        rec[k + "_per_run"] = max(per, 0)
        rec[k + "_open"] = max(one[k + "_net"] - per, 0)
    out.append(rec)
o = pd.DataFrame(out)
print(o.round(1).to_string(index=False))
o.to_csv(sys.argv[2], index=False)
