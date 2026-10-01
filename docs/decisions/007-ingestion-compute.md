# 007 Ingestion compute and orchestration

**Status:** PROPOSED

## Context

Plan prefers few vendors, ideally AWS only. Joe's virtual ingest runs on Modal in the `uk`
region and is CPU-bound in-region (~0.96 s per file, ~11,521 files per cycle).

## Options

| option | fit |
|---|---|
| EC2, one large instance, sequential cycles | Simplest; mirrors production steady state; cost = instance-hours |
| AWS Batch | Fan-out if one instance is too slow; more setup |
| Lambda | 15 min / 10 GB limits; awkward for HDF5 + Icechunk commits |
| Modal / Coiled | Fast to start but adds a vendor; Ryan prefers AWS |
| Airflow (MWAA) | Heavy for a batch study |

## Recommendation

EC2 in us-east-1, launched by a script in this repo, running a `wxtco ingest <method>
--cycles ...` CLI. One instance type for all methods so compute cost is comparable.
Instance-hours and S3 request counts are the cost inputs. Fall back to AWS Batch only
if wall clock is unacceptable.

## Consequences

Need EC2 quota, an IAM role with S3 + Arraylake token in Secrets Manager, and SSM for
headless access. See `docs/infra/access-checklist.md`.
