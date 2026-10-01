# 005 Table query engine

**Status:** PROPOSED

## Options from the plan

Spark/EMR, Snowflake, DuckDB, Zax SQL.

## Recommendation

- **DuckDB** (Iceberg extension against the Arraylake REST catalog) as the primary engine:
  reproducible by anyone, no vendor, per-query cost = instance time.
- **Athena** as secondary: AWS-native, serverless, priced per TB scanned, which gives a clean
  TCO number and matches the "AWS only" preference.
- **Snowflake** as stretch, for the partner audience, if credits are available.
- Spark/EMR only if DuckDB cannot complete the regional or ML queries at scale.

## Consequences

Iceberg catalog: Arraylake's (see 008). Verify DuckDB can read it before the design is final.
