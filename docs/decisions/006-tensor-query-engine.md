# 006 Tensor query engine

**Status:** PROPOSED

## Recommendation

- **Xarray on Icechunk** for all three query patterns. Dask only for the regional ensemble
  statistics query if a single process is too slow.
- Same code path for native (method 2) and virtual (method 3) repos; only the repo differs.
- **Zax SQL** as a stretch: same SQL text against tensors and tables would be a strong
  blog moment. Flux SQL could not read virtual chunks on 2026-09-13; Ryan reports this is fixed
  upstream (2026-09-18). Verify against the new org before relying on it.

## Consequences

Query library exposes each query as a function taking a "backend" object; backends are
`iceberg-duckdb`, `icechunk-native`, `icechunk-virtual`.
