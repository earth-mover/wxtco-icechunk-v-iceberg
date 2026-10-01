# 010 Query frequency model for TCO

**Status:** DECIDED 2026-09-18 as a baseline. Ryan expects to tweak once data is in place.

TCO needs an assumed workload. Proposed baseline, per day, for a mid-size weather-data team:

| query | frequency | rationale |
|---|---|---|
| Point forecast timeseries + heating degree days | 10,000 | API-style serving |
| Regional ensemble statistics (e.g. CRPS vs analysis) per cycle | 4 (1 per cycle) | verification |
| ML dataloader, one pass over 7 days of data | 1 | training epoch |
| ETL, one cycle | 4 | production cadence |

Report TCO at 1x and 10x this workload so readers can scale.
