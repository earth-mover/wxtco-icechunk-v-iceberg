# 001 Which dataset

**Status:** DECIDED 2026-09-18. Ryan: the UK example path in the plan was an error. MOGREPS-G.

## Context

`project_plan.md` names MOGREPS-G but the example path points at the *UK* bucket:
`s3://met-office-uk-ensemble-model-data/uk-ensemble/...`. These are different models.

| | MOGREPS-G (global) | MOGREPS-UK |
|---|---|---|
| bucket | `met-office-global-ensemble-model-data` | `met-office-uk-ensemble-model-data` |
| prefix | `global-ensemble/YYYY/MM/DD/THHMMZ/` | `uk-ensemble/YYYY/MM/DD/THHMMZ/` |
| cycles/day | 4 | more frequent |
| grid | 960 x 1280, 20 km, global | UK domain, 2.2 km |
| members | 18 | 3 per cycle (lagged ensemble) |
| one cycle | 14,020 files, 2.1 TB (surface only: 11,521 files, 232 GB) | 14,330 files, 255 GB |
| retention | ~31 days | shorter |
| existing Earthmover work | `metoffice/mogreps-g` repo, Joe's ingestion code | none |

Measured 2026-09-18 with `aws s3 ls --summarize --no-sign-request --region eu-west-2`.

## Recommendation

MOGREPS-G. It matches the plan text, the existing virtual repo, and Joe's reference code.
Global coverage also makes the "regional ensemble stats" and "ML dataloader" queries realistic.

## Consequences

All later sizing in this register assumes MOGREPS-G surface diagnostics.
