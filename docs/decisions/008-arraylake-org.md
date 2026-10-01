# 008 Arraylake org for the study

**Status:** DECIDED 2026-09-18

## Context

Iceberg catalog is behind the `iceberg` feature flag. Via MCP, Ryan's account sees only
`vandelay-industries`, which has `iceberg` and `sql-preview`. `metoffice/mogreps-g` is
readable but must not be reused per the plan.

## Decision

Ryan: "We can create a standalone Arraylake org and API tokens for this project. Don't
overindex on the current state." Create the org and a project-scoped `ema_` token when
session 002 starts; store the token in `.env` (gitignored) and, later, in AWS Secrets Manager.

## Original recommendation

Create a dedicated org (for example `earthmover-tco`) with the `iceberg` flag, a bucket
config pointing at our us-east-1 bucket, and a public VCAP for the static-copy bucket.
Clean org = clean storage accounting.

## Consequences

Needs an Earthmover admin to create the org and set flags. Record org name in `.env.example`.
