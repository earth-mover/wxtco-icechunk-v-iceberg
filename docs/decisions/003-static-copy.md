# 003 Static copy of source NetCDFs

**Status:** PROPOSED

## Context

Source files are deleted after ~31 days, whole cycles at a time. The virtual method (3)
points at the source files, so without a copy the result rots within a month and cannot
be reproduced. Source is in eu-west-2; Earthmover runs in us-east-1.

## Recommendation

Copy the study subset once to `s3://<earthmover-bucket>/mogreps-g/netcdf/<cycle>/...`
in us-east-1, preserving the Met Office key layout under the cycle prefix. All three
methods ingest from the copy. Use `aws s3 sync` or S3 Batch Operations from an EC2
instance in eu-west-2 (cheapest egress path is still cross-region at $0.02/GB).

Rough cost for 6.5 TB: one-time transfer ~$130; storage ~$150/month S3 Standard.
The copy is the "with original NetCDFs" storage variant in the TCO table.

## Consequences

- Region for all compute: us-east-1.
- The copy bucket needs a lifecycle rule or a manual delete at project end.
- Record `aws s3 ls --summarize` of the copy as the ground-truth storage number.
