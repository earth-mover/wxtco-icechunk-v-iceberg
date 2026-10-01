# 013 Arraylake access to the study bucket

**Status:** DECIDED 2026-09-18 (raised by Ryan: virtual ingest needs a VCAP on the NetCDF prefix and a proper Arraylake storage bucket with credential delegation).

## Decision

One bucket, two prefixes, one delegation role.

- IAM role `wxtco-arraylake` in account <ACCOUNT_ID>, trusted by Earthmover's signer service
  (`arn:aws:iam::<EARTHMOVER_SIGNER_ACCOUNT_ID>:role/EarthmoverSignerServiceRole-production`) with an external ID
  kept in Secrets Manager `wxtco/arraylake-external-id`. Policy: read/write/delete under
  `arraylake/`, read-only under `netcdf/`, list on the bucket.
- Bucket config `wxtco-storage` → `s3://em-tco-mogreps/arraylake`, org default. Holds the Icechunk
  repos (`mogreps-g-virtual`, later `mogreps-g-native`) and the Iceberg warehouse.
- Bucket config `wxtco-netcdf` → `s3://em-tco-mogreps/netcdf`, virtual chunk source, with a
  private virtual chunk access policy (subprefix "", `public=False`).

Auth config per Arraylake docs (`docs/setup/manage-storage`):
`{"method": "aws_customer_managed_role", "external_customer_id": "<ACCOUNT_ID>", "external_role_name": "wxtco-arraylake", "shared_secret": <external id>}`.

## Why one bucket

Storage accounting per method is by prefix (`aws s3 ls --summarize` on `arraylake/<repo prefix>`
and `netcdf/`). A second bucket adds IAM surface with no accounting benefit.

## Consequences

- Plan 03 Task 1 Step 6: `prod_repo` uses `bucket_nickname="wxtco-netcdf"` for the virtual source
  and `storage_nickname=None` (org default `wxtco-storage`). No bucket config is created by code.
- Iceberg tables land under `arraylake/` via the org default bucket config.
- Script: `scripts/aws/setup_arraylake_storage.sh`.
