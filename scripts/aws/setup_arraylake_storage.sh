#!/usr/bin/env bash
# Step 6: IAM role Arraylake assumes for the study bucket, then two bucket configs and a VCAP on org wxtco.
# Needs AdministratorAccess profile (IAM writes) and ARRAYLAKE_TOKEN + ARRAYLAKE_ORG in .env.
set -euo pipefail
cd "$(dirname "$0")/../.."
# .env carries the PowerUser profile; keep the caller's (Administrator) profile for the IAM writes.
CALLER_PROFILE="${AWS_PROFILE:-}"
set -a; . ./.env; set +a
[ -n "$CALLER_PROFILE" ] && export AWS_PROFILE="$CALLER_PROFILE"
ORG="${ARRAYLAKE_ORG:?}"
ROLE=wxtco-arraylake
SECRET_ID=wxtco/arraylake-external-id
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

# Shared secret (sts:ExternalId). Created once, kept in Secrets Manager so the role can be rebuilt.
if ! EXTERNAL_ID=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ID" --query SecretString --output text 2>/dev/null); then
  EXTERNAL_ID=$(python3 -c "import secrets;print(secrets.token_urlsafe(32))")
  aws secretsmanager create-secret --name "$SECRET_ID" --secret-string "$EXTERNAL_ID" --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing >/dev/null
  echo "created secret $SECRET_ID"
fi
TRUST=$(sed "s|\${EXTERNAL_ID}|$EXTERNAL_ID|" scripts/aws/trust_policy_wxtco_arraylake.json.tmpl)

aws iam get-policy --policy-arn "arn:aws:iam::$ACCOUNT:policy/$ROLE" >/dev/null 2>&1 || \
  aws iam create-policy --policy-name "$ROLE" --policy-document file://scripts/aws/iam_policy_wxtco_arraylake.json --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing >/dev/null
aws iam get-role --role-name "$ROLE" >/dev/null 2>&1 || \
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing >/dev/null
aws iam update-assume-role-policy --role-name "$ROLE" --policy-document "$TRUST"
aws iam attach-role-policy --role-name "$ROLE" --policy-arn "arn:aws:iam::$ACCOUNT:policy/$ROLE"
echo "role arn:aws:iam::$ACCOUNT:role/$ROLE ready"

# Bucket configs: repo metadata + Iceberg warehouse under arraylake/, NetCDF copy under netcdf/ as the virtual chunk source.
uv run python - "$ORG" "$ACCOUNT" "$ROLE" "$EXTERNAL_ID" <<'PY'
import sys
from arraylake import Client

org, account, role, external_id = sys.argv[1:]
client = Client()
have = {b.nickname: b for b in client.list_bucket_configs(org)}
auth = {"method": "aws_customer_managed_role", "external_customer_id": account, "external_role_name": role, "shared_secret": external_id}
for nickname, uri in [("wxtco-storage", "s3://em-tco-mogreps/arraylake"), ("wxtco-netcdf", "s3://em-tco-mogreps/netcdf")]:
    if nickname in have:
        print(f"bucket config {nickname} exists")
        continue
    client.create_bucket_config(org=org, nickname=nickname, uri=uri, region_name="us-east-1", auth_config=auth)
    print(f"created bucket config {nickname} -> {uri}")
if not any(b.is_default for b in client.list_bucket_configs(org)):
    client.set_default_bucket_config(org, "wxtco-storage")
    print("wxtco-storage is the org default")
try:
    client.set_virtual_chunk_access_policy(org, bucket_nickname="wxtco-netcdf", subprefix="", public=False)
    print("VCAP set on wxtco-netcdf")
except Exception as exc:  # needs org admin
    print(f"VCAP not set: {exc}")
PY
