#!/usr/bin/env bash
# Create the static copy bucket in the Earthmover Sandbox account. Idempotent.
set -euo pipefail
BUCKET="${WXTCO_BUCKET:-em-tco-mogreps}"
REGION="${WXTCO_COPY_REGION:-us-east-1}"
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "bucket $BUCKET exists"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
fi
aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging 'TagSet=[{Key=project,Value=wxtco},{Key=BillingCategory,Value=Marketing}]'
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# Request metrics let CloudWatch count GET/PUT for the cost model.
aws s3api put-bucket-metrics-configuration --bucket "$BUCKET" --id wxtco-all \
  --metrics-configuration '{"Id":"wxtco-all"}'
echo "ok: s3://$BUCKET in $REGION"
