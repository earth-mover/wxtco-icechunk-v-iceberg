#!/usr/bin/env bash
# Apply project=wxtco and BillingCategory=Marketing to every existing project resource. Idempotent.
# Needs the PowerUser profile: the controller user cannot tag S3, IAM or Secrets Manager.
set -euo pipefail
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
TAGS="Key=project,Value=wxtco Key=BillingCategory,Value=Marketing"
BUCKET="${WXTCO_BUCKET:-em-tco-mogreps}"

# S3: put-bucket-tagging replaces the whole set, so both tags go in together.
aws s3api put-bucket-tagging --bucket "$BUCKET" \
  --tagging 'TagSet=[{Key=project,Value=wxtco},{Key=BillingCategory,Value=Marketing}]'
echo "tagged bucket $BUCKET"

aws secretsmanager tag-resource --region "$REGION" --secret-id wxtco/arraylake-token --tags $TAGS
echo "tagged secret wxtco/arraylake-token"

for role in wxtco-ec2 wxtco-arraylake; do
  aws iam tag-role --role-name "$role" --tags $TAGS && echo "tagged role $role"
done
aws iam tag-instance-profile --instance-profile-name wxtco-ec2 --tags $TAGS && echo "tagged instance profile wxtco-ec2"
aws iam tag-user --user-name wxtco-controller --tags $TAGS && echo "tagged user wxtco-controller"
for policy in wxtco-ec2 wxtco-arraylake wxtco-controller; do
  aws iam tag-policy --policy-arn "arn:aws:iam::$ACCOUNT:policy/$policy" --tags $TAGS && echo "tagged policy $policy"
done

# EC2: anything alive with the project tag (instances and their volumes).
IDS=$(aws ec2 describe-instances --region "$REGION" --filters Name=tag:project,Values=wxtco \
  Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
VOLS=$(aws ec2 describe-volumes --region "$REGION" --filters Name=tag:project,Values=wxtco --query 'Volumes[].VolumeId' --output text)
if [ -n "$IDS$VOLS" ]; then
  aws ec2 create-tags --region "$REGION" --resources $IDS $VOLS --tags $TAGS
  echo "tagged ec2: $IDS $VOLS"
else
  echo "no live ec2 instances or volumes"
fi
echo DONE
