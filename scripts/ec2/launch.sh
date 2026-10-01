#!/usr/bin/env bash
# Upload the committed tree as a tarball, then launch one job instance. Prints the instance id.
# Usage: launch.sh <instance-type> <name>
# Optional: WXTCO_SUBNET_ID, WXTCO_SECURITY_GROUP_ID, WXTCO_VOLUME_GB (default VPC if unset);
#           WXTCO_FUSE=1 [WXTCO_FUSE_TTL=indefinite|minimal] mounts the copy bucket with Mountpoint.
set -euo pipefail
TYPE="${1:?instance type}"; NAME="${2:?name tag}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"  # DEVIATION: the controller policy is us-east-1 only.
SHA=$(git rev-parse --short HEAD)
if [ -n "$(git status --porcelain)" ]; then echo "commit first: working tree is dirty" >&2; exit 1; fi
REPO_ROOT=$(git rev-parse --show-toplevel)
git archive --format=tar.gz -o "/tmp/wxtco-$SHA.tar.gz" HEAD
aws s3 cp "/tmp/wxtco-$SHA.tar.gz" "s3://${WXTCO_BUCKET:-em-tco-mogreps}/_code/$SHA.tar.gz" \
  --region "$REGION" --only-show-errors
AMI=$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)
# DEVIATION: the CLI base64-encodes --user-data itself, so send plain text through a file.
USER_DATA_FILE=$(mktemp /tmp/wxtco-user-data-XXXXXX.sh)
trap 'rm -f "$USER_DATA_FILE" "/tmp/wxtco-$SHA.tar.gz"' EXIT
# WXTCO_FUSE=1 mounts the copy bucket read-only with Mountpoint (FUSE query method); WXTCO_FUSE_TTL is
# its metadata-ttl (`indefinite` default: the copy is immutable; `minimal` revalidates every open).
sed -e "s/\${WXTCO_ORG}/${WXTCO_ORG:-${ARRAYLAKE_ORG:?set WXTCO_ORG or ARRAYLAKE_ORG}}/" \
  -e "s/\${WXTCO_CODE_SHA}/$SHA/" \
  -e "s/\${WXTCO_FUSE}/${WXTCO_FUSE:-0}/" \
  -e "s/\${WXTCO_FUSE_TTL}/${WXTCO_FUSE_TTL:-indefinite}/" \
  "$REPO_ROOT/scripts/ec2/user_data.sh" > "$USER_DATA_FILE"
# DEVIATION: optional subnet and security group; without them EC2 uses the default VPC.
NET_ARGS=()
if [ -n "${WXTCO_SUBNET_ID:-}" ]; then NET_ARGS+=(--subnet-id "$WXTCO_SUBNET_ID"); fi
if [ -n "${WXTCO_SECURITY_GROUP_ID:-}" ]; then NET_ARGS+=(--security-group-ids "$WXTCO_SECURITY_GROUP_ID"); fi
# `project=wxtco` at launch satisfies the aws:RequestTag condition on ec2:RunInstances.
# DEVIATION: tag the gp3 root volume too, so Cost Explorer counts the EBS cost against the project.
aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$TYPE" \
  --iam-instance-profile Name=wxtco-ec2 \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=${WXTCO_VOLUME_GB:-200},VolumeType=gp3}" \
  --user-data "file://$USER_DATA_FILE" \
  "${NET_ARGS[@]+"${NET_ARGS[@]}"}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=wxtco},{Key=BillingCategory,Value=Marketing},{Key=Name,Value=$NAME}]" \
  "ResourceType=volume,Tags=[{Key=project,Value=wxtco},{Key=BillingCategory,Value=Marketing},{Key=Name,Value=$NAME}]" \
  --query 'Instances[0].InstanceId' --output text
