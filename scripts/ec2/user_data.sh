#!/usr/bin/env bash
# Cloud-init for job instances: uv, the repo tarball, the Arraylake token from Secrets Manager.
# launch.sh substitutes ${WXTCO_ORG} and ${WXTCO_CODE_SHA} before it sends this file.
set -euxo pipefail
# DEVIATION: a marker for a failed boot, because READY never appears and the cause is not obvious.
trap 'echo failed > /opt/wxtco/FAILED' ERR
mkdir -p /opt/wxtco
export HOME=/root  # DEVIATION: cloud-init gives no HOME; uv and pip need one.
# DEVIATION: AL2023 has aws-cli v2 preinstalled. `dnf install awscli` adds a v1 that shadows it.
dnf install -y tar gzip
curl -LsSf https://astral.sh/uv/install.sh | sh
ln -sf /root/.local/bin/uv /usr/local/bin/uv
mkdir -p /opt/wxtco/repo && cd /opt/wxtco/repo
# Code arrives as a tarball the controller uploaded; EC2 never needs GitHub credentials.
aws s3 cp "s3://em-tco-mogreps/_code/${WXTCO_CODE_SHA}.tar.gz" /tmp/code.tar.gz --region us-east-1
tar -xzf /tmp/code.tar.gz -C /opt/wxtco/repo
uv sync --frozen
# DEVIATION: IMDSv2 first, IMDSv1 as the fallback, because run-instances can require tokens.
IMDS_TOKEN=$(curl -sf -X PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300" || true)
INSTANCE_TYPE=$(curl -sf -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" \
  http://169.254.169.254/latest/meta-data/instance-type || echo unknown)
set +x  # DEVIATION: xtrace would write the Arraylake token to /var/log/cloud-init-output.log.
TOKEN=$(aws secretsmanager get-secret-value --secret-id wxtco/arraylake-token \
  --query SecretString --output text --region us-east-1)
# Settings read WXTCO_ORG first, then ARRAYLAKE_ORG; keep WXTCO_ORG.
# DEVIATION: single-quote each value. A token can hold `#`, which an unquoted value comments out.
cat > .env <<EOT
ARRAYLAKE_TOKEN='$TOKEN'
WXTCO_ORG='${WXTCO_ORG}'
WXTCO_INSTANCE_TYPE='$INSTANCE_TYPE'
WXTCO_GIT_SHA='${WXTCO_CODE_SHA}'
EOT
unset TOKEN
chmod 600 .env
set -x
# Optional read-only S3 mount for the FUSE query method. Off unless WXTCO_FUSE=1.
# Mounted here, not from an SSM job: mount-s3 forks without setsid, so an SSM-launched daemon dies
# with the command's process group. fstab keeps it across commands. No --cache: reads must hit S3.
# metadata-ttl: the copy is immutable, so `indefinite` avoids a HEAD+LIST per open (docs/infra/fuse-research.md).
if [ "${WXTCO_FUSE}" = "1" ]; then
  dnf install -y mount-s3 || {
    curl -fsSL -o /tmp/mount-s3.rpm https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm
    dnf install -y /tmp/mount-s3.rpm
  }
  mkdir -p /mnt/mogreps
  echo "s3://em-tco-mogreps/ /mnt/mogreps mount-s3 _netdev,nosuid,nodev,nofail,ro,region=us-east-1,max-threads=64,metadata-ttl=${WXTCO_FUSE_TTL} 0 0" >> /etc/fstab
  systemctl daemon-reload
  mount -a
  mountpoint -q /mnt/mogreps && echo ready > /opt/wxtco/FUSE_READY
  # Store keys are relative to the copy prefix, so the backend root is the prefix directory.
  echo "WXTCO_FUSE_ROOT='/mnt/mogreps/netcdf'" >> .env
fi
echo ready > /opt/wxtco/READY
