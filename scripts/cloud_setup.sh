#!/usr/bin/env bash
# Cloud environment setup script. Must exit 0. uv ships in the image; awscli is best-effort.
# apt, astral.sh, docker.com are blocked, and pip could not reach PyPI on 2026-09-18.
set -uxo pipefail
uv --version
uv tool install awscli >/tmp/awscli-install.log 2>&1 && echo "awscli installed via uv" || echo "awscli install failed; see /tmp/awscli-install.log; wxtco will use boto3"
command -v aws && aws --version || true
python3 --version
exit 0
