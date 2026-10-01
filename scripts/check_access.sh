#!/usr/bin/env bash
# Run the read-only access checks from docs/infra/access-checklist.md.
set -u
PROFILE="${AWS_PROFILE:-default}"
chk() { printf '%-28s' "$1"; shift; if "$@" >/dev/null 2>&1; then echo OK; else echo FAIL; fi; }
chk "aws sso ($PROFILE)" aws sts get-caller-identity --profile "$PROFILE"
chk "source bucket (anon)" aws s3 ls s3://met-office-global-ensemble-model-data/global-ensemble/ --region eu-west-2 --no-sign-request
chk "arraylake token env" test -n "${ARRAYLAKE_TOKEN:-}"
chk "gh auth" gh auth status
chk "uv" uv --version
