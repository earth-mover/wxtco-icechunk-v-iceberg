#!/usr/bin/env bash
# Run a command on the instance via SSM, detached, logging to S3.
# Usage: run_job.sh <instance-id> <name> -- <command...>
set -euo pipefail
ID="${1:?instance id}"; NAME="${2:?job name}"; shift 2
# DEVIATION: `[ "$1" = "--" ] && shift` exits under `set -e`/`set -u` when it is absent.
if [ "${1:-}" = "--" ]; then shift; fi
[ "$#" -gt 0 ] || { echo "usage: run_job.sh <instance-id> <name> -- <command...>" >&2; exit 2; }
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
LOG="/opt/wxtco/$NAME.log"
S3_LOG="s3://${WXTCO_BUCKET:-em-tco-mogreps}/_logs/$NAME.log"
# DEVIATION: shlex.join keeps the argument boundaries the caller gave us.
JOB=$(python3 -c 'import shlex,sys; print(shlex.join(sys.argv[1:]))' "$@")
# DEVIATION: no `nohup`. SSM waits for the command and kills the process group when the document
# returns, so the job stays in the foreground. The session may die; the job does not.
# DEVIATION: `set -e` for the setup, so a missing `.env` or a failed `cd` aborts before the job
# runs without credentials. HOME and UV_CACHE_DIR are set, because SSM gives a bare environment.
# DEVIATION: `set +e` before the job, so the log upload always happens and the status survives.
CMD="set -e; export HOME=/root UV_CACHE_DIR=/root/.cache/uv
cd /opt/wxtco/repo
set -a; . ./.env; set +a
export AWS_DEFAULT_REGION=$REGION
set +e
$JOB > $LOG 2>&1
status=\$?
aws s3 cp $LOG $S3_LOG --region $REGION --only-show-errors
exit \$status"
# DEVIATION: JSON-encode the parameters, so quotes in the command cannot break the CLI argument.
PARAMS=$(python3 -c 'import json,sys; print(json.dumps({"commands":[sys.argv[1]],"executionTimeout":["172800"]}))' "$CMD")
aws ssm send-command --region "$REGION" --instance-ids "$ID" --document-name AWS-RunShellScript \
  --parameters "$PARAMS" \
  --query Command.CommandId --output text
echo "log: $S3_LOG (uploaded when the job ends)" >&2
