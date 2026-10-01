#!/usr/bin/env bash
# Terminate a job instance and print its lifetime in hours for the cost log.
# Usage: terminate.sh <instance-id>
set -euo pipefail
ID="${1:?instance id}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
LAUNCH=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$ID" \
  --query 'Reservations[0].Instances[0].LaunchTime' --output text)
aws ec2 terminate-instances --region "$REGION" --instance-ids "$ID" >/dev/null
# DEVIATION: pass the timestamp as an argument, so a strange value cannot end the python literal.
python3 -c "
import sys
from datetime import datetime, timezone
t = datetime.fromisoformat(sys.argv[1].replace('Z', '+00:00'))
print(f'{(datetime.now(timezone.utc) - t).total_seconds() / 3600:.2f} hours')
" "$LAUNCH"
