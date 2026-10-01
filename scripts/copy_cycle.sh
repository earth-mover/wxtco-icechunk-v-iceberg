#!/usr/bin/env bash
# Copy one MOGREPS-G cycle (surface files only) into the static copy bucket.
# Usage: scripts/copy_cycle.sh 2026/09/16/T0000Z
set -euo pipefail
CYCLE="${1:?cycle like 2026/09/16/T0000Z}"
SRC="${WXTCO_SOURCE_URL:-s3://met-office-global-ensemble-model-data/global-ensemble}"
DST="${WXTCO_COPY_URL:-s3://em-tco-mogreps/netcdf}"
aws s3 cp --recursive --only-show-errors --copy-props none --source-region eu-west-2 --region us-east-1 \
  --exclude '*_on_*_levels.nc' "$SRC/$CYCLE/" "$DST/$CYCLE/"
uv run wxtco copy verify --cycle "$CYCLE"
