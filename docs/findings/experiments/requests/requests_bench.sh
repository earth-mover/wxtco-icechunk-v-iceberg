# S3 request counts per query (decision 016 follow-up). Each phase owns whole CloudWatch minutes.
set +e
mkdir -p /opt/wxtco/bench
export WXTCO_CHUNK_CACHE_BYTES=0 WXTCO_DUCKDB_FILE_CACHE=0
P1='{"cycle": "2026/09/15/T0000Z", "lat": 51.5, "lon": -0.1}'
P2='{"cycle": "2026/09/15/T0000Z", "lat_max": 59.06, "lat_min": 48.94, "lon_max": 2.25, "lon_min": -8.16}'
P3='{"cycles": ["2026/09/15/T0000Z", "2026/09/15/T0600Z", "2026/09/15/T1200Z", "2026/09/15/T1800Z"], "leads": [0, 6, 12, 24, 48, 72, 96, 120], "vars": ["temperature_at_screen_level", "wind_speed_at_10m", "pressure_at_mean_sea_level", "relative_humidity_at_screen_level"], "samples": 32, "batch": 8}'
align() { local now=$(date +%s); sleep $(( 60 - now % 60 + 1 )); }
phase() { # label method query runs params [env...]
  local label=$1 m=$2 q=$3 runs=$4 p=$5; shift 5
  align; local s=$(date +%s)
  env "$@" uv run wxtco bench --method $m --query $q --params "$p" --runs $runs --out /opt/wxtco/bench/req-$label-$q-r$runs.csv > /tmp/phase.out 2>&1 || echo "FAIL $label $q $runs"
  tail -1 /tmp/phase.out
  align; local e=$(date +%s)
  echo "PHASE $label $q $runs $s $e"
}
echo "== commit $WXTCO_GIT_SHA"
align; s=$(date +%s); sleep 180; align; e=$(date +%s); echo "PHASE idle idle 0 $s $e"
TBL="WXTCO_TABLE_ID=mogreps.surface_hilbert WXTCO_TABLE_SORT=hilbert"
for spec in "native_ts native_ts" "native_dl native_dl" "virtual virtual" "download download" "table_hilbert table"; do
  set -- $spec; label=$1 m=$2; e="WXTCO_NOOP=1"; [ "$label" = table_hilbert ] && e="$TBL"
  phase $label $m q1 1 "$P1" $e
  phase $label $m q1 6 "$P1" $e
  [ "$label" = table_hilbert ] && continue  # its Q2 and Q3 phases were already clean in the canonical run
  phase $label $m q2 1 "$P2" $e
  phase $label $m q2 4 "$P2" $e
  phase $label $m q3_stream 1 "$P3" $e
  phase $label $m q3_stream 2 "$P3" $e
done
aws s3 cp /opt/wxtco/bench/ s3://em-tco-mogreps/_logs/bench/requests/ --recursive --only-show-errors
echo REQUESTS_DONE
