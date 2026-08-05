#!/usr/bin/env bash
# Headless Locust run: warm-up phase (discarded), then the measured run.
#
# Standalone:        scripts/run_headless.sh
# Distributed:       LOCUST_MODE=master scripts/run_headless.sh     # on the master
#                    LOCUST_MODE=worker MASTER_HOST=10.0.0.5 scripts/run_headless.sh
# Workers must reach the master on TCP 5557. Run one worker per vCPU;
# set EXPECT_WORKERS on the master to that count.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo ".env missing — copy .env.example and fill it in" >&2; exit 1; }
set -a; source .env; set +a

LOCUST=".venv/bin/locust"
[ -x "$LOCUST" ] || LOCUST=locust
MODE="${LOCUST_MODE:-standalone}"
WARMUP="${WARMUP_TIME:-60s}"
CSV_PREFIX="${CSV_PREFIX:-results}"

soft_limit=$(ulimit -n)
if [ "$soft_limit" != "unlimited" ] && [ "$soft_limit" -lt 8192 ]; then
  echo "WARNING: ulimit -n is $soft_limit; raise it (scripts/ec2_setup.sh) or connections will fail" >&2
fi

case "$MODE" in
  worker)
    exec "$LOCUST" -f locustfile.py --worker --master-host "${MASTER_HOST:?set MASTER_HOST}"
    ;;
  master)
    COMMON=(--master --expect-workers "${EXPECT_WORKERS:?set EXPECT_WORKERS (one worker per vCPU)}")
    ;;
  standalone)
    COMMON=()
    ;;
  *) echo "LOCUST_MODE must be standalone|master|worker" >&2; exit 1 ;;
esac

# Warm-up: first-touch page faults, TLS handshakes, and connection-pool
# growth stay out of the reported percentiles.
echo "== warm-up ${WARMUP} (results discarded)"
"$LOCUST" -f locustfile.py --headless "${COMMON[@]}" \
  -u "${LOCUST_USERS:-200}" -r "${LOCUST_SPAWN_RATE:-20}" -t "$WARMUP" \
  --only-summary

echo "== measured run ${LOCUST_RUN_TIME:-10m} -> ${CSV_PREFIX}_*.csv + ${CSV_PREFIX}_report.html"
"$LOCUST" -f locustfile.py --headless "${COMMON[@]}" \
  -u "${LOCUST_USERS:-200}" -r "${LOCUST_SPAWN_RATE:-20}" -t "${LOCUST_RUN_TIME:-10m}" \
  --csv "$CSV_PREFIX" --csv-full-history --html "${CSV_PREFIX}_report.html"

echo "== report"
.venv/bin/python scripts/report.py --csv-prefix "$CSV_PREFIX" || \
  python3 scripts/report.py --csv-prefix "$CSV_PREFIX"
