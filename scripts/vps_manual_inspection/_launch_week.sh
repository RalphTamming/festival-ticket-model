#!/usr/bin/env bash
# Archive any prior week-runner logs and launch the 7-day production run via nohup.
# Designed to be invoked from an interactive ssh session and then forgotten.
set -euo pipefail

# Prefer plain Selenium on VPS where undetected-chromedriver is unstable / segfault-prone.
export TICKETSWAP_DRIVER_IMPL="${TICKETSWAP_DRIVER_IMPL:-selenium}"

cd "${FESTIVAL_TICKET_REPO:-/root/festival-ticket-model}"

mkdir -p logs/archive
ts=$(date -u +%Y%m%dT%H%M%SZ)
for f in logs/week_production_test.log logs/week_production_test.nohup.log; do
    if [ -e "$f" ]; then
        base=$(basename "$f" .log)
        mv "$f" "logs/archive/${base}_${ts}.log"
        echo "archived $f -> logs/archive/${base}_${ts}.log"
    fi
done

# Launch fully detached: nohup + disown + redirected stdio so ssh disconnects
# do not signal the runner.
nohup bash scripts/run_week_production_test.sh \
    > logs/week_production_test.nohup.log 2>&1 < /dev/null &
runner_pid=$!
disown
sleep 2

echo "runner_pid=${runner_pid}"
ps -p "${runner_pid}" -o pid,etime,cmd --no-headers || echo "RUNNER_NOT_RUNNING"

echo
echo "tail of nohup log:"
sleep 3
tail -n 20 logs/week_production_test.nohup.log 2>/dev/null || true
