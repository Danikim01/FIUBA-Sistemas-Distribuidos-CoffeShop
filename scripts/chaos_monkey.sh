#!/usr/bin/env bash

set -euo pipefail

COMPOSE_CMD=${COMPOSE_CMD:-"docker compose"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTERVAL_MIN=5
INTERVAL_MAX=15
DOWNTIME_MIN=3
DOWNTIME_MAX=10
STRIKE_COUNT=0   # 0 means run forever

# By default we target the same worker categories as kill_random_workers.sh.
INCLUDE_SHARDED=true
INCLUDE_FILTER=true
INCLUDE_EOF_BARRIER=false
INCLUDE_ROUTERS=false
INCLUDE_AGGREGATORS=false

function usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Chaos Monkey that randomly kills then restarts worker containers.

Options:
  -i, --interval-range MIN MAX    Seconds to wait between strikes (default: 5 15)
  --downtime-range MIN MAX        Seconds the worker stays down (default: 3 10)
  -c, --count N                   Number of strikes before exiting (default: 0 = infinite)
  -s, --sharded                   Include sharded workers
  -f, --filter                    Include filter workers
  -e, --eof-barrier              Include EOF barrier service
  -r, --routers                   Include sharding routers
  -g, --aggregators               Include aggregators
  -a, --all                       Include all worker categories
  -h, --help                      Show this help message

Environment variables:
  COMPOSE_CMD                    Docker compose command (default: 'docker compose')

Examples:
  $0                       # Chaos on sharded + filter workers
  $0 -a                    # Chaos across every worker type
  $0 -c 5 --downtime-range 1 5
EOF
}

function random_float() {
  local min=$1
  local max=$2
  python3 - "$min" "$max" <<'PY'
import random, sys
min_val = float(sys.argv[1])
max_val = float(sys.argv[2])
if max_val <= min_val:
    print(f"{min_val:.2f}")
else:
    print(f"{random.uniform(min_val, max_val):.2f}")
PY
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -i|--interval-range)
      INTERVAL_MIN=$2
      INTERVAL_MAX=$3
      shift 3
      ;;
    --downtime-range)
      DOWNTIME_MIN=$2
      DOWNTIME_MAX=$3
      shift 3
      ;;
    -c|--count)
      STRIKE_COUNT=$2
      shift 2
      ;;
    -s|--sharded)
      INCLUDE_SHARDED=true
      shift
      ;;
    -f|--filter)
      INCLUDE_FILTER=true
      shift
      ;;
    -e|--eof-barrier)
      INCLUDE_EOF_BARRIER=true
      shift
      ;;
    -r|--routers)
      INCLUDE_ROUTERS=true
      shift
      ;;
    -g|--aggregators)
      INCLUDE_AGGREGATORS=true
      shift
      ;;
    -a|--all)
      INCLUDE_SHARDED=true
      INCLUDE_FILTER=true
      INCLUDE_EOF_BARRIER=true
      INCLUDE_ROUTERS=true
      INCLUDE_AGGREGATORS=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if (( $(awk "BEGIN {print ($INTERVAL_MAX >= $INTERVAL_MIN)}") == 0 )); then
  echo "Interval max must be greater than or equal to min." >&2
  exit 1
fi

if (( $(awk "BEGIN {print ($DOWNTIME_MAX >= $DOWNTIME_MIN)}") == 0 )); then
  echo "Downtime max must be greater than or equal to min." >&2
  exit 1
fi

if ! [[ $STRIKE_COUNT =~ ^[0-9]+$ ]]; then
  echo "Strike count must be a non-negative integer." >&2
  exit 1
fi

WORKER_PATTERNS=()
[[ "$INCLUDE_SHARDED" == "true" ]] && WORKER_PATTERNS+=("*-worker-sharded-*")
[[ "$INCLUDE_FILTER" == "true" ]] && WORKER_PATTERNS+=("*-filter-worker-*")
[[ "$INCLUDE_EOF_BARRIER" == "true" ]] && WORKER_PATTERNS+=("*-eof-barrier")
[[ "$INCLUDE_ROUTERS" == "true" ]] && WORKER_PATTERNS+=("*-sharding-router")
[[ "$INCLUDE_AGGREGATORS" == "true" ]] && WORKER_PATTERNS+=("*-aggregator")

if [[ ${#WORKER_PATTERNS[@]} -eq 0 ]]; then
  echo "No worker categories enabled. Nothing to do." >&2
  exit 1
fi

choose_running_worker() {
  local running=()
  while IFS= read -r svc; do
    [[ -z $svc ]] && continue
    running+=("$svc")
  done < <($COMPOSE_CMD ps --services --filter status=running)

  local candidates=()
  for svc in "${running[@]}"; do
    for pattern in "${WORKER_PATTERNS[@]}"; do
      if [[ $svc == $pattern ]]; then
        candidates+=("$svc")
        break
      fi
    done
  done

  if [[ ${#candidates[@]} -eq 0 ]]; then
    echo ""
  else
    echo "${candidates[RANDOM % ${#candidates[@]}]}"
  fi
}

kill_worker() {
  local target=$1
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing $target"
  $COMPOSE_CMD kill "$target"
}

strike=0
while :; do
  if [[ $STRIKE_COUNT -ne 0 && $strike -ge $STRIKE_COUNT ]]; then
    echo "Completed requested $STRIKE_COUNT strikes."
    break
  fi

  interval=$(random_float "$INTERVAL_MIN" "$INTERVAL_MAX")
  echo "Waiting $interval seconds until next strike."
  sleep "$interval"

  worker=$(choose_running_worker)
  if [[ -z $worker ]]; then
    echo "No running workers matched; retrying."
    continue
  fi

  # Occasionally trigger full catastrophe instead of a single worker kill
  if (( RANDOM % 20 == 0 )); then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Chaos Monkey invoking catastrophe.sh!"
    "$SCRIPT_DIR/catastrophe.sh"
  else
    kill_worker "$worker"
  fi

  strike=$((strike + 1))
done
