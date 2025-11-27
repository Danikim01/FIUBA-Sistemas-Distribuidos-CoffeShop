#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="docker compose"

KEEP_SERVICES=(
  rabbitmq
  gateway
  healthchecker-1
)

is_client_service() {
  [[ $1 == client-* ]]
}

is_protected_service() {
  local svc=$1
  if is_client_service "$svc"; then
    return 0
  fi
  for keep in "${KEEP_SERVICES[@]}"; do
    [[ $svc == "$keep" ]] && return 0
  done
  return 1
}

running_services=()
while IFS= read -r svc; do
  [[ -z $svc ]] && continue
  running_services+=("$svc")
done < <($COMPOSE_CMD ps --services --filter status=running)

to_kill=()
for svc in "${running_services[@]}"; do
  if ! is_protected_service "$svc"; then
    to_kill+=("$svc")
  fi
done

if [[ ${#to_kill[@]} -gt 0 ]]; then
  $COMPOSE_CMD kill "${to_kill[@]}"
else
  echo "No running services detected."
fi
