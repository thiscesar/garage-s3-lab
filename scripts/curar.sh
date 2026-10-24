#!/usr/bin/env bash
# Reconecta um nó à rede interna sempre com o IP fixo dele.
# Uso: scripts/curar.sh garage1|garage2|garage3
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}

NO=${1:-}
case "$NO" in
  garage1|garage2|garage3) ;;
  *) echo "uso: $0 garage1|garage2|garage3" >&2; exit 2 ;;
esac
IP=$("$PY" -m lab.config ip-interno "${NO#garage}")

set -x
docker network connect --ip "$IP" t3_interna "$NO"
