#!/usr/bin/env bash
# Isola um nó da rede interna (RPC); ele continua acessível pela rede clientes.
# Uso: scripts/particionar.sh garage1|garage2|garage3
set -euo pipefail
cd "$(dirname "$0")/.."

NO=${1:-}
case "$NO" in
  garage1|garage2|garage3) ;;
  *) echo "uso: $0 garage1|garage2|garage3" >&2; exit 2 ;;
esac

set -x
docker network disconnect t3_interna "$NO"
