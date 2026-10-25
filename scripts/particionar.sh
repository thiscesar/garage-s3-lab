#!/usr/bin/env bash
# Isola um nó da rede interna (RPC); ele continua acessível pela rede clientes.
# Script fino: a lógica fica no labctl, que mostra o `docker network disconnect` executado.
# Uso: scripts/particionar.sh garage1|garage2|garage3
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  garage1|garage2|garage3) ;;
  *) echo "uso: $0 garage1|garage2|garage3" >&2; exit 2 ;;
esac

exec .venv/bin/labctl net partition "${1#garage}"
