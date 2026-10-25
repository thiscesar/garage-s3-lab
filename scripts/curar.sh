#!/usr/bin/env bash
# Reconecta um nó à rede interna sempre com o IP fixo dele.
# Script fino: a lógica fica no labctl, que mostra o `docker network connect --ip ...` executado.
# Uso: scripts/curar.sh garage1|garage2|garage3
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  garage1|garage2|garage3) ;;
  *) echo "uso: $0 garage1|garage2|garage3" >&2; exit 2 ;;
esac

exec .venv/bin/labctl net heal "${1#garage}"
