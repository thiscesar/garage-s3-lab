#!/usr/bin/env bash
# Troca o consistency_mode: renderiza a configuração, reinicia os 3 nós e espera /health = 200
#. Script fino: a lógica fica no labctl, que mostra cada ação Docker executada.
# Uso: scripts/modo.sh consistent|degraded|dangerous
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-}" in
  consistent|degraded|dangerous) ;;
  *) echo "uso: $0 consistent|degraded|dangerous" >&2; exit 2 ;;
esac

exec .venv/bin/labctl mode set "$1"
