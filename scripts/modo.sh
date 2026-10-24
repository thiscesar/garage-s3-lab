#!/usr/bin/env bash
# Troca o consistency_mode: renderiza a configuração e reinicia os 3 nós.
# Uso: scripts/modo.sh consistent|degraded|dangerous
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}

MODO=${1:-}
case "$MODO" in
  consistent|degraded|dangerous) ;;
  *) echo "uso: $0 consistent|degraded|dangerous" >&2; exit 2 ;;
esac

set -x
"$PY" -m lab.config render --mode "$MODO"
docker compose restart garage1 garage2 garage3
{ set +x; } 2>/dev/null

# Espera GET /health = 200 em cada nó (200 = há quórum; 503 = não há)
for n in 1 2 3; do
  url="http://$("$PY" -m lab.config ip-clientes "$n"):3903/health"
  for ((i = 0; i < 60; i++)); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url")" = 200 ] && break
    sleep 1
  done
  codigo=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url" || true)
  if [ "$codigo" != 200 ]; then
    echo "ERRO: garage$n não respondeu 200 em $url (último código: $codigo)" >&2
    exit 1
  fi
  echo "garage$n: /health 200"
done
echo "modo atual: $(cat generated/MODE)"
