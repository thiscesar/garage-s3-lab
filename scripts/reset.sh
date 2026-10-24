#!/usr/bin/env bash
# Derruba o laboratório e apaga dados/ e generated/. Mantém o .env (segredos).
# Só afeta os recursos deste projeto (compose "garage-s3-lab").
set -euo pipefail
cd "$(dirname "$0")/.."

set -x
if [ -f .env ]; then
  docker compose down -v --remove-orphans
else
  { set +x; } 2>/dev/null
  echo "aviso: sem .env, nada a derrubar pelo compose" >&2
  set -x
fi
rm -rf dados generated
