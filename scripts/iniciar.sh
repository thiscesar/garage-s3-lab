#!/usr/bin/env bash
# Monta o cluster (roteiro §7.2): conecta os nós, grava os bootstrap_peers,
# aplica o layout com uma zona por nó, cria o bucket sdlab e a chave sd-key, grava as credenciais
# no .env e recria o cliente.
# Idempotente: o que já existe não é refeito.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}

garage() { docker exec "$1" /garage "${@:2}" 2>/dev/null; }   # stderr da CLI só traz logs INFO

esperar() {   # esperar <descrição> <segundos> <comando...>
  local desc=$1 limite=$2; shift 2
  for ((i = 0; i < limite; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "ERRO: tempo esgotado esperando $desc" >&2
  return 1
}

saudavel() { [ "$(docker inspect -f '{{.State.Health.Status}}' "$1")" = healthy ]; }
cluster_completo() {
  local s; s=$(garage garage1 status)
  for n in 1 2 3; do grep -q "$("$PY" -m lab.config ip-interno "$n"):3901" <<<"$s" || return 1; done
}

for n in 1 2 3; do esperar "garage$n saudável" 60 saudavel "garage$n"; done

set -x
# 1. Conectar garage1 aos outros dois (formato <id>@<ip>:<porta>, dado por `node id -q`)
ID1=$(garage garage1 node id -q)
ID2=$(garage garage2 node id -q)
ID3=$(garage garage3 node id -q)
garage garage1 node connect "$ID2"
garage garage1 node connect "$ID3"
{ set +x; } 2>/dev/null
esperar "os 3 nós no garage status" 30 cluster_completo
set -x

# 1b. bootstrap_peers: grava os 3 IDs e re-renderiza mantendo o modo atual.
# Os nós em execução já estão conectados; a config nova vale a partir do próximo restart,
# para que todo restart reconecte sozinho (a peer_list do disco só é gravada a cada 60 s).
"$PY" -m lab.config definir-pares "$ID1" "$ID2" "$ID3"
"$PY" -m lab.config render --mode "$(cat generated/MODE)"

# 2. Layout: uma zona por nó, 1 GB cada; aplica só se ainda não houver layout (versão 0)
VERSAO=$(garage garage1 layout show | sed -n 's/^Current cluster layout version: //p')
if [ "$VERSAO" = 0 ]; then
  garage garage1 layout assign -z dz1 -c 1G "${ID1%%@*}"
  garage garage1 layout assign -z dz2 -c 1G "${ID2%%@*}"
  garage garage1 layout assign -z dz3 -c 1G "${ID3%%@*}"
  garage garage1 layout apply --version $((VERSAO + 1))
fi

# 3. Bucket e chave
garage garage1 bucket info sdlab >/dev/null || garage garage1 bucket create sdlab
garage garage1 key info sd-key >/dev/null || garage garage1 key create sd-key
garage garage1 bucket allow --read --write --owner sdlab --key sd-key
{ set +x; } 2>/dev/null

# 4. Credenciais no .env (sem eco do segredo) e cliente recriado para lê-las
INFO=$(garage garage1 key info --show-secret sd-key)
"$PY" -m lab.config definir AWS_ACCESS_KEY_ID "$(sed -n 's/^Key ID: *//p' <<<"$INFO")"
"$PY" -m lab.config definir AWS_SECRET_ACCESS_KEY "$(sed -n 's/^Secret key: *//p' <<<"$INFO")"
echo "+ credenciais da sd-key gravadas no .env"
set -x
docker compose up -d --force-recreate cliente
{ set +x; } 2>/dev/null

echo "cluster pronto: layout versão $(garage garage1 layout show | sed -n 's/^Current cluster layout version: //p'), bucket sdlab, chave sd-key"
