"""Cliente S3 manual do laboratório (roteiro §7.2).

Uso (dentro do contêiner `cliente`):
    python /s3.py <nó> put <chave> <valor>
    python /s3.py <nó> get <chave>
    python /s3.py <nó> ls [prefixo]
    python /s3.py <nó> bench [n]        # n PUTs + n GETs de 4 KiB (padrão 20)

<nó> é 1, 2 ou 3: a requisição entra pelo garage<nó>.
Credenciais e faixa de rede vêm das variáveis do .env (env_file do compose).
"""

import os
import statistics
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

BUCKET = "sdlab"


def cliente_s3(no: int):
    prefixo = os.environ.get("REDE_CLIENTES_PREFIXO")
    chave = os.environ.get("AWS_ACCESS_KEY_ID")
    segredo = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not (prefixo and chave and segredo):
        sys.exit("ERRO: REDE_CLIENTES_PREFIXO e credenciais ausentes; rode `make up` (o iniciar.sh grava no .env).")
    return boto3.client(
        "s3",
        endpoint_url=f"http://{prefixo}.{20 + no}:3900",
        region_name="garage",
        aws_access_key_id=chave,
        aws_secret_access_key=segredo,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},  # sem retries
            connect_timeout=1,
            read_timeout=3,
            # Somente o necessário de checksums (comportamento do boto3 anterior à 1.36).
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def cmd_put(s3, chave: str, valor: str) -> None:
    s3.put_object(Bucket=BUCKET, Key=chave, Body=valor.encode())
    print(f"PUT {chave} ok")


def cmd_get(s3, chave: str) -> None:
    resp = s3.get_object(Bucket=BUCKET, Key=chave)
    print(resp["Body"].read().decode(errors="replace"))


def cmd_ls(s3, prefixo: str = "") -> None:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo)
    for obj in resp.get("Contents", []):
        print(f"{obj['Key']}\t{obj['Size']}\t{obj['LastModified'].isoformat()}")
    print(f"({resp.get('KeyCount', 0)} objetos)")


def _resumo(nome: str, tempos: list[float]) -> str:
    ms = sorted(t * 1000 for t in tempos)
    p95 = ms[min(len(ms) - 1, round(0.95 * (len(ms) - 1)))]
    return f"{nome}: n={len(ms)} média={statistics.mean(ms):.2f} ms p50={statistics.median(ms):.2f} ms p95={p95:.2f} ms"


def cmd_bench(s3, n: str = "20") -> None:
    total = int(n)
    corpo = os.urandom(4096)
    puts, gets = [], []
    for i in range(total):
        t0 = time.monotonic()
        s3.put_object(Bucket=BUCKET, Key=f"bench/{i}", Body=corpo)
        puts.append(time.monotonic() - t0)
    for i in range(total):
        t0 = time.monotonic()
        s3.get_object(Bucket=BUCKET, Key=f"bench/{i}")["Body"].read()
        gets.append(time.monotonic() - t0)
    print(_resumo("PUT 4 KiB", puts))
    print(_resumo("GET 4 KiB", gets))


COMANDOS = {"put": (cmd_put, 2), "get": (cmd_get, 1), "ls": (cmd_ls, 0), "bench": (cmd_bench, 0)}


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[0] not in ("1", "2", "3") or argv[1] not in COMANDOS:
        print(__doc__, file=sys.stderr)
        return 2
    funcao, minimo = COMANDOS[argv[1]]
    args = argv[2:]
    if len(args) < minimo:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        funcao(cliente_s3(int(argv[0])), *args)
    except ClientError as e:
        erro = e.response.get("Error", {})
        print(f"ERRO S3 via garage{argv[0]}: {erro.get('Code')}: {erro.get('Message')}", file=sys.stderr)
        return 1
    except BotoCoreError as e:
        print(f"ERRO de conexão via garage{argv[0]}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
