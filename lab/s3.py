"""Clientes S3 (boto3) por nó, sem retries e com timeouts curtos.

Um cliente boto3 não deve ser compartilhado entre threads: use `cliente_s3()` uma vez por thread.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from lab.config import Topologia, carregar_env

BUCKET = "sdlab"


def config_cliente(connect_timeout: float = 1, read_timeout: float = 3) -> Config:
    """Configuração comum dos clientes S3 do laboratório: uma única tentativa por operação.

    No botocore, `max_attempts` conta as *novas* tentativas (`max_attempts: 1` = 2 envios); "sem retries" é
    `total_max_attempts: 1` (botocore/config.py, docstring de `retries`).
    """
    return Config(
        s3={"addressing_style": "path"},
        retries={"total_max_attempts": 1, "mode": "standard"},  # sem retries
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        # Somente o necessário de checksums (comportamento do boto3 anterior à 1.36)
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )


def cliente_s3(
    n: int,
    env: dict[str, str] | None = None,
    connect_timeout: float = 1,
    read_timeout: float = 3,
    credenciais: tuple[str, str] | None = None,
):
    """Cliente boto3 que entra pelo garage<n>; credenciais da sd-key (.env), salvo se `credenciais` for dado."""
    env = carregar_env() if env is None else env
    chave, segredo = credenciais or (env.get("AWS_ACCESS_KEY_ID"), env.get("AWS_SECRET_ACCESS_KEY"))
    if not (chave and segredo):
        raise RuntimeError("credenciais S3 vazias no .env: rode `make up` (o iniciar.sh as grava)")
    return boto3.client(
        "s3",
        endpoint_url=f"http://{Topologia.do_env(env).ip_clientes(n)}:3900",
        region_name="garage",
        aws_access_key_id=chave,
        aws_secret_access_key=segredo,
        config=config_cliente(connect_timeout, read_timeout),
    )


@dataclass
class Resultado:
    """Uma operação S3 com o tempo medido no relógio monotônico do host."""

    op: str
    no: int
    chave: str
    t_inicio: float
    t_fim: float
    ok: bool
    http: int | None = None
    erro: str | None = None
    dados: object = None

    @property
    def latencia_ms(self) -> float:
        return (self.t_fim - self.t_inicio) * 1000


def _executar(op: str, no: int, chave: str, funcao) -> Resultado:
    t0 = time.monotonic()
    try:
        resp, dados = funcao()
        t1 = time.monotonic()
        http = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return Resultado(op, no, chave, t0, t1, True, http=http, dados=dados)
    except ClientError as e:
        t1 = time.monotonic()
        erro = e.response.get("Error", {})
        http = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return Resultado(op, no, chave, t0, t1, False, http=http, erro=f"{erro.get('Code')}: {erro.get('Message')}")
    except BotoCoreError as e:
        return Resultado(op, no, chave, t0, time.monotonic(), False, erro=f"{type(e).__name__}: {e}")


def put(s3, no: int, chave: str, corpo: bytes, metadados: dict[str, str] | None = None) -> Resultado:
    return _executar(
        "PUT", no, chave, lambda: (s3.put_object(Bucket=BUCKET, Key=chave, Body=corpo, Metadata=metadados or {}), None)
    )


def get(s3, no: int, chave: str) -> Resultado:
    def f():
        resp = s3.get_object(Bucket=BUCKET, Key=chave)
        return resp, resp["Body"].read()

    return _executar("GET", no, chave, f)


def listar(s3, no: int, prefixo: str = "") -> Resultado:
    def f():
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo)
        return resp, [o["Key"] for o in resp.get("Contents", [])]

    return _executar("LIST", no, prefixo, f)


def apagar(s3, no: int, chave: str) -> Resultado:
    return _executar("DELETE", no, chave, lambda: (s3.delete_object(Bucket=BUCKET, Key=chave), None))
