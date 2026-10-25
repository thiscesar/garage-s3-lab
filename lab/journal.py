"""Journal das ações que mudam o cluster: results/_journal.jsonl.

Fica num módulo próprio para que cluster, garage, CLI, experimentos e painel registrem no mesmo lugar.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from lab.config import RAIZ

DIR_RESULTS = RAIZ / "results"
ARQ_JOURNAL = DIR_RESULTS / "_journal.jsonl"


def agora_iso() -> str:
    return datetime.now(UTC).astimezone().isoformat(timespec="milliseconds")


def relativo(caminho) -> str:
    """Caminho relativo à raiz do projeto quando possível (para o journal e manifests); senão, absoluto."""
    try:
        return str(caminho.relative_to(RAIZ))
    except ValueError:
        return str(caminho)


def registrar(comando: str, args: dict | None = None, saida: object = None) -> None:
    DIR_RESULTS.mkdir(parents=True, exist_ok=True)
    linha = {"ts": agora_iso(), "comando": comando, "args": args or {}, "saida": saida}
    with ARQ_JOURNAL.open("a") as f:
        f.write(json.dumps(linha, ensure_ascii=False, default=str) + "\n")


def ultimas(n: int = 20) -> list[dict]:
    if not ARQ_JOURNAL.exists():
        return []
    linhas = ARQ_JOURNAL.read_text().splitlines()[-n:]
    return [json.loads(linha) for linha in linhas if linha.strip()]
