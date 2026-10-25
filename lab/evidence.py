"""Evidências e journal (roteiro R3: evidência de cada experimento).

- Evidência: saída de comando gravada com cabeçalho (comando, horário de início, duração).
- Snapshot: retrato completo do cluster (estado, saúde, layout, estatísticas, workers, logs, disco, memória).
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

from lab import cluster, garage, journal
from lab.config import NOS, RAIZ, modo_atual
from lab.journal import DIR_RESULTS, agora_iso


def salvar(destino: Path, nome: str, comando: str, coletar: Callable[[], object]) -> Path:
    """Executa `coletar()` e grava o resultado com cabeçalho. Exceções viram texto no próprio arquivo."""
    destino.mkdir(parents=True, exist_ok=True)
    inicio_iso, t0 = agora_iso(), time.monotonic()
    try:
        conteudo = coletar()
        if not isinstance(conteudo, str):
            conteudo = json.dumps(conteudo, indent=2, ensure_ascii=False, default=str)
        situacao = "ok"
    except Exception as e:  # noqa: BLE001 — evidência de falha também é evidência
        conteudo, situacao = f"{type(e).__name__}: {e}", "erro"
    duracao = time.monotonic() - t0
    arq = destino / nome
    arq.write_text(
        f"# comando: {comando}\n# início: {inicio_iso}\n# duração: {duracao:.3f} s\n# situação: {situacao}\n\n"
        + conteudo.rstrip("\n")
        + "\n"
    )
    return arq


def _du_dados() -> str:
    alvos = [str(RAIZ / "dados" / f"g{n}" / "data") for n in NOS]
    return subprocess.run(["du", "-sb", *alvos], capture_output=True, text=True, check=True).stdout


def _memoria() -> dict:
    return {s: cluster.memoria(s) for s in cluster.SERVICOS}


def snapshot(rotulo: str, base: Path | None = None, janela_logs: timedelta = timedelta(minutes=5)) -> Path:
    """Grava um retrato do cluster em results/snapshots/<data-hora>_<rótulo>/ (ou em `base`)."""
    carimbo = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    destino = (base or DIR_RESULTS / "snapshots") / f"{carimbo}_{rotulo}"
    api = garage.AdminAPI()
    desde = datetime.now() - janela_logs

    salvar(
        destino,
        "estado.txt",
        "docker inspect (lab.cluster.estado) + generated/MODE",
        lambda: {"modo": modo_atual(), "nos": [cluster.estado(n) for n in NOS]},
    )
    for n in NOS:
        salvar(
            destino, f"health_garage{n}.txt", f"GET http://<garage{n}>:3903/health", lambda n=n: {"http": api.health(n)}
        )
        salvar(
            destino,
            f"cluster_status_garage{n}.txt",
            f"GET /v2/GetClusterStatus (garage{n})",
            lambda n=n: api.cluster_status(n),
        )
        salvar(
            destino,
            f"cluster_health_garage{n}.txt",
            f"GET /v2/GetClusterHealth (garage{n})",
            lambda n=n: api.cluster_health(n),
        )
        salvar(
            destino,
            f"workers_garage{n}.txt",
            f"POST /v2/ListWorkers?node=self (garage{n})",
            lambda n=n: api.list_workers(n),
        )
    salvar(destino, "layout_show.txt", "garage -h <id>@127.0.0.1:3901 layout show (garage1)", garage.layout_show)
    salvar(
        destino, "layout_history.txt", "garage -h <id>@127.0.0.1:3901 layout history (garage1)", garage.layout_history
    )
    salvar(destino, "stats.txt", "garage -h <id>@127.0.0.1:3901 stats -a (garage1)", garage.stats_todos)
    salvar(destino, "du_dados.txt", "du -sb dados/g{1,2,3}/data", _du_dados)
    salvar(destino, "docker_stats.txt", "docker stats --no-stream (memória, bytes)", _memoria)
    for servico in cluster.SERVICOS:
        salvar(
            destino,
            f"logs_{servico}.txt",
            f"docker logs --since {desde.isoformat(timespec='seconds')} {servico}",
            lambda s=servico: cluster.logs(s, desde=desde),
        )
    api.fechar()
    journal.registrar("evidence snapshot", {"rotulo": rotulo}, journal.relativo(destino))
    return destino
