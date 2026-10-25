"""Controle dos contêineres e da rede do laboratório pelo Docker SDK.

Toda ação que muda o cluster passa por `eco()`, que mostra o comando Docker equivalente:
assim a CLI mantém o valor didático dos scripts (cada linha, um conceito).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import datetime
from functools import cache

import docker
from docker.errors import NotFound
from docker.models.containers import Container

from lab import journal
from lab.config import NOS, Topologia

REDE_INTERNA = "t3_interna"
SERVICOS = ("garage1", "garage2", "garage3", "cliente")

# Destino do eco das ações (a CLI troca por um print colorido); padrão: silencioso
eco: Callable[[str], None] = lambda _msg: None


def definir_eco(funcao: Callable[[str], None]) -> None:
    global eco
    eco = funcao


@cache
def _docker() -> docker.DockerClient:
    return docker.from_env()


def nome(n: int) -> str:
    if n not in NOS:
        raise ValueError(f"nó inválido: {n} (use 1, 2 ou 3)")
    return f"garage{n}"


def container(nome_ou_no: str | int) -> Container:
    alvo = nome(nome_ou_no) if isinstance(nome_ou_no, int) else nome_ou_no
    return _docker().containers.get(alvo)


# ---------------------------------------------------------------- estado


def na_rede_interna(n: int) -> bool:
    c = container(n)
    c.reload()
    return REDE_INTERNA in (c.attrs["NetworkSettings"]["Networks"] or {})


def estado(n: int) -> dict:
    """Estado do contêiner do nó n: status, saúde (healthcheck), rede interna e IPs."""
    try:
        c = container(n)
    except NotFound:
        return {"no": n, "nome": nome(n), "status": "inexistente", "saude": None, "na_interna": False, "ips": {}}
    c.reload()
    redes = c.attrs["NetworkSettings"]["Networks"] or {}
    return {
        "no": n,
        "nome": c.name,
        "status": c.status,
        "saude": (c.attrs["State"].get("Health") or {}).get("Status"),
        "na_interna": REDE_INTERNA in redes,
        "ips": {rede: info.get("IPAddress") for rede, info in redes.items()},
        "oom": c.attrs["State"].get("OOMKilled", False),
        "mem_limit": c.attrs["HostConfig"].get("Memory"),
    }


def memoria(nome_servico: str) -> int | None:
    """Memória em uso (bytes), no mesmo critério do `docker stats`; None se o contêiner não roda.

    A API do Docker dá o uso total; o `docker stats` desconta o cache de arquivos (`inactive_file` no
    cgroup v2, `total_inactive_file` no v1). Fonte: https://docs.docker.com/reference/cli/docker/container/stats/
    """
    try:
        c = container(nome_servico)
    except NotFound:
        return None
    if c.status != "running":
        return None
    m = c.stats(stream=False).get("memory_stats", {})
    if "usage" not in m:
        return None
    detalhes = m.get("stats", {})
    cache = detalhes.get("inactive_file", detalhes.get("total_inactive_file", 0))
    return m["usage"] - cache


# ---------------------------------------------------------------- ações


def parar(n: int) -> None:
    eco(f"docker stop {nome(n)}")
    container(n).stop()
    journal.registrar("node stop", {"no": n})


def ligar(n: int) -> None:
    eco(f"docker start {nome(n)}")
    container(n).start()
    journal.registrar("node start", {"no": n})


def reiniciar(nos: tuple[int, ...] = NOS) -> None:
    """Reinicia os nós **em paralelo**.

    Em sequência, um nó que volta antes de o outro cair marca o par como desconectado e só tenta de novo
    30 s depois (peering.rs l. 24): a malha completa levava ~30–60 s. Em paralelo, ~10 s (medido).
    """
    nomes = " ".join(nome(n) for n in nos)
    eco(f"docker restart {nomes}   # em paralelo")
    threads = [threading.Thread(target=lambda n=n: container(n).restart()) for n in nos]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    journal.registrar("node restart", {"nos": list(nos), "paralelo": True})


def particionar(n: int) -> bool:
    """Desconecta o nó da rede interna (RPC). Retorna False se ele já estava fora."""
    if not na_rede_interna(n):
        return False
    eco(f"docker network disconnect {REDE_INTERNA} {nome(n)}")
    _docker().networks.get(REDE_INTERNA).disconnect(container(n))
    journal.registrar("net partition", {"no": n})
    return True


def curar(n: int, topo: Topologia | None = None) -> bool:
    """Reconecta o nó à rede interna sempre com o IP fixo dele. Retorna False se ele já estava dentro."""
    if na_rede_interna(n):
        return False
    ip = (topo or Topologia.do_env()).ip_interno(n)
    eco(f"docker network connect --ip {ip} {REDE_INTERNA} {nome(n)}")
    _docker().networks.get(REDE_INTERNA).connect(container(n), ipv4_address=ip)
    journal.registrar("net heal", {"no": n, "ip": ip})
    return True


# ---------------------------------------------------------------- execução e logs


def executar(n: int, args: list[str]) -> tuple[int, str]:
    """Roda um comando no contêiner do nó (forma exec: a imagem só tem /garage). Retorna (código, stdout)."""
    resultado = container(n).exec_run(args, demux=True)
    saida = (resultado.output[0] or b"").decode(errors="replace") if resultado.output else ""
    return resultado.exit_code, saida


def logs(nome_servico: str, desde: datetime | None = None, ate: datetime | None = None) -> str:
    return container(nome_servico).logs(since=desde, until=ate, timestamps=True).decode(errors="replace")


def executar_servico(nome_servico: str, args: list[str]) -> str:
    """Roda um comando num contêiner do laboratório pelo nome (ex.: "cliente") e devolve stdout + stderr."""
    resultado = container(nome_servico).exec_run(args)
    return (resultado.output or b"").decode(errors="replace")
