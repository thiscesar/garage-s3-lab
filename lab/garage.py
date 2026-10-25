"""Acesso ao Garage: Admin API v2 primeiro, CLI só para o resto.

Admin API: HTTP na porta 3903 pelo IP do nó na rede
`clientes`, `Authorization: Bearer <ADMIN_TOKEN>`; `/health` sem token; `/metrics` com o token
de métricas. Parâmetro `node` das rotas por nó: "self", "*" ou prefixo do ID.

CLI: `docker exec garageN /garage -h <id>@127.0.0.1:3901 ...`. Sem `-h`, a CLI usa o
`rpc_public_addr` e não alcança o próprio nó quando ele está isolado da rede interna (observado no laboratório).
"""

from __future__ import annotations

import time

import httpx

from lab import cluster, journal
from lab.config import NOS, Topologia, carregar_env, modo_atual, renderizar

PORTA_ADMIN = 3903
PORTA_RPC = 3901


class ErroGarage(Exception):
    """Falha ao falar com o Garage (API ou CLI)."""


# ---------------------------------------------------------------- Admin API v2


class AdminAPI:
    def __init__(self, env: dict[str, str] | None = None, timeout: float = 3.0):
        env = carregar_env() if env is None else env
        self.topo = Topologia.do_env(env)
        self._token = env.get("ADMIN_TOKEN", "")
        self._token_metricas = env.get("METRICS_TOKEN", "")
        self._http = httpx.Client(timeout=timeout)

    def url(self, n: int, caminho: str) -> str:
        return f"http://{self.topo.ip_clientes(n)}:{PORTA_ADMIN}{caminho}"

    def _pedir(self, metodo: str, n: int, caminho: str, token: str | None, **kwargs) -> httpx.Response:
        cabecalhos = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            return self._http.request(metodo, self.url(n, caminho), headers=cabecalhos, **kwargs)
        except httpx.HTTPError as e:
            raise ErroGarage(f"garage{n}: {metodo} {caminho}: {e}") from e

    def _json(self, metodo: str, n: int, endpoint: str, **kwargs) -> dict:
        resp = self._pedir(metodo, n, f"/v2/{endpoint}", self._token, **kwargs)
        if resp.status_code != 200:
            raise ErroGarage(f"garage{n}: {endpoint} → HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def health(self, n: int) -> int | None:
        """Código HTTP de GET /health (200 = há quórum, 503 = não há) ou None se o nó não responde."""
        try:
            return self._pedir("GET", n, "/health", None).status_code
        except ErroGarage:
            return None

    def cluster_health(self, n: int) -> dict:
        return self._json("GET", n, "GetClusterHealth")

    def cluster_status(self, n: int) -> dict:
        return self._json("GET", n, "GetClusterStatus")

    def list_workers(self, n: int, alvo: str = "self") -> dict:
        return self._json("POST", n, "ListWorkers", params={"node": alvo}, json={})

    def launch_repair(self, n: int, tipo: str, alvo: str = "*") -> dict:
        """Dispara um reparo (ex.: "tables", "blocks") nos nós `alvo` ("*" = todos)."""
        return self._json("POST", n, "LaunchRepairOperation", params={"node": alvo}, json={"repairType": tipo})

    def metrics(self, n: int) -> str:
        resp = self._pedir("GET", n, "/metrics", self._token_metricas)
        if resp.status_code != 200:
            raise ErroGarage(f"garage{n}: /metrics → HTTP {resp.status_code}")
        return resp.text

    def fechar(self) -> None:
        self._http.close()


def esperar_saude(nos: tuple[int, ...] = NOS, limite_s: float = 60, api: AdminAPI | None = None) -> dict[int, float]:
    """Espera GET /health = 200 em cada nó; devolve o tempo (s) até cada um responder 200."""
    api = api or AdminAPI()
    inicio = time.monotonic()
    tempos: dict[int, float] = {}
    while len(tempos) < len(nos):
        for n in nos:
            if n not in tempos and api.health(n) == 200:
                tempos[n] = time.monotonic() - inicio
        if len(tempos) == len(nos):
            break
        if time.monotonic() - inicio > limite_s:
            faltam = [f"garage{n}" for n in nos if n not in tempos]
            raise ErroGarage(f"/health não respondeu 200 em {limite_s:.0f} s: {', '.join(faltam)}")
        time.sleep(0.5)
    return tempos


def definir_modo(modo: str, limite_s: float = 60) -> dict[int, float]:
    """Troca o consistency_mode: renderiza, reinicia os 3 nós e espera /health = 200.

    A reconexão após o restart depende de `bootstrap_peers`. Devolve o tempo até cada nó responder 200.
    """
    cluster.eco(f"python -m lab.config render --mode {modo}")
    renderizar(modo)
    t0 = time.monotonic()
    cluster.reiniciar(NOS)
    tempos = esperar_saude(NOS, limite_s)
    journal.registrar(
        "mode set",
        {"modo": modo},
        {
            "s_apos_restart_ate_health_200": {f"garage{n}": round(t, 2) for n, t in tempos.items()},
            "total_s": round(time.monotonic() - t0, 2),
        },
    )
    return tempos


def restaurar(limite_s: float = 90) -> dict:
    """Volta o cluster ao estado padrão: cura partições, religa nós parados e volta a `consistent`.

    Usado no `finally` de todo experimento e por `labctl net heal --all`. Devolve o que foi feito.
    """
    feito: dict = {"curados": [], "religados": [], "modo": None}
    for n in NOS:
        if cluster.estado(n)["status"] != "running":
            cluster.ligar(n)
            feito["religados"].append(n)
    for n in NOS:
        if cluster.curar(n):
            feito["curados"].append(n)
    if modo_atual() != "consistent":
        definir_modo("consistent", limite_s)
        feito["modo"] = "consistent"
    else:
        esperar_saude(NOS, limite_s)
    journal.registrar("restaurar", {}, feito)
    return feito


# ---------------------------------------------------------------- CLI


def node_id(n: int) -> str:
    """`garage node id -q` → "<id>@<ip>:<porta>". Lê a chave local; funciona com o nó isolado."""
    codigo, saida = cluster.executar(n, ["/garage", "node", "id", "-q"])
    if codigo != 0 or "@" not in saida:
        raise ErroGarage(f"garage{n}: `node id -q` falhou (código {codigo})")
    return saida.strip()


def cli(n: int, *args: str) -> str:
    """Roda `/garage -h <id>@127.0.0.1:3901 <args>` no nó n e devolve o stdout (logs vão para o stderr)."""
    id_no = node_id(n).split("@", 1)[0]
    comando = ["/garage", "-h", f"{id_no}@127.0.0.1:{PORTA_RPC}", *args]
    codigo, saida = cluster.executar(n, comando)
    if codigo != 0:
        raise ErroGarage(f"garage{n}: `garage {' '.join(args)}` falhou (código {codigo})")
    return saida


def layout_show(n: int = 1) -> str:
    return cli(n, "layout", "show")


def layout_history(n: int = 1) -> str:
    return cli(n, "layout", "history")


def stats_todos(n: int = 1) -> str:
    return cli(n, "stats", "-a")


def status_texto(n: int = 1) -> str:
    return cli(n, "status")
