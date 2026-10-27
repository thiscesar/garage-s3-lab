"""Acesso ao Garage: Admin API v2 primeiro, CLI só para o resto.

Admin API: HTTP na porta 3903 pelo IP do nó na rede
`clientes`, `Authorization: Bearer <ADMIN_TOKEN>`; `/health` sem token; `/metrics` com o token
de métricas. Parâmetro `node` das rotas por nó: "self", "*" ou prefixo do ID.

CLI: `docker exec garageN /garage -h <id>@127.0.0.1:3901 ...`. Sem `-h`, a CLI usa o
`rpc_public_addr` e não alcança o próprio nó quando ele está isolado da rede interna (observado no laboratório).
"""

from __future__ import annotations

import re
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

    def estatisticas_no(self, n: int) -> dict:
        """GetNodeStatistics do próprio nó n, já interpretado por `ler_estatisticas_no` (tabelas e blocos)."""
        r = self._json("GET", n, "GetNodeStatistics", params={"node": "self"})
        return ler_estatisticas_no(next(iter(r["success"].values()))["freeform"])

    def metrics(self, n: int) -> str:
        resp = self._pedir("GET", n, "/metrics", self._token_metricas)
        if resp.status_code != 200:
            raise ErroGarage(f"garage{n}: /metrics → HTTP {resp.status_code}")
        return resp.text

    def fechar(self) -> None:
        self._http.close()


_RE_RPC = re.compile(r"^rpc_request_counter\{(?P<rotulos>[^}]*)\}\s+(?P<valor>[0-9.eE+-]+)$")
_RE_ROTULO = re.compile(r'(\w+)="([^"]*)"')


def contadores_rpc(texto_metricas: str) -> dict[tuple[str, str, str], float]:
    """`rpc_request_counter{from, rpc_endpoint, to}` → {(from, to, endpoint): valor}.

    Cada nó só expõe os RPCs que ele emitiu (`from` = ele próprio).
    """
    contadores = {}
    for linha in texto_metricas.splitlines():
        m = _RE_RPC.match(linha.strip())
        if m:
            r = dict(_RE_ROTULO.findall(m.group("rotulos")))
            contadores[(r.get("from", ""), r.get("to", ""), r.get("rpc_endpoint", ""))] = float(m.group("valor"))
    return contadores


def metrica(texto_metricas: str, nome: str) -> float | None:
    """Valor de uma métrica Prometheus sem rótulos (ex.: `block_bytes_read`); None se ausente."""
    for linha in texto_metricas.splitlines():
        partes = linha.split()
        if len(partes) == 2 and partes[0] == nome:
            return float(partes[1])
    return None


def ler_estatisticas_no(freeform: str) -> dict:
    """Texto de GetNodeStatistics → {"tabelas": {nome: {Items, MklItems, MklTodo, InsQueue, GcTodo}}, "blocos": {...}}."""
    tabelas, blocos, em_tabelas = {}, {}, False
    for linha in freeform.splitlines():
        if linha.strip().startswith("Table") and "Items" in linha:
            em_tabelas = True
            continue
        if em_tabelas:
            m = re.match(r"\s+(\w+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$", linha)
            if m:
                tabelas[m.group(1)] = dict(
                    zip(("Items", "MklItems", "MklTodo", "InsQueue", "GcTodo"), map(int, m.groups()[1:]), strict=True)
                )
                continue
            em_tabelas = False
        for chave, rotulo in (
            ("rc", "number of RC entries"),
            ("resync", "resync queue length"),
            ("erros", "blocks with resync errors"),
        ):
            m = re.search(rf"{rotulo}:\s+(\d+)", linha)
            if m:
                blocos[chave] = int(m.group(1))
    return {"tabelas": tabelas, "blocos": blocos}


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


def malha(api: AdminAPI | None = None, nos: tuple[int, ...] = NOS) -> dict[int, set[str] | None]:
    """Quem cada nó vê como ativo (`isUp` em GetClusterStatus); None se o nó não responde."""
    api = api or AdminAPI()
    visoes: dict[int, set[str] | None] = {}
    for n in nos:
        try:
            st = api.cluster_status(n)
            visoes[n] = {x["hostname"] for x in st["nodes"] if x.get("isUp") and x.get("hostname")}
        except ErroGarage:
            visoes[n] = None
    return visoes


def esperar_malha(
    nos: tuple[int, ...] = NOS, limite_s: float = 120, leituras_estaveis: int = 2, api: AdminAPI | None = None
) -> float:
    """Espera a malha completa: cada nó vê todos os `nos` como ativos, em `leituras_estaveis` leituras seguidas.

    O /health pode dar 200 com o nó vendo só a si mesmo logo após um restart, e a reconexão leva
    de segundos a dezenas de segundos (novas tentativas a cada 30 s, peering.rs l. 24). Devolve o tempo (s).
    """
    api = api or AdminAPI()
    esperados = {f"garage{n}" for n in nos}
    inicio, seguidas = time.monotonic(), 0
    while True:
        visoes = malha(api, nos)
        seguidas = seguidas + 1 if all(v == esperados for v in visoes.values()) else 0
        if seguidas >= leituras_estaveis:
            return time.monotonic() - inicio
        if time.monotonic() - inicio > limite_s:
            detalhe = "; ".join(f"garage{n} vê {sorted(v) if v is not None else '—'}" for n, v in visoes.items())
            raise ErroGarage(f"malha incompleta após {limite_s:.0f} s: {detalhe}")
        time.sleep(1.0)


def definir_modo(modo: str, limite_s: float = 60) -> dict[int, float]:
    """Troca o consistency_mode: renderiza, reinicia os 3 nós e espera /health = 200.

    A reconexão após o restart depende de `bootstrap_peers`. Depois do /health, espera a malha
    completa. Devolve o tempo até cada nó responder 200.
    """
    cluster.eco(f"python -m lab.config render --mode {modo}")
    renderizar(modo)
    t0 = time.monotonic()
    cluster.reiniciar(NOS)
    t_restart = time.monotonic() - t0
    tempos = esperar_saude(NOS, limite_s)
    t_malha = esperar_malha(NOS, limite_s=max(limite_s, 120))
    journal.registrar(
        "mode set",
        {"modo": modo},
        {
            "s_restart": round(t_restart, 2),
            "s_apos_restart_ate_health_200": {f"garage{n}": round(t, 2) for n, t in tempos.items()},
            "s_ate_malha_completa": round(t_malha, 2),
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
        feito["s_ate_malha_completa"] = round(esperar_malha(NOS, limite_s=max(limite_s, 120)), 2)
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
    codigo, saida, erro = cluster.executar_com_erro(n, comando)
    if codigo != 0:
        linhas = [x for x in erro.splitlines() if " INFO " not in x and x.strip()]
        detalhe = "\n".join(linhas)[-500:]
        raise ErroGarage(f"garage{n}: `garage {' '.join(args)}` falhou (código {codigo}): {detalhe}")
    return saida


def layout_show(n: int = 1) -> str:
    return cli(n, "layout", "show")


def layout_history(n: int = 1) -> str:
    return cli(n, "layout", "history")


def stats_todos(n: int = 1) -> str:
    return cli(n, "stats", "-a")


def status_texto(n: int = 1) -> str:
    return cli(n, "status")
