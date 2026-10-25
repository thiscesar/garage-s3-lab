"""Painel da demo ao vivo: FastAPI + Server-Sent Events, rodando no host.

Toda ação usa a biblioteca `lab/` (cluster, garage, workload, oráculo, journal); aqui só há agendamento,
estado em memória e a entrega para o navegador:

- coletor rápido (1 s): contêiner, /health, rede interna, quem cada nó vê (GetClusterStatus), modo e layout;
- coletor lento (5 s): memória, disco de `dados/gN/data`, itens da tabela de objetos por nó, journal e logs;
- carga: `lab.workload.Workload` com um *sink* que guarda os eventos (já classificados pelo oráculo);
- `/api/stream`: lotes de operações a cada 250 ms (amostrados acima de `MAX_LOTE`) e o estado a cada 1 s.

Uma ação de controle por vez (lock); enquanto uma ação roda, as outras recebem HTTP 409.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import threading
import time
from collections import deque
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from lab import cluster, garage, journal
from lab.config import MODOS, N_REPLICAS, NOS, QUORUNS, modo_atual
from lab.workload import ConfigCarga, Workload

ESTATICOS = Path(__file__).parent / "static"
MAX_LOTE = 300  # operações por lote de 250 ms enviadas ao navegador (acima disso, amostra)
ANSI = re.compile(r"\x1b\[[0-9;]*m")

# Presets de carga: os mesmos papéis dos experimentos.
PRESETS: dict[str, dict] = {
    "A1": {  # escrita pela maioria, leitura pelos dois lados (E9)
        "descricao": "escritor via g1 (10 PUT/s + 2 frias/s); leitores via g2 e g3 (10 GET/s cada)",
        "config": {"escritores": (1,), "leitores": (2, 3), "taxa_escrita": 10, "taxa_fria": 2, "taxa_leitura": 10},
    },
    "A2": {  # escrita pelos dois lados na mesma chave
        "descricao": "escritores via g1 (maj) e g3 (min) na mesma chave, 5 PUT/s cada; leitores via g2 e g3",
        "config": {
            "escritores": (1, 3),
            "leitores": (2, 3),
            "taxa_escrita": 5,
            "taxa_fria": 0,
            "taxa_leitura": 10,
            "lados": {1: "maj", 3: "min"},
        },
    },
    "A4": {  # sem falha, carga alta: janela de leitura desatualizada com R = 1
        "descricao": "escritor via g1 (20 PUT/s); leitores via g2 e g3 (20 GET/s cada)",
        "config": {"escritores": (1,), "leitores": (2, 3), "taxa_escrita": 20, "taxa_fria": 0, "taxa_leitura": 20},
    },
}


class Ocupado(Exception):
    """Já há uma ação de controle em andamento."""


class Painel:
    def __init__(self) -> None:
        self.t0 = time.monotonic()
        self._lock_acao = threading.Lock()
        self._lock_carga = threading.Lock()
        self._lock_eventos = threading.Lock()
        self.acao: dict | None = None  # ação em andamento ou a última concluída
        self.rapido: dict = {}
        self.lento: dict = {}
        self.marcos: deque[dict] = deque(maxlen=30)
        self.eventos: deque[dict] = deque(maxlen=20_000)
        self.total_eventos = 0  # quantos eventos já entraram (o SSE envia só os novos)
        self.carga: Workload | None = None
        self.preset: str | None = None
        self.chave_quente: str | None = None
        self.versoes: dict = {"confirmada": None, "lida": {}}
        self._parar = threading.Event()
        self._threads: list[threading.Thread] = []

    def agora(self) -> float:
        return round(time.monotonic() - self.t0, 3)

    # ------------------------------------------------------------ coletores

    def iniciar(self) -> None:
        for alvo, periodo in ((self._coletar_rapido, 1.0), (self._coletar_lento, 5.0)):
            t = threading.Thread(target=self._laco, args=(alvo, periodo), daemon=True)
            t.start()
            self._threads.append(t)

    def encerrar(self) -> None:
        self._parar.set()
        self.parar_carga()

    def _laco(self, coletar: Callable[[garage.AdminAPI], None], periodo: float) -> None:
        api = garage.AdminAPI(timeout=2.0)  # um cliente por thread
        while not self._parar.is_set():
            try:
                coletar(api)
            except Exception as e:  # noqa: BLE001 — o painel segue de pé; o erro aparece na tela
                self.rapido["erro_coleta"] = f"{type(e).__name__}: {e}"
            self._parar.wait(periodo)

    def _coletar_rapido(self, api: garage.AdminAPI) -> None:
        nos, layout, zonas = {}, None, {}
        for n in NOS:
            e = cluster.estado(n)
            info = {
                "status": e["status"],
                "saude": e["saude"],
                "na_interna": e["na_interna"],
                "ip_interno": (e["ips"] or {}).get(cluster.REDE_INTERNA),
                "health": api.health(n) if e["status"] == "running" else None,
                "ve_up": None,
            }
            if e["status"] == "running":
                try:
                    st = api.cluster_status(n)
                    info["ve_up"] = sorted(
                        (x.get("hostname") or x["id"][:8]).replace("garage", "g") for x in st["nodes"] if x.get("isUp")
                    )
                    layout = st.get("layoutVersion") if layout is None else layout
                    for x in st["nodes"]:
                        if x.get("hostname"):
                            zonas[x["hostname"]] = (x.get("role") or {}).get("zone")
                except garage.ErroGarage:
                    pass
            nos[n] = info
        for n in NOS:
            nos[n]["zona"] = zonas.get(f"garage{n}")
        modo = modo_atual()
        r, w = QUORUNS.get(modo, (None, None))
        self.rapido = {
            "agora": self.agora(),
            "nos": nos,
            "modo": modo,
            "quorum": {"N": N_REPLICAS, "R": r, "W": w, "R+W>N": None if r is None else r + w > N_REPLICAS},
            "layout": layout,
        }

    def _coletar_lento(self, api: garage.AdminAPI) -> None:
        nos: dict[int, dict] = {}
        try:
            disco = cluster.uso_disco_dados()
        except (OSError, ValueError):
            disco = {}
        desde = datetime.now() - timedelta(seconds=60)
        for n in NOS:
            d: dict = {"memoria": cluster.memoria(f"garage{n}"), "disco_dados": disco.get(f"garage{n}")}
            try:
                tab = api.estatisticas_no(n)["tabelas"].get("object", {})
                d["objetos"] = tab.get("Items")
            except (garage.ErroGarage, KeyError, StopIteration):
                d["objetos"] = None
            try:
                linhas = ANSI.sub("", cluster.logs(f"garage{n}", desde=desde)).splitlines()
                # o próprio painel consulta a Admin API a cada 1 s: essas linhas são ruído
                d["logs"] = [x for x in linhas if "generic_server" not in x][-6:]
            except Exception:  # noqa: BLE001 — contêiner removido ou parado: sem logs
                d["logs"] = []
            nos[n] = d
        self.lento = {"nos": nos, "journal": journal.ultimas(12)}

    # ------------------------------------------------------------ ações de controle

    def executar(self, nome: str, funcao: Callable[[], object]) -> dict:
        """Roda `funcao` numa thread, uma ação por vez. Levanta `Ocupado` se já houver uma em andamento."""
        if not self._lock_acao.acquire(blocking=False):
            raise Ocupado(self.acao["nome"] if self.acao else "ação")
        self.acao = {"nome": nome, "inicio": self.agora(), "situacao": "em andamento", "resultado": None}
        self.marcos.append({"t": self.agora(), "rotulo": nome})

        def rodar() -> None:
            try:
                self.acao["resultado"] = funcao()
                self.acao["situacao"] = "ok"
            except Exception as e:  # noqa: BLE001 — reportado na tela
                self.acao["situacao"] = "erro"
                self.acao["resultado"] = f"{type(e).__name__}: {e}"
            finally:
                self.acao["fim"] = self.agora()
                self._lock_acao.release()

        threading.Thread(target=rodar, daemon=True).start()
        return dict(self.acao)

    # ------------------------------------------------------------ carga

    def _guardar(self, ev: dict) -> None:
        compacto = {
            "t": ev["t_start"],
            "te": ev["t_end"],
            "op": ev["op"],
            "no": ev["node"],
            "cls": ev.get("class") or ("ok" if ev["status"] == "ok" else "error"),
            "v": ev.get("version_read") if ev["op"] == "GET" else ev.get("version_written"),
            "lag": ev.get("lag_ms"),
            "erro": (ev.get("error") or "")[:80] or None,
        }
        with self._lock_eventos:
            self.eventos.append(compacto)
            self.total_eventos += 1
            if ev["key"] == self.chave_quente:
                if ev["op"] == "PUT" and ev["status"] == "ok":
                    atual = self.versoes["confirmada"] or 0
                    self.versoes["confirmada"] = max(atual, ev["version_written"])
                elif ev["op"] == "GET" and ev.get("class") in ("fresh", "stale"):
                    self.versoes["lida"][ev["node"]] = ev.get("version_read")

    def iniciar_carga(self, preset: str) -> dict:
        if preset not in PRESETS:
            raise ValueError(f"preset inválido: {preset} (use {', '.join(PRESETS)})")
        with self._lock_carga:
            self._parar_carga_sem_lock()
            tag = f"painel/{preset}-{int(time.time())}"
            cfg = ConfigCarga(**PRESETS[preset]["config"], chave_quente=f"{tag}/hot", prefixo_frias=f"{tag}/cold/")
            self.versoes = {"confirmada": None, "lida": {}}
            self.chave_quente = cfg.chave_quente
            self.carga = Workload(cfg, {"run": "painel", "scenario": preset}, [self._guardar], t0=self.t0)
            self.carga.iniciar()
            self.preset = preset
            journal.registrar("painel carga", {"preset": preset, "chave": cfg.chave_quente})
        self.marcos.append({"t": self.agora(), "rotulo": f"carga {preset}"})
        return {"preset": preset, "chave_quente": self.chave_quente}

    def _parar_carga_sem_lock(self) -> dict | None:
        if self.carga is None:
            return None
        w, self.carga, preset = self.carga, None, self.preset
        self.preset = None
        try:
            w.parar()
            erro = None
        except RuntimeError as e:
            erro = str(e)
        journal.registrar("painel carga parada", {"preset": preset}, {"erro": erro})
        return {"preset": preset, "resumo": w.resumo(), "erro": erro}

    def parar_carga(self) -> dict | None:
        with self._lock_carga:
            res = self._parar_carga_sem_lock()
        if res:
            self.marcos.append({"t": self.agora(), "rotulo": "carga parada"})
        return res

    # ------------------------------------------------------------ saída

    def lote(self, desde: int) -> tuple[list[dict], int]:
        """Eventos novos desde o contador `desde` (amostrados se forem muitos) e o novo contador."""
        with self._lock_eventos:
            total = self.total_eventos
            k = min(total - desde, len(self.eventos))
            novos = list(self.eventos)[-k:] if k > 0 else []
        if len(novos) > MAX_LOTE:
            novos = sorted(random.sample(novos, MAX_LOTE), key=lambda e: e["t"])
        return novos, total

    def estado(self) -> dict:
        with self._lock_eventos:
            versoes = {"confirmada": self.versoes["confirmada"], "lida": dict(self.versoes["lida"])}
        return {
            **self.rapido,
            "agora": self.agora(),
            "lento": self.lento,
            "acao": self.acao,
            "ocupado": self._lock_acao.locked(),
            "carga": {"preset": self.preset, "chave_quente": self.chave_quente, "versoes": versoes},
            "marcos": list(self.marcos),
            "presets": {k: v["descricao"] for k, v in PRESETS.items()},
            "modos": list(MODOS),
        }


# ---------------------------------------------------------------- aplicação


class PedidoModo(BaseModel):
    mode: str


class PedidoCarga(BaseModel):
    preset: str


class PedidoRepair(BaseModel):
    tipo: str


def criar_app(painel: Painel | None = None, iniciar_coletores: bool = True) -> FastAPI:
    p = painel or Painel()

    @asynccontextmanager
    async def ciclo_de_vida(app: FastAPI):
        if iniciar_coletores:
            p.iniciar()
        yield
        p.encerrar()

    app = FastAPI(title="Painel Garage S3 (Ênfase A)", lifespan=ciclo_de_vida)
    app.state.painel = p
    app.mount("/static", StaticFiles(directory=ESTATICOS), name="static")

    def _no(n: int) -> int:
        if n not in NOS:
            raise HTTPException(404, f"nó inexistente: {n}")
        return n

    def _acao(nome: str, funcao: Callable[[], object]) -> dict:
        try:
            return p.executar(nome, funcao)
        except Ocupado as e:
            raise HTTPException(409, f"aguarde: ação em andamento ({e})") from e

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(ESTATICOS / "index.html")

    @app.get("/api/state")
    def estado() -> dict:
        return p.estado()

    @app.post("/api/net/{n}/{acao}")
    def rede(n: int, acao: str) -> dict:
        _no(n)
        if acao == "partition":
            return _acao(f"particionar g{n}", lambda: cluster.particionar(n))
        if acao == "heal":
            return _acao(f"curar g{n}", lambda: cluster.curar(n))
        raise HTTPException(404, "use partition ou heal")

    @app.post("/api/node/{n}/{acao}")
    def no(n: int, acao: str) -> dict:
        _no(n)
        if acao == "stop":
            return _acao(f"parar g{n}", lambda: cluster.parar(n))
        if acao == "start":
            return _acao(f"ligar g{n}", lambda: cluster.ligar(n))
        raise HTTPException(404, "use stop ou start")

    @app.post("/api/mode")
    def modo(pedido: PedidoModo) -> dict:
        if pedido.mode not in MODOS:
            raise HTTPException(422, f"modo inválido: {pedido.mode}")
        return _acao(f"modo {pedido.mode}", lambda: garage.definir_modo(pedido.mode))

    @app.post("/api/restore")
    def restaurar() -> dict:
        return _acao("restaurar (curar, religar, consistent)", garage.restaurar)

    @app.post("/api/repair")
    def repair(pedido: PedidoRepair) -> dict:
        if pedido.tipo not in ("tables", "blocks"):
            raise HTTPException(422, "use tables ou blocks")
        return _acao(f"repair {pedido.tipo}", lambda: garage.AdminAPI().launch_repair(1, pedido.tipo))

    @app.post("/api/workload/start")
    def carga_iniciar(pedido: PedidoCarga) -> dict:
        try:
            return p.iniciar_carga(pedido.preset)
        except ValueError as e:
            raise HTTPException(422, str(e)) from e

    @app.post("/api/workload/stop")
    def carga_parar() -> dict:
        return p.parar_carga() or {"preset": None}

    @app.get("/api/stream")
    async def stream() -> StreamingResponse:
        async def gerar():
            visto, prox_estado = p.total_eventos, 0.0
            while True:
                novos, visto = p.lote(visto)
                if novos:
                    yield f"event: ops\ndata: {json.dumps({'agora': p.agora(), 'ops': novos})}\n\n"
                if time.monotonic() >= prox_estado:
                    yield f"event: estado\ndata: {json.dumps(p.estado(), default=str)}\n\n"
                    prox_estado = time.monotonic() + 1.0
                await asyncio.sleep(0.25)

        return StreamingResponse(gerar(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    return app


app = criar_app()
