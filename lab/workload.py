"""Gerador de carga com classificação das leituras pelo oráculo.

Papéis, cada um numa thread com cliente boto3 próprio (um cliente por thread):
- escritor quente: PUT na chave quente pelo nó `n` a `taxa_escrita` PUT/s;
- escritor frio:   PUT em chaves frias novas pelo nó `n` a `taxa_fria` PUT/s;
- leitor:          GET da chave quente pelo nó `n` a `taxa_leitura` GET/s.

Cada objeto leva no corpo o JSON {"v": versão, "writer": lado, "node": n, "t": monotônico} e a versão também
em `x-amz-meta-version` (o Garage preserva `x-amz-meta-*`: `put.rs` l. 686–692). As versões de uma chave são
alocadas por um contador único, mesmo com vários escritores.

Ritmo pelo relógio monotônico do host: a operação k sai em t0 + k/taxa; se uma operação atrasar
(ex.: timeout de 3 s), a próxima sai assim que possível, sem rajada para compensar.

Eventos vão para os *sinks*: arquivo JSONL, lista em memória ou fila do painel.
Leituras só são emitidas quando o oráculo pode classificá-las de forma definitiva (escritas anteriores terminadas).
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from lab import s3 as lab_s3
from lab.oracle import Oraculo

Evento = dict
Sink = Callable[[Evento], None]


@dataclass
class ConfigCarga:
    escritores: tuple[int, ...] = (1,)  # nós por onde se escreve a chave quente (e as frias)
    leitores: tuple[int, ...] = (2, 3)  # nós por onde se lê a chave quente
    taxa_escrita: float = 10.0  # PUT/s na chave quente, por escritor
    taxa_fria: float = 2.0  # PUT/s em chaves frias novas, por escritor (0 desliga)
    taxa_leitura: float = 10.0  # GET/s por leitor
    chave_quente: str = "e9/hot"
    prefixo_frias: str = "e9/cold/"
    lados: dict[int, str] = field(default_factory=dict)  # nó → "maj"/"min" (A2); padrão "maj"
    connect_timeout: float = 1.0
    read_timeout: float = 3.0


class SinkJSONL:
    """Grava um evento por linha (ops.jsonl). Seguro para várias threads."""

    def __init__(self, caminho: Path):
        caminho.parent.mkdir(parents=True, exist_ok=True)
        self._arq = caminho.open("a")
        self._lock = threading.Lock()

    def __call__(self, evento: Evento) -> None:
        with self._lock:
            self._arq.write(json.dumps(evento, ensure_ascii=False) + "\n")

    def fechar(self) -> None:
        with self._lock:
            self._arq.close()


class Workload:
    def __init__(
        self,
        config: ConfigCarga,
        contexto: dict | None = None,
        sinks: list[Sink] | None = None,
        t0: float | None = None,
        oraculo: Oraculo | None = None,
    ):
        self.config = config
        self.contexto = {"run": None, "rep": None, "mode": None, "scenario": None, **(contexto or {})}
        self.sinks = list(sinks or [])
        self.t0 = time.monotonic() if t0 is None else t0
        self.oraculo = oraculo or Oraculo()
        self._fase = "baseline"
        self._parar = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lock_versao = threading.Lock()
        self._versoes: dict[str, int] = {}
        self._lock_leituras = threading.Lock()
        self._leituras_pendentes: list[Evento] = []
        self._contagem: Counter = Counter()
        self._lock_emissao = threading.Lock()
        self._erros_threads: list[BaseException] = []
        self.chaves_frias: list[str] = []  # chaves frias com PUT confirmado

    # ------------------------------------------------------------ controle

    @property
    def fase(self) -> str:
        return self._fase

    @fase.setter
    def fase(self, valor: str) -> None:
        self._fase = valor

    def agora(self) -> float:
        """Tempo relativo ao início da execução (s), no relógio monotônico do host."""
        return time.monotonic() - self.t0

    def iniciar(self) -> None:
        c = self.config
        for n in c.escritores:
            self._lancar(f"escritor-quente-g{n}", self._escritor, n, c.chave_quente, c.taxa_escrita, False)
            if c.taxa_fria > 0:
                self._lancar(f"escritor-frio-g{n}", self._escritor, n, c.prefixo_frias, c.taxa_fria, True)
        for n in c.leitores:
            self._lancar(f"leitor-g{n}", self._leitor, n, c.chave_quente, c.taxa_leitura)

    def parar(self) -> None:
        self._parar.set()
        for t in self._threads:
            t.join()
        self._threads.clear()
        self._despachar_leituras(forcar=True)
        if self._erros_threads:
            raise RuntimeError(f"falha numa thread da carga: {self._erros_threads[0]!r}") from self._erros_threads[0]

    def executar_por(self, segundos: float) -> None:
        self.iniciar()
        try:
            self._parar.wait(segundos)
        finally:
            self.parar()

    def resumo(self) -> dict[str, int]:
        """Contagem por "op/nó/classe" (leituras) e "op/nó/status" (escritas)."""
        return dict(sorted(self._contagem.items()))

    # ------------------------------------------------------------ internos

    def _lancar(self, nome: str, alvo, *args) -> None:
        def protegido() -> None:
            try:
                alvo(*args)
            except BaseException as e:  # noqa: BLE001 — repassada por parar()
                self._erros_threads.append(e)
                self._parar.set()

        t = threading.Thread(target=protegido, name=nome, daemon=True)
        self._threads.append(t)
        t.start()

    def _ritmo(self, taxa: float):
        """Gera os instantes de disparo; sem rajada depois de atraso."""
        intervalo = 1.0 / taxa
        proximo = time.monotonic()
        while not self._parar.is_set():
            espera = proximo - time.monotonic()
            if espera > 0 and self._parar.wait(espera):
                return
            yield
            proximo = max(proximo + intervalo, time.monotonic())

    def _proxima_versao(self, chave: str) -> int:
        with self._lock_versao:
            self._versoes[chave] = self._versoes.get(chave, 0) + 1
            return self._versoes[chave]

    def _emitir(self, evento: Evento) -> None:
        with self._lock_emissao:
            self._contagem[f"{evento['op']}/g{evento['node']}/{evento['class'] or evento['status']}"] += 1
            for sink in self.sinks:
                sink(evento)

    def _evento(self, op: str, no: int, chave: str, res: lab_s3.Resultado, fase: str) -> Evento:
        return {
            **self.contexto,
            "phase": fase,  # fase no INÍCIO da operação
            "op": op,
            "node": no,
            "key": chave,
            "t_start": round(res.t_inicio - self.t0, 4),
            "t_end": round(res.t_fim - self.t0, 4),
            "status": "ok" if res.ok else "error",
            "http": res.http,
            "error": res.erro,
            "version_written": None,
            "version_read": None,
            "class": None,
            "motivo": None,
            "lag_versions": None,
            "lag_ms": None,
        }

    def _escritor(self, no: int, chave_ou_prefixo: str, taxa: float, frias: bool) -> None:
        s3 = lab_s3.cliente_s3(no, connect_timeout=self.config.connect_timeout, read_timeout=self.config.read_timeout)
        lado = self.config.lados.get(no, "maj")
        for i, _ in enumerate(self._ritmo(taxa), start=1):
            chave = f"{chave_ou_prefixo}g{no}-{i}" if frias else chave_ou_prefixo
            versao = self._proxima_versao(chave)
            corpo = json.dumps({"v": versao, "writer": lado, "node": no, "t": round(self.agora(), 4)}).encode()
            fase = self._fase
            self.oraculo.inicio_escrita(chave, versao, lado)
            res = lab_s3.put(s3, no, chave, corpo, {"version": str(versao)})
            self.oraculo.fim_escrita(chave, versao, res.t_fim, res.ok)
            if frias and res.ok:
                self.chaves_frias.append(chave)
            evento = self._evento("PUT", no, chave, res, fase)
            evento["version_written"] = versao
            self._emitir(evento)
            self._despachar_leituras()

    def _leitor(self, no: int, chave: str, taxa: float) -> None:
        s3 = lab_s3.cliente_s3(no, connect_timeout=self.config.connect_timeout, read_timeout=self.config.read_timeout)
        for _ in self._ritmo(taxa):
            fase = self._fase
            res = lab_s3.get(s3, no, chave)
            evento = self._evento("GET", no, chave, res, fase)
            evento["_res"] = res
            with self._lock_leituras:
                self._leituras_pendentes.append(evento)
            self._despachar_leituras()

    def _despachar_leituras(self, forcar: bool = False) -> None:
        """Classifica e emite as leituras cuja classificação já é definitiva (ou todas, no fim)."""
        with self._lock_leituras:
            prontas, pendentes = [], []
            for ev in self._leituras_pendentes:
                res = ev["_res"]
                (prontas if forcar or self.oraculo.pronto(ev["key"], res.t_fim) else pendentes).append(ev)
            self._leituras_pendentes = pendentes
        for ev in prontas:
            res: lab_s3.Resultado = ev.pop("_res")
            nosuchkey = bool(res.erro and res.erro.startswith("NoSuchKey"))
            versao = _versao_do_corpo(res.dados) if res.ok else None
            c = self.oraculo.classificar(
                ev["key"], res.t_inicio, res.t_fim, versao, erro=None if res.ok else res.erro, nosuchkey=nosuchkey
            )
            ev.update(
                {
                    "version_read": 0 if nosuchkey else versao,
                    "class": c.classe,
                    "motivo": c.motivo,
                    "lag_versions": c.lag_versions,
                    "lag_ms": round(c.lag_ms, 1) if c.lag_ms is not None else None,
                }
            )
            self._emitir(ev)


def _versao_do_corpo(dados: bytes | None) -> int | None:
    try:
        return int(json.loads(dados)["v"])
    except (TypeError, ValueError, KeyError):
        return None
