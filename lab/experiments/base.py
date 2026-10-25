"""Classe base dos experimentos.

Cuida do que é comum a todos: diretório da execução, manifest, cópia da previsão, repetições,
captura de exceções e restauração do cluster no `finally` (cura partições, religa nós, volta a
`consistent`). Cada experimento implementa `run_rep(i)` e, se quiser, `precondition()` e `analyze()`.
"""

from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from importlib import metadata
from pathlib import Path

from lab import cluster, evidence, garage, journal, report
from lab.config import NOS, RAIZ, Topologia, modo_atual
from lab.journal import DIR_RESULTS, agora_iso

DIR_PREDICOES = RAIZ / "predicoes"


def fase_por_intervalo(ts: float, te: float, marcos: dict) -> str:
    """Fase de uma operação pelo intervalo inteiro [ts, te].

    baseline: termina antes de `partition_start`; partition: começa depois de `partition_at` e termina antes de
    `heal_start`; heal: começa depois de `heal_at`; senão "transicao" (atravessa uma fronteira; reportada à parte).
    """
    if te < marcos["partition_start"]:
        return "baseline"
    if ts >= marcos["partition_at"] and te < marcos["heal_start"]:
        return "partition"
    if ts >= marcos["heal_at"]:
        return "heal"
    return "transicao"


def monitorar_visao(api: garage.AdminAPI, t0: float, parar: threading.Event, saida: list, nos=(1, 3)) -> None:
    """A cada 1 s, quem cada nó de `nos` vê como ativo (GetClusterStatus), até `parar`; mede a detecção (E6, E9).

    Cada amostra: {"t": s desde t0, "no": n, "ve_up": [hostnames] ou None se a Admin API não respondeu}.
    """
    while not parar.is_set():
        for n in nos:
            try:
                st = api.cluster_status(n)
                up = sorted(x["hostname"] for x in st["nodes"] if x.get("isUp") and x.get("hostname"))
            except garage.ErroGarage:
                up = None
            saida.append({"t": round(time.monotonic() - t0, 2), "no": n, "ve_up": up})
        parar.wait(1.0)


class PrecondicaoFalhou(Exception):
    """O cluster não está no estado que o experimento exige; nada foi executado."""


def _cmd(args: list[str]) -> str | None:
    try:
        return subprocess.run(args, capture_output=True, text=True, check=True, cwd=RAIZ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _versao_pacote(nome: str) -> str | None:
    try:
        return metadata.version(nome)
    except metadata.PackageNotFoundError:
        return None


def _host() -> dict:
    cpu = None
    try:
        for linha in Path("/proc/cpuinfo").read_text().splitlines():
            if linha.startswith("model name"):
                cpu = linha.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    ram = None
    try:
        for linha in Path("/proc/meminfo").read_text().splitlines():
            if linha.startswith("MemTotal"):
                ram = int(linha.split()[1]) * 1024
                break
    except OSError:
        pass
    return {
        "cpu": cpu,
        "nucleos": os.cpu_count(),
        "ram_bytes": ram,
        "kernel": platform.release(),
        "so": platform.platform(),
    }


class Experiment:
    """Base de um experimento. Subclasses definem `nome` e implementam `run_rep(i)`."""

    nome: str = "E?"
    descricao: str = ""
    modo_esperado: str = "consistent"  # estado padrão exigido no início (os modos mudam dentro da repetição)
    reps_padrao: int = 5  # roteiro R7: ≥ 5 repetições quando há medição
    reps_quick: int = 1

    def __init__(
        self,
        reps: int | None = None,
        quick: bool = False,
        modo: str | None = None,
        cenario: str | None = None,
        parametros: dict | None = None,
        base: Path = DIR_RESULTS,
    ):
        self.quick = quick
        self.reps = reps if reps is not None else (self.reps_quick if quick else self.reps_padrao)
        self.modo = modo or self.modo_esperado
        self.cenario = cenario
        self.parametros = parametros or {}
        self.inicio = datetime.now()
        self.dir = base / self.nome / self.inicio.strftime("%Y-%m-%dT%H-%M-%S")
        self.api = garage.AdminAPI()
        self.manifest: dict = {}
        self.repeticoes: list[dict] = []

    # ------------------------------------------------------------ ganchos das subclasses

    def precondition(self) -> None:
        """Padrão: 3 nós rodando, na rede interna, /health 200 e no modo esperado."""
        problemas = []
        for n in NOS:
            e = cluster.estado(n)
            if e["status"] != "running":
                problemas.append(f"garage{n} não está rodando ({e['status']})")
            elif not e["na_interna"]:
                problemas.append(f"garage{n} está fora da rede interna")
            elif self.api.health(n) != 200:
                problemas.append(f"garage{n}: /health ≠ 200")
        if modo_atual() != self.modo:
            problemas.append(f"modo atual {modo_atual()!r}, esperado {self.modo!r}")
        if not problemas:
            try:
                garage.esperar_malha(NOS, limite_s=60, api=self.api)  # todos veem todos
            except garage.ErroGarage as e:
                problemas.append(str(e))
        if problemas:
            raise PrecondicaoFalhou("; ".join(problemas))

    def run_rep(self, i: int) -> dict | None:
        raise NotImplementedError

    def teardown(self) -> dict:
        """Padrão: restaura o cluster (cura, religa, volta a consistent). Subclasses podem estender."""
        return garage.restaurar()

    def analyze(self) -> dict | None:
        """Devolve {"resultados": [{id, resultado, situacao}], "secoes": [(título, md)], "figuras": [nomes]}."""
        return None

    # ------------------------------------------------------------ ajudantes

    @property
    def dir_evidencias(self) -> Path:
        return self.dir / "evidence"

    @property
    def dir_figuras(self) -> Path:
        return self.dir / "figures"

    def evidencia(self, nome: str, comando: str, coletar) -> Path:
        """Grava a saída de `coletar()` em evidence/<nome> com cabeçalho (comando, início, duração, situação)."""
        return evidence.salvar(self.dir_evidencias, nome, comando, coletar)

    def gravar_csv(self, nome: str, linhas: list[dict]) -> Path:
        arq = self.dir / nome
        if not linhas:
            arq.write_text("")
            return arq
        campos = list(dict.fromkeys(k for linha in linhas for k in linha))
        with arq.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=campos)
            w.writeheader()
            w.writerows(linhas)
        return arq

    # ------------------------------------------------------------ manifest

    def _montar_manifest(self) -> dict:
        topo = Topologia.do_env()
        try:
            versoes_garage = sorted({n.get("garageVersion") for n in self.api.cluster_status(1)["nodes"]})
        except garage.ErroGarage:
            versoes_garage = None
        sujo = _cmd(["git", "status", "--porcelain"])
        return {
            "experimento": self.nome,
            "descricao": self.descricao,
            "quick": self.quick,
            "modo": self.modo,
            "cenario": self.cenario,
            "parametros": self.parametros,
            "reps": self.reps,
            "inicio": agora_iso(),
            "fim": None,
            "git": {"commit": _cmd(["git", "rev-parse", "HEAD"]), "alteracoes_locais": bool(sujo)},
            "versoes": {
                "garage": versoes_garage,
                "docker": _cmd(["docker", "version", "--format", "{{.Server.Version}}"]),
                "compose": _cmd(["docker", "compose", "version", "--short"]),
                "python": sys.version.split()[0],
                "boto3": _versao_pacote("boto3"),
            },
            "host": _host(),
            "redes": {  # faixas efetivas desta máquina
                "interna": str(topo.subrede_interna),
                "clientes": str(topo.subrede_clientes),
                "ips_internos": {f"garage{n}": topo.ip_interno(n) for n in NOS},
                "ips_clientes": {f"garage{n}": topo.ip_clientes(n) for n in NOS},
            },
        }

    def _gravar_manifest(self) -> None:
        self.manifest["repeticoes"] = self.repeticoes
        (self.dir / "manifest.json").write_text(json.dumps(self.manifest, indent=2, ensure_ascii=False, default=str))

    # ------------------------------------------------------------ execução

    def run(self) -> Path:
        """Executa pré-condição, repetições e análise; restaura o cluster sempre (finally)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self._montar_manifest()
        previsao = DIR_PREDICOES / f"{self.nome}.yaml"
        if previsao.exists():
            shutil.copy(previsao, self.dir / "predicao.yaml")
        journal.registrar(
            "run",
            {
                "experimento": self.nome,
                "reps": self.reps,
                "quick": self.quick,
                "modo": self.modo,
                "cenario": self.cenario,
            },
            journal.relativo(self.dir),
        )
        try:
            self.precondition()
            for i in range(self.reps):
                t0 = time.monotonic()
                registro: dict = {"rep": i, "inicio": agora_iso()}
                try:
                    registro["resultado"] = self.run_rep(i)
                    registro["situacao"] = "ok"
                except Exception as e:  # noqa: BLE001 — qualquer falha da repetição é registrada
                    registro["situacao"] = "erro"
                    registro["erro"] = f"{type(e).__name__}: {e}"
                    registro["traceback"] = traceback.format_exc()
                    garage.restaurar()  # a próxima repetição começa do estado padrão
                registro["duracao_s"] = round(time.monotonic() - t0, 3)
                self.repeticoes.append(registro)
                self._gravar_manifest()
        except PrecondicaoFalhou as e:
            self.manifest["precondicao"] = f"falhou: {e}"
            raise
        finally:
            try:
                self.manifest["restauracao"] = self.teardown()
            finally:
                self.manifest["fim"] = agora_iso()
                self._gravar_manifest()
        analise = self.analyze()
        self.manifest["analise"] = analise
        self._gravar_manifest()
        report.gerar_summary(self.dir, analise)
        return self.dir
