"""E7 — Recuperação (roteiro §4.2 e §7.3).

Cada repetição tem dois ciclos, um com `repair` e outro com convergência natural:
  particiona garage3 → escreve M objetos via garage1 (maioria; 150 pequenos dentro dos metadados + 50 de 16 KiB
  com bloco) → mantém a partição até 120 s (maior que a janela de detecção) → cura → espera a malha completa →
  (reparo) dispara LaunchRepairOperation tables e blocks em todos os nós | (natural) só observa →
  mede o tempo até convergir.
Convergência (sem ler objetos, para não disparar repair-on-read, C4): pelas estatísticas de cada nó
(GetNodeStatistics): contagens iguais nas tabelas object, version e block_ref, filas de Merkle e de inserção
zeradas, mesmas entradas de blocos e fila de resync zerada; e `du -sb` de data/ igual nos 3 nós.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
import uuid

from lab import cluster, garage, plots, stats
from lab import s3 as lab_s3
from lab.config import NOS, RAIZ
from lab.experiments.base import Experiment
from lab.report import tabela_md

TABELAS = ("object", "version", "block_ref")
PEQUENOS, COM_BLOCO = 150, 50


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


def _du() -> dict[str, int]:
    alvos = [str(RAIZ / "dados" / f"g{n}" / "data") for n in NOS]
    saida = subprocess.run(["du", "-sb", *alvos], capture_output=True, text=True, check=True).stdout
    return {f"garage{n}": int(linha.split()[0]) for n, linha in zip(NOS, saida.splitlines(), strict=True)}


class E7(Experiment):
    nome = "E7"
    descricao = "Recuperação"

    def _estado(self) -> dict:
        por_no = {}
        for n in NOS:
            r = self.api._json("GET", n, "GetNodeStatistics", params={"node": "self"})
            freeform = next(iter(r["success"].values()))["freeform"]
            por_no[f"garage{n}"] = ler_estatisticas_no(freeform)
        return {"nos": por_no, "du": _du()}

    @staticmethod
    def convergido(estado: dict) -> tuple[bool, list[str]]:
        """Critério de convergência; devolve (ok, motivos de não convergência)."""
        motivos, nos = [], estado["nos"]
        for t in TABELAS:
            itens = {no: d["tabelas"].get(t, {}).get("Items") for no, d in nos.items()}
            if len(set(itens.values())) != 1:
                motivos.append(f"{t}: itens diferentes {itens}")
            filas = {
                no: (d["tabelas"].get(t, {}).get("MklTodo", 0), d["tabelas"].get(t, {}).get("InsQueue", 0))
                for no, d in nos.items()
            }
            if any(a or b for a, b in filas.values()):
                motivos.append(f"{t}: filas (MklTodo, InsQueue) {filas}")
        rc = {no: d["blocos"].get("rc") for no, d in nos.items()}
        if len(set(rc.values())) != 1:
            motivos.append(f"blocos (RC) diferentes {rc}")
        if any(d["blocos"].get("resync", 0) for d in nos.values()):
            motivos.append("fila de resync de blocos > 0")
        du = estado["du"]
        if max(du.values()) - min(du.values()) > 64 * 1024:
            motivos.append(f"du de data/ diferente {du}")
        return not motivos, motivos

    def _ciclo(self, i: int, tipo: str) -> dict:
        particao_s = 90 if self.quick else 120
        limite_natural = 150 if self.quick else 15 * 60
        cliente = lab_s3.cliente_s3(1)
        prefixo = f"e7/{tipo}/r{i}-{uuid.uuid4().hex[:6]}"
        t0 = time.monotonic()
        cluster.particionar(3)
        escritos = 0
        for k in range(PEQUENOS + COM_BLOCO):
            corpo = os.urandom(100) if k < PEQUENOS else os.urandom(16 * 1024)
            escritos += lab_s3.put(cliente, 1, f"{prefixo}/{k:04d}", corpo).ok
        restante = particao_s - (time.monotonic() - t0)
        if restante > 0:
            time.sleep(restante)
        cluster.curar(3)
        t_malha = garage.esperar_malha(limite_s=180, api=self.api)
        t_cura = time.monotonic()
        logo_apos = self._estado()
        self.evidencia(
            f"rep{i}_{tipo}_estado_logo_apos_cura.txt", "GetNodeStatistics (3 nós) + du -sb", lambda: logo_apos
        )
        reparos = {}
        if tipo == "reparo":
            for alvo in ("tables", "blocks"):
                reparos[alvo] = self.api.launch_repair(1, alvo, "*")
        limite = 300 if tipo == "reparo" else limite_natural
        t_conv, ultimo_motivo, amostras = None, None, 0
        while time.monotonic() - t_cura <= limite:
            estado = self._estado()
            amostras += 1
            ok, motivos = self.convergido(estado)
            if ok:
                t_conv = round(time.monotonic() - t_cura, 1)
                break
            ultimo_motivo = motivos
            time.sleep(2 if tipo == "reparo" else 10)
        final = self._estado()
        self.evidencia(f"rep{i}_{tipo}_estado_final.txt", "GetNodeStatistics (3 nós) + du -sb", lambda: final)
        return {
            "tipo": tipo,
            "escritos": escritos,
            "t_malha_s": round(t_malha, 1),
            "t_convergencia_s": t_conv,
            "limite_s": limite,
            "amostras": amostras,
            "motivos_nao_convergencia": ultimo_motivo,
            "logo_apos": logo_apos,
            "final": final,
            "reparos": reparos,
        }

    def run_rep(self, i: int) -> dict:
        try:
            return {"reparo": self._ciclo(i, "reparo"), "natural": self._ciclo(i, "natural")}
        finally:
            garage.restaurar()

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}

        def deficit(estado):
            """Itens de object/version/block_ref que faltam no garage3 em relação ao máximo."""
            nos = estado["nos"]
            return {
                t: max(d["tabelas"][t]["Items"] for d in nos.values()) - nos["garage3"]["tabelas"][t]["Items"]
                for t in TABELAS
            }

        linhas_csv, tab = [], []
        for k, r in enumerate(reps):
            for tipo in ("reparo", "natural"):
                c = r[tipo]
                d = deficit(c["logo_apos"])
                linhas_csv.append(
                    {
                        "rep": k,
                        "ciclo": tipo,
                        "t_convergencia_s": c["t_convergencia_s"],
                        "t_malha_s": c["t_malha_s"],
                        **{f"faltava_{t}": v for t, v in d.items()},
                    }
                )
                tab.append(
                    [
                        k,
                        tipo,
                        c["escritos"],
                        ", ".join(f"{t} {v}" for t, v in d.items()),
                        c["t_malha_s"],
                        c["t_convergencia_s"] if c["t_convergencia_s"] is not None else f"> {c['limite_s']} s",
                    ]
                )
        self.gravar_csv("metrics.csv", linhas_csv)

        def tempos(tipo):
            return [r[tipo]["t_convergencia_s"] for r in reps]

        def resumo(v):
            x = [a for a in v if a is not None]
            if not x:
                return "nenhuma convergência no limite"
            s = stats.resumir(x)
            extra = f"; {len(v) - len(x)} sem convergir no limite" if len(x) < len(v) else ""
            return f"{s.media:.1f} ± {0 if s.n < 2 else s.desvio:.1f} s (mín {s.minimo:.1f}, máx {s.maximo:.1f}, n={s.n}{extra})"

        faltou = [
            any(v > 0 for v in deficit(r[tipo]["logo_apos"]).values()) for r in reps for tipo in ("reparo", "natural")
        ]
        nat, rep = tempos("natural"), tempos("reparo")
        p1 = all(faltou)
        p2 = all(t is not None and t <= 11 * 60 for t in nat)
        p3 = all(t is not None for t in rep) and all(
            r is not None and (n is None or r < n) for r, n in zip(rep, nat, strict=True)
        )
        p4 = all(
            self.convergido(r[tipo]["final"])[0]
            for r in reps
            for tipo in ("reparo", "natural")
            if r[tipo]["t_convergencia_s"] is not None
        )
        medias = [
            stats.resumir([x for x in v if x is not None]).media if any(x is not None for x in v) else 0
            for v in (rep, nat)
        ]
        plots.salvar(
            plots.barras_com_erro(
                ["com repair", "natural"],
                medias,
                [0, 0],
                "Tempo até convergir após a malha voltar",
                "s",
                ["#2f855a", "#4a5568"],
                linha_limite=(600, "10 min (código, C5b)"),
            ),
            self.dir_figuras,
            "tempo_de_convergencia",
        )
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": f"garage3 com itens faltando logo após a malha voltar em {sum(faltou)}/{len(faltou)} ciclos",
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else ("inconclusiva" if any(t is None for t in nat) else "refutada"),
                "resultado": f"convergência natural: {resumo(nat)}",
            },
            {"id": "P3", "situacao": "confirmada" if p3 else "refutada", "resultado": f"com repair: {resumo(rep)}"},
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "estado final idêntico (critério) em todos os ciclos que convergiram"
                if p4
                else "algum ciclo convergiu no critério mas o estado final divergiu",
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "Ciclos",
                    tabela_md(
                        [
                            "rep",
                            "ciclo",
                            "objetos escritos na partição",
                            "faltando no garage3 logo após a cura",
                            "malha (s)",
                            "convergência (s)",
                        ],
                        tab,
                    ),
                ),
                (
                    "Critério de convergência",
                    (
                        "Pelas estatísticas de cada nó (GetNodeStatistics), sem ler objetos: itens iguais em object, version e "
                        "block_ref; MklTodo e InsQueue zerados; mesmas entradas de blocos (RC) e fila de resync zerada; `du -sb` "
                        "de data/ igual (tolerância 64 KiB). Tempo contado a partir da malha completa após curar."
                    ),
                ),
            ],
            "figuras": ["tempo_de_convergencia"],
        }
