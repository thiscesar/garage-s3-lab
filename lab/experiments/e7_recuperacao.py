"""E7 — Recuperação (roteiro §4.2 e §7.3).

Cada repetição tem dois ciclos, um com `repair` e outro com convergência natural:
  particiona garage3 → escreve M objetos via garage1 (maioria; 150 pequenos dentro dos metadados + 50 de 16 KiB
  com bloco) → mantém a partição até 120 s (maior que a janela de detecção) → cura → espera a malha completa →
  (reparo) dispara LaunchRepairOperation tables e blocks em todos os nós | (natural) só observa →
  mede o tempo até convergir.
Convergência (sem ler objetos, para não disparar repair-on-read, C4): pelas estatísticas de cada nó
(GetNodeStatistics): contagens iguais nas tabelas object, version e block_ref, filas de Merkle e de inserção
zeradas, mesmas entradas de blocos e fila de resync zerada; e `du -sb` de data/ igual nos 3 nós.

Ordem dos ciclos e efeito do reparo (código Garage v2.3.0, commit 7b119c0):
- `LaunchRepairOperation tables` (o mesmo que `garage repair tables` do roteiro E7.2) chama `add_full_sync()` em
  todas as tabelas (src/api/admin/repair.rs l. 43–47). O worker de sincronização então agenda a próxima
  anti-entropia para `Instant::now() + ANTI_ENTROPY_INTERVAL` (src/table/sync.rs l. 504–521, l. 520), 10 min nas
  tabelas particionadas (src/table/replication/sharded.rs l. 27). Fora isso, ele só sincroniza quando chega essa
  hora, quando o layout muda ou num novo reparo (sync.rs l. 580–596); ao iniciar o nó, a 1ª é 20 s depois (l. 86).
- Na execução de referência, cada repetição roda o ciclo com reparo e logo depois o natural. O ciclo natural
  começa, então, com o relógio da anti-entropia zerado pelo reparo anterior: os ~475 s medidos são
  600 s − (tempo desde o reparo até a cura: convergência do reparo + 120 s de partição + cura) e confirmam o
  intervalo de 10 min do código, mas NÃO são o tempo natural de recuperação de uma falha real, que pode ser
  qualquer um entre 0 e 10 min (mais 600 s para os dados de bloco, block/manager.rs l. 474–485).
- `--scenario natural-primeiro` (opcional, não é a referência): roda o ciclo natural antes do de reparo, com uma
  pausa aleatória de 0 a 600 s antes dele, e registra em cada ciclo o tempo desde o último reparo disparado pela
  execução (o laboratório não sabe de reparos ou reinícios anteriores a ela).
"""

from __future__ import annotations

import os
import random
import time
import uuid

from lab import cluster, garage, plots, stats
from lab import s3 as lab_s3
from lab.config import NOS
from lab.experiments.base import Experiment
from lab.report import tabela_md

TABELAS = ("object", "version", "block_ref")
PEQUENOS, COM_BLOCO = 150, 50
CENARIOS = {None: ("reparo", "natural"), "natural-primeiro": ("natural", "reparo")}
PAUSA_MAX_S = 600  # natural-primeiro: pausa aleatória antes do ciclo natural (fase uniforme no ciclo de 10 min)


class E7(Experiment):
    nome = "E7"
    descricao = "Recuperação"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.cenario not in CENARIOS:
            raise ValueError(f"cenário inválido: {self.cenario} (use natural-primeiro ou nenhum)")
        self.ordem = CENARIOS[self.cenario]
        self.parametros = {"ordem": list(self.ordem)}
        self._ultimo_reparo: float | None = None  # instante (monotônico) do último repair tables desta execução

    def _estado(self) -> dict:
        por_no = {f"garage{n}": self.api.estatisticas_no(n) for n in NOS}
        return {"nos": por_no, "du": cluster.uso_disco_dados()}

    @staticmethod
    def convergido(estado: dict) -> tuple[bool, list[str]]:
        """Critério de convergência; devolve (ok, motivos de não convergência).

        A fila de resync de blocos NÃO entra: todo bloco novo é agendado para conferência 2 × rpc_timeout
        (600 s) depois de escrito (block/manager.rs l. 474–485), então ela fica > 0 por ~10 min sem divergência.
        """
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
        du = estado["du"]
        if max(du.values()) - min(du.values()) > 64 * 1024:
            motivos.append(f"du de data/ diferente {du}")
        return not motivos, motivos

    def _ciclo(self, i: int, tipo: str) -> dict:
        particao_s = 90 if self.quick else 120
        limite_natural = 150 if self.quick else 25 * 60  # dados de bloco ~ tabelas + 600 s
        cliente = lab_s3.cliente_s3(1)
        prefixo = f"e7/{tipo}/r{i}-{uuid.uuid4().hex[:6]}"
        pausa = None
        if self.cenario == "natural-primeiro" and tipo == "natural":
            pausa = round(random.uniform(0, 30 if self.quick else PAUSA_MAX_S), 1)
            time.sleep(pausa)
        t0 = time.monotonic()
        cluster.particionar(3)
        escritos = 0
        for k in range(PEQUENOS + COM_BLOCO):
            corpo = os.urandom(100) if k < PEQUENOS else os.urandom(16 * 1024)
            escritos += lab_s3.put(cliente, 1, f"{prefixo}/{k:04d}", corpo).ok
        restante = particao_s - (time.monotonic() - t0)
        if restante > 0:
            time.sleep(restante)
        # P1: retrato no fim da partição, antes de curar (a Admin API do garage3 responde pela rede clientes)
        antes_cura = self._estado()
        self.evidencia(
            f"rep{i}_{tipo}_estado_antes_da_cura.txt", "GetNodeStatistics (3 nós) + du -sb", lambda: antes_cura
        )
        cluster.curar(3)
        t_cura = time.monotonic()
        desde_reparo = None if self._ultimo_reparo is None else round(t_cura - self._ultimo_reparo, 1)
        reparos, t_malha, t_tabelas = {}, None, None
        limite = 600 if tipo == "reparo" else limite_natural
        t_conv, ultimo_motivo, amostras = None, None, 0
        while time.monotonic() - t_cura <= limite:
            agora = time.monotonic() - t_cura
            if t_malha is None and all(v == {f"garage{n}" for n in NOS} for v in garage.malha(self.api).values()):
                t_malha = agora
                if tipo == "reparo":  # o repair precisa da malha completa; 1º tables (roteiro E7.2)
                    reparos["tables"] = self.api.launch_repair(1, "tables", "*")
                    self._ultimo_reparo = time.monotonic()
            estado = self._estado()
            amostras += 1
            ok, motivos = self.convergido(estado)
            so_disco = all(m.startswith("du ") for m in motivos)  # tabelas e RC já iguais; falta dado de bloco
            if t_tabelas is None and t_malha is not None and (ok or so_disco):
                t_tabelas = round(agora, 1)
                if tipo == "reparo":  # blocks só depois de as tabelas convergirem
                    reparos["blocks"] = self.api.launch_repair(1, "blocks", "*")
            if ok and t_malha is not None:
                t_conv = round(time.monotonic() - t_cura, 1)
                break
            ultimo_motivo = motivos
            time.sleep(2 if tipo == "reparo" else 5)
        if t_malha is None:
            t_malha = garage.esperar_malha(limite_s=180, api=self.api)
        final = self._estado()
        self.evidencia(f"rep{i}_{tipo}_estado_final.txt", "GetNodeStatistics (3 nós) + du -sb", lambda: final)
        return {
            "tipo": tipo,
            "escritos": escritos,
            "t_malha_s": round(t_malha, 1),
            "antes_cura": antes_cura,
            "t_convergencia_s": t_conv,
            "t_tabelas_s": t_tabelas,
            "limite_s": limite,
            "amostras": amostras,
            "motivos_nao_convergencia": ultimo_motivo,
            "final": final,
            "reparos": reparos,
            "pausa_antes_s": pausa,
            "desde_ultimo_reparo_s": desde_reparo,
        }

    def run_rep(self, i: int) -> dict:
        try:
            return {tipo: self._ciclo(i, tipo) for tipo in self.ordem}
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
        ordem = tuple((self.parametros or {}).get("ordem", ("reparo", "natural")))
        for k, r in enumerate(reps):
            for tipo in ordem:
                c = r[tipo]
                d = deficit(c["antes_cura"])
                linhas_csv.append(
                    {
                        "rep": k,
                        "ciclo": tipo,
                        "t_convergencia_s": c["t_convergencia_s"],
                        "t_malha_s": c["t_malha_s"],
                        "desde_ultimo_reparo_s": c.get("desde_ultimo_reparo_s"),
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
                        c.get("t_tabelas_s") if c.get("t_tabelas_s") is not None else "—",
                        c["t_convergencia_s"] if c["t_convergencia_s"] is not None else f"> {c['limite_s']} s",
                        c.get("desde_ultimo_reparo_s") if c.get("desde_ultimo_reparo_s") is not None else "—",
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
            any(v > 0 for v in deficit(r[tipo]["antes_cura"]).values()) for r in reps for tipo in ("reparo", "natural")
        ]
        nat, rep = tempos("natural"), tempos("reparo")
        p1 = all(faltou)
        p2 = all(t is not None and t <= 11 * 60 for t in nat)  # critério pré-registrado (P2: ~10 min do código)
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
                "Tempo até convergir após curar",
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
                "resultado": f"garage3 com itens faltando ao fim da partição (antes de curar) em {sum(faltou)}/{len(faltou)} ciclos",
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
                            "faltando no garage3 antes da cura",
                            "malha (s)",
                            "tabelas iguais (s)",
                            "convergência total, com dados de bloco (s)",
                            "desde o último reparo, na cura (s)",
                        ],
                        tab,
                    ),
                ),
                (
                    "Ordem dos ciclos e efeito do reparo",
                    (
                        f"Ordem nesta execução: {' → '.join(ordem)}. O `repair tables` (LaunchRepairOperation, o mesmo "
                        "`add_full_sync()` do comando `garage repair tables` do roteiro: src/api/admin/repair.rs l. 43–47) "
                        "reagenda a próxima anti-entropia para 10 min depois (src/table/sync.rs l. 520; "
                        "src/table/replication/sharded.rs l. 27). Quando o ciclo natural vem logo depois de um ciclo com "
                        "reparo, as tabelas convergem cerca de 600 s − (tempo do reparo até a cura) depois da cura: o "
                        "número confirma o intervalo de 10 min do código, mas é efeito do desenho, não o tempo natural de "
                        "uma falha real (entre 0 e 10 min, conforme a fase do ciclo, mais ~600 s para os dados de bloco). "
                        'A coluna "desde o último reparo" só é registrada a partir da .'
                    ),
                ),
                (
                    "Critério de convergência",
                    (
                        "Pelas estatísticas de cada nó (GetNodeStatistics), sem ler objetos: itens iguais em object, version e "
                        "block_ref; MklTodo e InsQueue zerados; mesmas entradas de blocos (RC); `du -sb` "
                        "de data/ igual (tolerância 64 KiB). A fila de resync não entra (conferência agendada de blocos novos, 600 s; block/manager.rs l. 474–485). Tempo contado a partir da cura (connect); o repair é disparado "
                        "quando a malha fica completa."
                    ),
                ),
            ],
            "figuras": ["tempo_de_convergencia"],
        }
