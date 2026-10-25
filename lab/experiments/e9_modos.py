"""E9 — Ênfase A: modos de consistência sob partição (roteiro §7.5).

Cenário A1 nos 3 modos (consistent, degraded, dangerous) × 5 repetições. Cada repetição roda uma rodada por
modo, com a ordem dos modos girando de uma repetição para a outra. Rodada:

  troca de modo (malha completa) → aquecimento 5 s sem carga → baseline 10 s → partição do garage3 por 120 s
 → cura → malha completa → +10 s → para a carga → verificação.

Carga: escritor via garage1 (maioria) — 10 PUT/s na chave quente + 2 PUT/s em chaves frias novas;
leitores da chave quente via garage2 (maioria) e garage3 (minoria), 10 GET/s cada. Na mesma partição, sem custo
de tempo: listagem das chaves frias via garage2 e garage3 a cada 1 s (A5) e uma sonda de escrita de
1 PUT/s via garage3 numa chave própria (quem aceita escrita na minoria). Um monitor lê GetClusterStatus de
garage1 e garage3 a cada 1 s (janela de detecção, como no E6).

Classificação: leitura "respondeu" = classe fresh ou stale (NoSuchKey incluído);
erro separado em timeout × "Could not reach quorum" × outro; disponibilidade = respondeu / tentativas; taxa de
desatualizadas = stale / respondeu, ou "n/a" sem nenhuma resposta (P9). Fase pelo intervalo inteiro da operação.

Fatos usados (código v2.3.0): os corpos (~60 B) ficam abaixo de INLINE_THRESHOLD = 3072 B
(block/manager.rs l. 44), então PUT e GET só tocam a tabela de objetos (put.rs l. 170–222; get.rs l. 313–317,
451–458), com os quóruns R e W do modo (table/replication/sharded.rs l. 50–68; rpc/replication_mode.rs l. 41–55).
A listagem usa a mesma leitura com quórum (list.rs l. 69–71; table.rs l. 363–454).
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter

from lab import cluster, garage, plots, stats
from lab import s3 as lab_s3
from lab.config import N_REPLICAS, QUORUNS
from lab.experiments.base import Experiment, fase_por_intervalo, monitorar_visao
from lab.report import tabela_md
from lab.workload import ConfigCarga, SinkJSONL, Workload

MODOS = ("consistent", "degraded", "dangerous")
FASES = ("baseline", "partition", "transicao", "heal")
# papel de cada (op, nó) na análise
PAPEIS = {
    ("GET", 3): "leitura minoria (g3)",
    ("GET", 2): "leitura maioria (g2)",
    ("PUT", 1): "escrita maioria (g1)",
    ("PUT", 3): "sonda de escrita minoria (g3)",
    ("LIST", 3): "listagem minoria (g3)",
    ("LIST", 2): "listagem maioria (g2)",
}

# ---------------------------------------------------------------- critérios das previsões (definidos ANTES de rodar)
# P1 consistent, minoria: 0 leituras respondidas e 0 desatualizadas na partição.
# P2 degraded, minoria: disponibilidade ≥ 90% e desatualizadas ≥ 80% das respondidas.
# P3 dangerous, minoria: idem P2; sonda de escrita da minoria ≥ 90% ok em dangerous e ≤ 10% em consistent/degraded.
# P4 maioria (leitor g2): disponibilidade ≥ 95% nos 3 modos; desatualizadas ≤ 1% (consistent, degraded), ≤ 5% (dangerous).
# P5 listagem minoria: 0 respondidas em consistent; em degraded/dangerous ≥ 90% respondidas e ≥ 50% delas incompletas.
# P6 em toda rodada, 1ª leitura atualizada via g3 após curar ≤ (malha completa − heal_at) + 10 s; chaves frias
#    confirmadas na partição faltando via g3 na verificação: soma > 0 em degraded e em dangerous, = 0 em consistent.
DISP_MIN_RESPONDE = 0.90
STALE_MIN_MINORIA = 0.80
SONDA_MIN_DANGEROUS = 0.90
SONDA_MAX_OUTROS = 0.10
DISP_MIN_MAIORIA = 0.95
STALE_MAX_MAIORIA = {"consistent": 0.01, "degraded": 0.01, "dangerous": 0.05}
LISTA_MIN_RESPONDE = 0.90
LISTA_MIN_INCOMPLETA = 0.50
FOLGA_P6_S = 10.0


def _tipo_erro(erro: str) -> str:
    if "Could not reach quorum" in erro:
        return "quorum"
    if "timeout" in erro.lower():
        return "timeout"
    return "outro_erro"


def _respondeu(ev: dict) -> bool:
    if ev["op"] == "GET":
        return ev.get("class") in ("fresh", "stale")  # NoSuchKey classificado entra aqui
    return ev["status"] == "ok"


def _pct(a: int, b: int) -> str:
    return f"{a / b * 100:.1f}%" if b else "n/a"


def _ic(p: stats.Proporcao) -> str:
    return f"{p.p * 100:.1f}% [{p.ic95[0] * 100:.1f}–{p.ic95[1] * 100:.1f}]"


class E9(Experiment):
    nome = "E9"
    descricao = "Ênfase A — modos de consistência sob partição (A1, com listagem e sonda de escrita)"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # --mode restringe as rodadas a um modo; o estado exigido no início continua sendo consistent
        so_um = kwargs.get("modo")
        if so_um is not None and so_um not in MODOS:
            raise ValueError(f"modo inválido: {so_um} (use {', '.join(MODOS)})")
        self.modos = (so_um,) if so_um else MODOS
        self.parametros = {
            "modos": list(self.modos),
            "aquecimento_s": 5,
            "baseline_s": 5 if self.quick else 10,
            "particao_s": 40 if self.quick else 120,
            "pos_malha_s": 10,
            "taxa_escrita": 10,
            "taxa_fria": 2,
            "taxa_leitura": 10,
            "taxa_sonda": 1,
            "intervalo_listagem_s": 1,
        }
        self.modo = self.modo_esperado

    # ------------------------------------------------------------ execução

    def run_rep(self, i: int) -> dict:
        k = i % len(self.modos)
        ordem = self.modos[k:] + self.modos[:k]  # ordem girada entre repetições
        rodadas = {}
        for modo in ordem:
            garage.definir_modo(modo)  # inclui a espera pela malha completa
            time.sleep(self.parametros["aquecimento_s"])
            rodadas[modo] = self._rodada(i, modo)
        return {"ordem": list(ordem), "rodadas": rodadas}

    def _listador(self, w: Workload, no: int, prefixo: str, contexto: dict, sink, parar: threading.Event) -> None:
        s3 = lab_s3.cliente_s3(no)
        while not parar.is_set():
            esperadas = set(w.chaves_frias)  # só chaves com PUT já confirmado antes de listar
            res = lab_s3.listar(s3, no, prefixo)
            fase = w.fase
            vistas = set(res.dados) if res.ok else set()
            sink(
                {
                    **contexto,
                    "scenario": "A5",
                    "phase": fase,
                    "op": "LIST",
                    "node": no,
                    "key": prefixo,
                    "t_start": round(res.t_inicio - w.t0, 4),
                    "t_end": round(res.t_fim - w.t0, 4),
                    "status": "ok" if res.ok else "error",
                    "http": res.http,
                    "error": res.erro,
                    "esperadas": len(esperadas),
                    "faltando": len(esperadas - vistas) if res.ok else None,
                    "truncada": res.ok and len(vistas) >= 1000,  # ListObjectsV2 devolve até 1000 por página
                }
            )
            parar.wait(self.parametros["intervalo_listagem_s"])

    def _rodada(self, i: int, modo: str) -> dict:
        p = self.parametros
        tag = f"e9/{modo}/r{i}-{int(time.time())}"
        contexto = {"run": self.dir.name, "rep": i, "mode": modo, "scenario": "A1"}
        arq_ops = self.dir / f"ops_rep{i}_{modo}.jsonl"
        sink = SinkJSONL(arq_ops)
        w = Workload(
            ConfigCarga(
                escritores=(1,),
                leitores=(2, 3),
                taxa_escrita=p["taxa_escrita"],
                taxa_fria=p["taxa_fria"],
                taxa_leitura=p["taxa_leitura"],
                chave_quente=f"{tag}/hot",
                prefixo_frias=f"{tag}/cold/",
                lados={1: "maj"},
            ),
            contexto,
            [sink],
        )
        sonda = Workload(
            ConfigCarga(
                escritores=(3,),
                leitores=(),
                taxa_escrita=p["taxa_sonda"],
                taxa_fria=0,
                chave_quente=f"{tag}/sonda",
                lados={3: "min"},
            ),
            {**contexto, "scenario": "sonda"},
            [sink],
            t0=w.t0,
        )
        monitor: list = []
        parar = threading.Event()
        auxiliares = [threading.Thread(target=monitorar_visao, args=(self.api, w.t0, parar, monitor))]
        auxiliares += [
            threading.Thread(target=self._listador, args=(w, n, f"{tag}/cold/", contexto, sink, parar)) for n in (2, 3)
        ]
        marcos: dict[str, float] = {}
        try:
            for th in auxiliares:
                th.start()
            w.fase = sonda.fase = "baseline"
            w.iniciar()
            sonda.iniciar()
            time.sleep(p["baseline_s"])
            marcos["partition_start"] = round(w.agora(), 3)  # antes do disconnect
            cluster.particionar(3)
            marcos["partition_at"] = round(w.agora(), 3)  # depois de o disconnect terminar
            w.fase = sonda.fase = "partition"
            time.sleep(p["particao_s"])
            self.evidencia(
                f"rep{i}_{modo}_status_fim_particao.txt",
                "GetClusterStatus garage1 e garage3 (fim da partição)",
                lambda: {n: self.api.cluster_status(n) for n in (1, 3)},
            )
            marcos["heal_start"] = round(w.agora(), 3)  # antes do connect
            cluster.curar(3)
            marcos["heal_at"] = round(w.agora(), 3)
            w.fase = sonda.fase = "heal"
            t_malha = garage.esperar_malha(limite_s=180, api=self.api)
            marcos["converged_at"] = round(w.agora(), 3)
            time.sleep(p["pos_malha_s"])
        finally:
            parar.set()
            for th in auxiliares:
                if th.is_alive():
                    th.join()
            try:
                w.parar()
            finally:
                try:
                    sonda.parar()
                finally:
                    sink.fechar()
        (self.dir / f"phases_rep{i}_{modo}.json").write_text(json.dumps(marcos))
        verificacao = self._verificar(i, modo, w, arq_ops, marcos, tag)
        return {
            "marcos": marcos,
            "t_malha_s": round(t_malha, 2),
            "monitor": monitor,
            "verificacao": verificacao,
        }

    def _verificar(self, i: int, modo: str, w: Workload, arq_ops, marcos: dict, tag: str) -> dict:
        """Depois da carga: chave quente pelos 3 nós; chaves frias confirmadas na partição via garage3 e garage2."""
        quente = f"{tag}/hot"
        confirmadas = [e.versao for e in w.oraculo.escritas(quente) if e.ok]
        ultima = max(confirmadas, default=0)
        lida = {}
        for n in (1, 2, 3):
            res = lab_s3.get(lab_s3.cliente_s3(n), n, quente)
            lida[n] = (json.loads(res.dados)["v"] if res.ok else None, res.erro)
        frias = []
        for linha in arq_ops.read_text().splitlines():
            ev = json.loads(linha)
            if (
                ev["op"] == "PUT"
                and ev["node"] == 1
                and ev["key"].startswith(f"{tag}/cold/")
                and ev["status"] == "ok"
                and fase_por_intervalo(ev["t_start"], ev["t_end"], marcos) == "partition"
            ):
                frias.append(ev["key"])
        faltando: dict[int, list[str]] = {}
        erros: dict[int, int] = {}
        for n in (3, 2):  # garage3 primeiro: em consistent, a leitura com R = 2 via garage2 não repara antes
            s3 = lab_s3.cliente_s3(n)
            faltando[n], erros[n] = [], 0
            for chave in frias:
                res = lab_s3.get(s3, n, chave)
                if res.ok:
                    continue
                if res.erro and res.erro.startswith("NoSuchKey"):
                    faltando[n].append(chave)
                else:
                    erros[n] += 1
        self.evidencia(
            f"rep{i}_{modo}_verificacao.txt",
            "GET da chave quente pelos 3 nós e das chaves frias confirmadas na partição via garage3 e garage2",
            lambda: {
                "ultima_versao_confirmada": ultima,
                "lida_por_no": lida,
                "frias_da_particao": len(frias),
                "faltando": faltando,
                "erros": erros,
            },
        )
        return {
            "ultima_versao_confirmada": ultima,
            "quente_lida": {n: v for n, (v, _) in lida.items()},
            "quente_atualizada_nos": sorted(n for n, (v, _) in lida.items() if v == ultima),
            "frias_da_particao": len(frias),
            "frias_faltando": {n: len(v) for n, v in faltando.items()},
            "frias_erros": erros,
        }

    # ------------------------------------------------------------ análise

    def _eventos(self, i: int, modo: str, marcos: dict):
        for linha in (self.dir / f"ops_rep{i}_{modo}.jsonl").read_text().splitlines():
            ev = json.loads(linha)
            ev["fase"] = fase_por_intervalo(ev["t_start"], ev["t_end"], marcos)
            yield ev

    @staticmethod
    def _deteccao(monitor: list, marcos: dict) -> float | None:
        """1º instante após partition_at em que garage1 não vê garage3 ou garage3 não vê garage1."""
        pa, ha = marcos["partition_at"], marcos["heal_at"]
        for x in monitor:
            if pa <= x["t"] <= ha and x["ve_up"] is not None:
                outro = "garage3" if x["no"] == 1 else "garage1"
                if outro not in x["ve_up"]:
                    return x["t"]
        return None

    def analyze(self) -> dict:
        reps = [(r["rep"], r["resultado"]) for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}

        cont: dict[tuple, Counter] = {}  # (modo, fase, op, nó) → contagens
        lat: dict[tuple, list[float]] = {}
        lag: dict[str, list[float]] = {m: [] for m in self.modos}
        csv_linhas: list[dict] = []
        tempos: dict[str, dict[str, list]] = {
            m: {"deteccao": [], "quorum_min": [], "malha": [], "fresh_g3": [], "p6_ok": []} for m in self.modos
        }
        verif: dict[str, list[dict]] = {m: [] for m in self.modos}
        anomalias = 0
        linha_do_tempo: dict[str, tuple] = {}

        for i, res in reps:
            for modo, rod in res["rodadas"].items():
                marcos = rod["marcos"]
                por_rep: dict[tuple, Counter] = {}
                prim_quorum = prim_fresh = None
                eventos = list(self._eventos(i, modo, marcos))
                for ev in eventos:
                    chave = (modo, ev["fase"], ev["op"], ev["node"])
                    for alvo in (cont.setdefault(chave, Counter()), por_rep.setdefault(chave, Counter())):
                        alvo["tentativas"] += 1
                        if _respondeu(ev):
                            alvo["respondeu"] += 1
                            if ev.get("class") == "stale":
                                alvo["stale"] += 1
                            if ev.get("motivo") == "nosuchkey":
                                alvo["nosuchkey"] += 1
                            if ev["op"] == "LIST" and ev.get("faltando"):
                                alvo["incompleta"] += 1
                            if ev.get("truncada"):
                                alvo["truncada"] += 1
                        elif ev.get("class") == "anomaly":
                            alvo["anomaly"] += 1
                        else:
                            alvo[_tipo_erro(ev.get("error") or "")] += 1
                    if ev.get("class") == "anomaly":
                        anomalias += 1
                    if _respondeu(ev):
                        lat.setdefault(chave, []).append((ev["t_end"] - ev["t_start"]) * 1000)
                    if ev["op"] == "GET" and ev["node"] == 3:
                        if ev["fase"] == "partition" and ev.get("class") == "stale" and ev.get("lag_ms") is not None:
                            lag[modo].append(ev["lag_ms"])
                        if (
                            ev["fase"] == "partition"
                            and "Could not reach quorum" in (ev.get("error") or "")
                            and (prim_quorum is None or ev["t_end"] < prim_quorum)
                        ):
                            prim_quorum = ev["t_end"]
                        if (
                            ev["fase"] == "heal"
                            and ev.get("class") == "fresh"
                            and (prim_fresh is None or ev["t_end"] < prim_fresh)
                        ):
                            prim_fresh = ev["t_end"]
                for (m, fase, op, no), c in por_rep.items():
                    csv_linhas.append({"rep": i, "modo": m, "cenario": "A1", "fase": fase, "op": op, "no": no, **c})
                det = self._deteccao(rod["monitor"], marcos)
                malha = marcos["converged_at"] - marcos["heal_at"]
                fresh_g3 = None if prim_fresh is None else prim_fresh - marcos["heal_at"]
                t = tempos[modo]
                t["deteccao"].append(None if det is None else det - marcos["partition_at"])
                t["quorum_min"].append(None if prim_quorum is None else prim_quorum - marcos["partition_at"])
                t["malha"].append(malha)
                t["fresh_g3"].append(fresh_g3)
                t["p6_ok"].append(fresh_g3 is not None and fresh_g3 <= malha + FOLGA_P6_S)
                verif[modo].append({"rep": i, **rod["verificacao"]})
                if modo not in linha_do_tempo:
                    linha_do_tempo[modo] = (i, eventos, marcos, det)

        self.gravar_csv("metrics.csv", csv_linhas)

        def c(modo, op, no, fase="partition") -> Counter:
            return cont.get((modo, fase, op, no), Counter())

        def taxa_stale(cc: Counter) -> str:
            return _ic(stats.wilson(cc["stale"], cc["respondeu"])) if cc["respondeu"] else "n/a"

        # ---------------- tabela teórica × observada
        prev_leitura = {
            "consistent": "indisponível",
            "degraded": "disponível, desatualizada",
            "dangerous": "disponível, desatualizada",
        }
        prev_escrita = {"consistent": "indisponível", "degraded": "indisponível", "dangerous": "disponível"}
        teoria = []
        for m in self.modos:
            r, w_ = QUORUNS[m]
            g3, s3_ = c(m, "GET", 3), c(m, "PUT", 3)
            teoria.append(
                [
                    m,
                    N_REPLICAS,
                    r,
                    w_,
                    "sim" if r + w_ > N_REPLICAS else "não",
                    prev_leitura[m],
                    f"{_pct(g3['respondeu'], g3['tentativas'])} respondidas; desatualizadas {taxa_stale(g3)}",
                    prev_escrita[m],
                    f"{_pct(s3_['respondeu'], s3_['tentativas'])} ok",
                ]
            )

        # ---------------- operações por modo, fase e papel
        ops_tab = []
        for (m, fase, op, no), cc in sorted(
            cont.items(), key=lambda kv: (self.modos.index(kv[0][0]), FASES.index(kv[0][1]), kv[0][2], kv[0][3])
        ):
            lt = lat.get((m, fase, op, no)) or []
            rs = stats.resumir(lt) if lt else None
            ops_tab.append(
                [
                    m,
                    fase,
                    PAPEIS.get((op, no), f"{op} g{no}"),
                    cc["tentativas"],
                    cc["respondeu"],
                    _pct(cc["respondeu"], cc["tentativas"]),
                    taxa_stale(cc) if op == "GET" else "—",
                    cc["nosuchkey"] if op == "GET" else "—",
                    _pct(cc["incompleta"], cc["respondeu"]) if op == "LIST" else "—",
                    cc["timeout"],
                    cc["quorum"],
                    cc["outro_erro"],
                    f"{rs.p50:.1f}" if rs else "—",
                    f"{rs.p95:.1f}" if rs else "—",
                ]
            )

        # ---------------- atraso das desatualizadas da minoria
        lag_tab = []
        for m in self.modos:
            if lag[m]:
                rs = stats.resumir(lag[m])
                lag_tab.append([m, rs.n, f"{rs.p50:.0f}", f"{rs.p95:.0f}", f"{rs.maximo:.0f}"])
            else:
                lag_tab.append([m, 0, "—", "—", "—"])

        def resumo(v):
            x = [a for a in v if a is not None]
            if not x:
                return "—"
            s = stats.resumir(x)
            return f"{s.media:.1f} ± {0 if s.n < 2 else s.desvio:.1f} s (n={s.n})" + (
                "" if len(x) == len(v) else f"; ausente em {len(v) - len(x)}"
            )

        tempos_tab = [
            [
                m,
                resumo(tempos[m]["deteccao"]),
                resumo(tempos[m]["quorum_min"]),
                resumo(tempos[m]["malha"]),
                resumo(tempos[m]["fresh_g3"]),
            ]
            for m in self.modos
        ]

        verif_tab = []
        for m in self.modos:
            for v in verif[m]:
                verif_tab.append(
                    [
                        m,
                        v["rep"],
                        v["ultima_versao_confirmada"],
                        ", ".join(f"g{n}={v['quente_lida'][n]}" for n in sorted(v["quente_lida"])),
                        v["frias_da_particao"],
                        v["frias_faltando"].get(3, v["frias_faltando"].get("3")),
                        v["frias_faltando"].get(2, v["frias_faltando"].get("2")),
                        sum(v["frias_erros"].values()),
                    ]
                )

        # ---------------- figuras
        figuras = self._figuras(c, lag, linha_do_tempo)

        # ---------------- previsões (critérios definidos no topo do módulo, antes das execuções)
        def disp(cc: Counter) -> float:
            return cc["respondeu"] / cc["tentativas"] if cc["tentativas"] else 0.0

        def frac_stale(cc: Counter) -> float:
            return cc["stale"] / cc["respondeu"] if cc["respondeu"] else 0.0

        def tem(m):
            return m in self.modos

        resultados = []
        if tem("consistent"):
            g3 = c("consistent", "GET", 3)
            resultados.append(
                {
                    "id": "P1",
                    "situacao": "confirmada" if g3["respondeu"] == 0 and g3["stale"] == 0 else "refutada",
                    "resultado": f"consistent, leitura minoria na partição: {g3['respondeu']}/{g3['tentativas']} "
                    f"respondidas ({g3['timeout']} timeouts, {g3['quorum']} sem quórum); desatualizadas: {g3['stale']}",
                }
            )
        for pid, m in (("P2", "degraded"), ("P3", "dangerous")):
            if not tem(m):
                continue
            g3 = c(m, "GET", 3)
            ok = disp(g3) >= DISP_MIN_RESPONDE and frac_stale(g3) >= STALE_MIN_MINORIA
            texto = (
                f"{m}, leitura minoria na partição: {_pct(g3['respondeu'], g3['tentativas'])} respondidas; "
                f"desatualizadas {taxa_stale(g3)}"
            )
            if pid == "P3":
                sondas = {mm: c(mm, "PUT", 3) for mm in self.modos}
                ok = (
                    ok
                    and disp(sondas["dangerous"]) >= SONDA_MIN_DANGEROUS
                    and all(disp(sondas[mm]) <= SONDA_MAX_OUTROS for mm in self.modos if mm != "dangerous")
                )
                texto += "; sonda de escrita da minoria: " + ", ".join(
                    f"{mm} {_pct(s['respondeu'], s['tentativas'])} ok" for mm, s in sondas.items()
                )
            resultados.append({"id": pid, "situacao": "confirmada" if ok else "refutada", "resultado": texto})
        g2 = {m: c(m, "GET", 2) for m in self.modos}
        p4 = all(disp(g2[m]) >= DISP_MIN_MAIORIA and frac_stale(g2[m]) <= STALE_MAX_MAIORIA[m] for m in self.modos)
        resultados.append(
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "leitura maioria (g2) na partição: "
                + "; ".join(
                    f"{m} {_pct(g2[m]['respondeu'], g2[m]['tentativas'])} respondidas, desatualizadas {taxa_stale(g2[m])}"
                    for m in self.modos
                ),
            }
        )
        l3 = {m: c(m, "LIST", 3) for m in self.modos}
        p5 = all(
            l3[m]["respondeu"] == 0
            if m == "consistent"
            else disp(l3[m]) >= LISTA_MIN_RESPONDE
            and l3[m]["respondeu"]
            and l3[m]["incompleta"] / l3[m]["respondeu"] >= LISTA_MIN_INCOMPLETA
            for m in self.modos
        )
        resultados.append(
            {
                "id": "P5",
                "situacao": "confirmada" if p5 else "refutada",
                "resultado": "listagem minoria na partição: "
                + "; ".join(
                    f"{m} {_pct(l3[m]['respondeu'], l3[m]['tentativas'])} respondidas, "
                    f"{_pct(l3[m]['incompleta'], l3[m]['respondeu'])} delas incompletas"
                    for m in self.modos
                ),
            }
        )
        falt = {m: sum(v["frias_faltando"].get(3, 0) for v in verif[m]) for m in self.modos}
        p6 = all(all(tempos[m]["p6_ok"]) for m in self.modos) and all(
            falt[m] == 0 if m == "consistent" else falt[m] > 0 for m in self.modos
        )
        resultados.append(
            {
                "id": "P6",
                "situacao": "confirmada" if p6 else "refutada",
                "resultado": "1ª leitura atualizada via g3 após curar: "
                + "; ".join(
                    f"{m} {resumo(tempos[m]['fresh_g3'])} (≤ malha + {FOLGA_P6_S:.0f} s em "
                    f"{sum(tempos[m]['p6_ok'])}/{len(tempos[m]['p6_ok'])})"
                    for m in self.modos
                )
                + ". Chaves frias da partição faltando via g3 na verificação: "
                + "; ".join(f"{m} {falt[m]}/{sum(v['frias_da_particao'] for v in verif[m])}" for m in self.modos),
            }
        )

        secoes = [
            (
                "Tabela teórica × observada (fase de partição, todas as repetições)",
                tabela_md(
                    [
                        "modo",
                        "N",
                        "R",
                        "W",
                        "R + W > N",
                        "leitura minoria (teoria)",
                        "leitura minoria (observado)",
                        "escrita minoria (teoria)",
                        "escrita minoria (sonda)",
                    ],
                    teoria,
                ),
            ),
            (
                "Operações por modo, fase e papel (todas as repetições)",
                "Respondida = leitura classificada como fresh ou stale (NoSuchKey incluído); desatualizadas "
                "= stale / respondidas, com IC95% de Wilson, ou n/a sem resposta (P9). Fase pelo intervalo inteiro "
                "da operação: *transicao* = atravessou um marco.\n\n"
                + tabela_md(
                    [
                        "modo",
                        "fase",
                        "papel",
                        "tentativas",
                        "respondidas",
                        "disponibilidade",
                        "desatualizadas",
                        "NoSuchKey",
                        "listagens incompletas",
                        "timeout",
                        "sem quórum",
                        "outro erro",
                        "p50 (ms)",
                        "p95 (ms)",
                    ],
                    ops_tab,
                ),
            ),
            (
                "Atraso das leituras desatualizadas da minoria (lag_ms, fase de partição)",
                tabela_md(["modo", "n", "p50 (ms)", "p95 (ms)", "máximo (ms)"], lag_tab),
            ),
            (
                "Janela de detecção e reconexão (média ± desvio entre repetições)",
                tabela_md(
                    [
                        "modo",
                        "detecção (1º isUp = false, g1 ou g3)",
                        "1º 'sem quórum' na leitura da minoria",
                        "malha completa após curar",
                        "1ª leitura atualizada via g3 após curar",
                    ],
                    tempos_tab,
                ),
            ),
            (
                "Verificação depois da carga",
                "Chave quente lida pelos 3 nós (versão lida × última confirmada) e chaves frias confirmadas durante "
                "a partição lidas via garage3 e via garage2 (faltando = NoSuchKey).\n\n"
                + tabela_md(
                    [
                        "modo",
                        "rep",
                        "última confirmada",
                        "chave quente lida",
                        "frias da partição",
                        "faltando via g3",
                        "faltando via g2",
                        "erros",
                    ],
                    verif_tab,
                ),
            ),
            ("Conclusão (CAP / PACELC)", self._conclusao(c)),
        ]
        if anomalias:
            secoes.insert(0, ("⚠️ Anomalias", f"{anomalias} leituras `anomaly`: a execução é inválida."))
        return {"resultados": resultados, "secoes": secoes, "figuras": figuras}

    def _conclusao(self, c) -> str:
        partes = []
        for m in self.modos:
            g3, s3_ = c(m, "GET", 3), c(m, "PUT", 3)
            disp = g3["respondeu"] / g3["tentativas"] if g3["tentativas"] else 0
            stale = g3["stale"] / g3["respondeu"] if g3["respondeu"] else None
            sonda = s3_["respondeu"] / s3_["tentativas"] if s3_["tentativas"] else 0
            if disp == 0:
                lado = "a minoria **não respondeu** a leituras (escolha por **C**, sacrificando **A**)"
            else:
                lado = (
                    f"a minoria respondeu {disp * 100:.0f}% das leituras, "
                    + (f"{stale * 100:.0f}% delas desatualizadas" if stale is not None else "")
                    + " (escolha por **A**, sacrificando **C**)"
                )
            partes.append(f"- **{m}:** {lado}; escrita pela minoria aceita em {sonda * 100:.0f}% das tentativas.")
        partes.append(
            "\nNos termos do PACELC, sob partição (P) `consistent` escolhe C e `degraded`/`dangerous` escolhem A; "
            "a outra metade do PACELC (latência × consistência sem partição) aparece no baseline e no E4. "
            "Texto gerado a partir dos números; a interpretação final é da equipe."
        )
        return "\n".join(partes)

    def _figuras(self, c, lag: dict, linha_do_tempo: dict) -> list[str]:
        figuras = []
        papeis = [
            ("GET", 3, "leitura minoria (g3)", "#c53030"),
            ("GET", 2, "leitura maioria (g2)", "#2b6cb0"),
            ("PUT", 3, "escrita minoria (sonda g3)", "#dd8a1b"),
            ("LIST", 3, "listagem minoria (g3)", "#805ad5"),
        ]
        series = {}
        for op, no, rotulo, _ in papeis:
            vals = []
            for m in self.modos:
                cc = c(m, op, no)
                vals.append(cc["respondeu"] / cc["tentativas"] * 100 if cc["tentativas"] else 0)
            series[rotulo] = (vals, [0] * len(self.modos))
        plots.salvar(
            plots.barras_agrupadas(
                list(self.modos),
                series,
                "Disponibilidade na partição por modo e papel",
                "% de operações respondidas",
                {rot: cor for _, _, rot, cor in papeis},
            ),
            self.dir_figuras,
            "disponibilidade_por_modo",
        )
        figuras.append("disponibilidade_por_modo")

        medias, inf, sup, rotulos = [], [], [], []
        for m in self.modos:
            cc = c(m, "GET", 3)
            if cc["respondeu"]:
                pw = stats.wilson(cc["stale"], cc["respondeu"])
                medias.append(pw.p * 100)
                inf.append((pw.p - pw.ic95[0]) * 100)
                sup.append((pw.ic95[1] - pw.p) * 100)
                rotulos.append(m)
            else:
                medias.append(0)
                inf.append(0)
                sup.append(0)
                rotulos.append(f"{m}\n(n/a: 0 respostas)")
        plots.salvar(
            plots.barras_com_erro(
                rotulos,
                medias,
                [inf, sup],
                "Leituras desatualizadas na minoria (partição), IC95% Wilson",
                "% das leituras respondidas",
                [plots.CORES_MODO[m] for m in self.modos],
            ),
            self.dir_figuras,
            "stale_rate_por_modo",
        )
        figuras.append("stale_rate_por_modo")

        plots.salvar(
            plots.cdf(
                {m: lag[m] for m in self.modos},
                "Atraso das leituras desatualizadas da minoria",
                "lag (ms) = início da leitura − 1ª confirmação que a superou",
                plots.CORES_MODO,
            ),
            self.dir_figuras,
            "lag_cdf",
        )
        figuras.append("lag_cdf")

        cor_ok, cor_inc = plots.CORES_CLASSE["fresh"], plots.CORES_CLASSE["stale"]
        cor_err = plots.CORES_CLASSE["error"]
        for m, (i, eventos, marcos, det) in linha_do_tempo.items():
            pa = marcos["partition_at"]
            faixas_def = [
                ("g2 leitura", "GET", 2),
                ("g3 leitura", "GET", 3),
                ("g1 escrita", "PUT", 1),
                ("g3 escrita (sonda)", "PUT", 3),
                ("g3 listagem", "LIST", 3),
            ]
            faixas = []
            for rotulo, op, no in faixas_def:
                pontos = []
                for ev in eventos:
                    if ev["op"] != op or ev["node"] != no:
                        continue
                    if op == "GET":
                        cor = plots.CORES_CLASSE.get(ev.get("class") or "error", cor_err)
                    elif op == "LIST":
                        cor = cor_err if ev["status"] != "ok" else (cor_inc if ev.get("faltando") else cor_ok)
                    else:
                        cor = cor_ok if ev["status"] == "ok" else cor_err
                    pontos.append((ev["t_start"] - pa, cor))
                faixas.append((rotulo, pontos))
            rel = {
                "partição": 0.0,
                "cura": marcos["heal_at"] - pa,
                "malha completa": marcos["converged_at"] - pa,
            }
            if det is not None:
                rel["detecção"] = det - pa
            plots.salvar(
                plots.linha_do_tempo(
                    faixas,
                    rel,
                    f"E9 — {m} (repetição {i}): garage3 particionado",
                    legenda={
                        "atualizada / ok / listagem completa": cor_ok,
                        "desatualizada / listagem incompleta": cor_inc,
                        "erro (timeout ou sem quórum)": cor_err,
                    },
                ),
                self.dir_figuras,
                f"timeline_{m}",
            )
            figuras.append(f"timeline_{m}")
        return figuras
